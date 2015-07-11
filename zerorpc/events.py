# -*- coding: utf-8 -*-

from __future__ import absolute_import

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock

from zerorpc.context import Context
import zerorpc.gevent_zmq as zmq


class Sender(object):
    """
        模拟: zeromq.Socket的send函数
    """
    def __init__(self, socket):
        self._socket = socket

        # gevent定制的Queue
        self._send_queue = gevent.queue.Channel()

        # 通过gevent.spawn创新新的"线程", 异步执行_sender任务
        self._send_task = gevent.spawn(self._sender)

    def __del__(self):
        self.close()

    def close(self):
        if self._send_task:
            self._send_task.kill()

    def _sender(self):
        running = True

        # iterator的特殊处理, 每次iterator，都会将数据从queue中读取出来
        for parts in self._send_queue:

            # 如何发送一个parts呢? 一个part, 一个part地发送
            for i in xrange(len(parts) - 1):
                try:
                    self._socket.send(parts[i], flags=zmq.SNDMORE)
                except gevent.GreenletExit:
                    # Greenlet即将退出之前, 确保一个完整的message被发送出去
                    if i == 0:
                        return
                    running = False
                    self._socket.send(parts[i], flags=zmq.SNDMORE)
            self._socket.send(parts[-1])
            if not running:
                return

    # 直接将数据添加到_send_queue中
    def __call__(self, parts):
        self._send_queue.put(parts)


class Receiver(object):

    def __init__(self, socket):
        self._socket = socket
        self._recv_queue = gevent.queue.Channel()
        self._recv_task = gevent.spawn(self._recver)

    def __del__(self):
        self.close()

    def close(self):
        if self._recv_task:
            self._recv_task.kill()

    def _recver(self):
        running = True
        while True:
            # 一口气读完所有的数据包
            parts = []
            while True:
                try:
                    # 监听Socket
                    part = self._socket.recv()
                except gevent.GreenletExit:
                    running = False
                    if len(parts) == 0:
                        return

                    # 继续读取数据，直到一个完整的消息被读取完毕
                    part = self._socket.recv()

                parts.append(part)
                # 直到没有更多的消息，则算是一个parts的读取完毕
                if not self._socket.getsockopt(zmq.RCVMORE):
                    break
            if not running:
                break

            # 添加一个完整的数据包
            self._recv_queue.put(parts)

    def __call__(self):
        # 没调用一次: Receiver就从: queue中读取一个parts的数据
        return self._recv_queue.get()


class Event(object):
    """
        将name, _args, _header进行打包
    """

    __slots__ = ['_msg', '_id']

    def __init__(self, msg, id=None):
        self._msg = msg
        self._id = id


    @property
    def msg(self):
        return self._msg

    @msg.setter
    def msg(self, v):
        self._msg = v

    @property
    def id(self):
        return self._id

    def __str__(self, ignore_args=False):
        return "Thrift Event"


class Events(object):
    """
        Events内部管理: zmq.Socket
    """

    def __init__(self, zmq_socket_type, context=None):
        # 默认为: zmq.ROUTER
        self._zmq_socket_type = zmq_socket_type
        self._context = context or Context.get_instance()

        # 通过zmq将请求变成Event
        # 创建指定类型的_socket, 注意: _send, _recv 的格式, 接受的数据都是以msg为单位的
        self._socket = zmq.Socket(self._context, zmq_socket_type)

        self._send = self._socket.send_multipart
        self._recv = self._socket.recv_multipart

        # 异步发送数据（存在缓存)
        # 在DEALER/ROUTER模式下，可能有大量的REQ/REP, 需要异步处理
        # 而普通的REQ/REP则直接block住，按照 lock-step模式进行交互
        if zmq_socket_type in (zmq.PUSH, zmq.PUB, zmq.DEALER, zmq.ROUTER):
            self._send = Sender(self._socket)

        if zmq_socket_type in (zmq.PULL, zmq.SUB, zmq.DEALER, zmq.ROUTER):
            self._recv = Receiver(self._socket)


    @property
    def recv_is_available(self):
        """
            默认情况下: _zmq_socket_type为 zmq.ROUTER
        :return:
        """
        return self._zmq_socket_type in (zmq.PULL, zmq.SUB, zmq.DEALER, zmq.ROUTER)

    def __del__(self):
        try:
            if not self._socket.closed:
                self.close()
        except AttributeError:
            pass

    def close(self):
        try:
            self._send.close()
        except AttributeError:
            pass
        try:
            self._recv.close()
        except AttributeError:
            pass
        self._socket.close()

    def _resolve_endpoint(self, endpoint, resolve=True):
        # 对: endpoint调用一些事先准备好的func, 进行处理; 默认不做任何事情
        if resolve:
            endpoint = self._context.hook_resolve_endpoint(endpoint)

        if isinstance(endpoint, (tuple, list)):
            r = []
            for sub_endpoint in endpoint:
                r.extend(self._resolve_endpoint(sub_endpoint, resolve))
            return r
        return [endpoint]

    def connect(self, endpoint, resolve=True):
        r = []
        for endpoint_ in self._resolve_endpoint(endpoint, resolve):
            r.append(self._socket.connect(endpoint_))
        return r

    def bind(self, endpoint, resolve=True):
        """
            将_socket绑定到指定的endpoint
        :param endpoint:
        :param resolve:
        :return:
        """
        r = []
        for endpoint_ in self._resolve_endpoint(endpoint, resolve):
            r.append(self._socket.bind(endpoint_))
        return r

    def create_event(self, msg, id):
        """
            创建一个Event对象
        :param name:
        :param args:
        :param xheader:
        :return:
        """
        event = Event(msg, id)
        return event

    def emit_event(self, event, id=None):
        """
        发送Event
        :param event:
        :param id:
        :return:
        """
        if id is not None:
            # 带有identity的情况
            parts = list(id)
            parts.extend(['', event.msg])

        elif self._zmq_socket_type in (zmq.DEALER, zmq.ROUTER):
            # 都以: REQ为标准，数据统一处理为: ("", data)
            parts = ('', event.msg)
        else:

            # 其他的type?
            parts = (event.msg,)
        self._send(parts)

    def emit(self, msg, id = None):
        event = self.create_event(msg, id)
        return self.emit_event(event, id)

    def recv(self):
        # 读取用户请求
        parts = self._recv()
        if len(parts) == 1:
            identity = None
            msg = parts[0]
        else:
            identity = parts[0:-2]
            msg = parts[-1]

        event = Event(msg, identity)
        return event

    def setsockopt(self, *args):
        return self._socket.setsockopt(*args)

    @property
    def context(self):
        return self._context

def get_stack_info():
    import inspect
    stacks = inspect.stack()
    results = []
    for stack in stacks[2:]:
        func_name = "%s %s %s %d" % (stack[1], stack[3], stack[4], stack[2])
        func_name = func_name.replace("/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/", "")
        results.append(func_name)
    return "\n".join(results)
