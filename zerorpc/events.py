# -*- coding: utf-8 -*-

from __future__ import absolute_import
import os
from random import randint

from zerorpc.context import Context
import zerorpc.gevent_zmq as zmq


# 当前的Server是否以 ppworker的模式运行，默认为False
mode_ppworker = False
PPP_READY = "\x01"      # Signals worker is ready
PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat

class Events(object):
    """
        Events内部管理: zmq.Socket
    """

    def __init__(self, zmq_socket_type, context=None):
        # 默认为: zmq.ROUTER
        self._zmq_socket_type = zmq_socket_type
        self._context = context or Context.get_instance()

        self._identity = "%04X-%04X" % (os.getpgid(), randint(0, 0x10000))

        self.socket = None
        self.poller = zmq.Poller()


    def create_worker_socket(self):
        """
        创建一个新的连接，并且
        :return:
        """
        self.socket = zmq.Socket(self._context, self._zmq_socket_type)

        if mode_ppworker:
            self.socket.setsockopt(zmq.IDENTITY, self._identity)
            self.poller.register(self.socket, zmq.POLLIN)
            # self.socket.send(PPP_READY)

        # zeromq send是异步的，因此也不要考虑优化这一块的性能
        self._send = self.socket.send_multipart
        self._recv = self.socket.recv_multipart

    def reconnect(self, connection_to):
        # 关闭之前的连接
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()

        self.create_worker_socket()
        self.connect(connection_to)


    @property
    def recv_is_available(self):
        """
            默认情况下: _zmq_socket_type为 zmq.ROUTER
        :return:
        """
        return self._zmq_socket_type in (zmq.PULL, zmq.SUB, zmq.DEALER, zmq.ROUTER)

    def __del__(self):
        try:
            if not self.socket.closed:
                self.close()
        except AttributeError:
            pass

    def close(self):
        self.socket.close()

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
            r.append(self.socket.connect(endpoint_))
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
            r.append(self.socket.bind(endpoint_))
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
        return self.socket.setsockopt(*args)

    @property
    def context(self):
        return self._context

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

def get_stack_info():
    import inspect
    stacks = inspect.stack()
    results = []
    for stack in stacks[2:]:
        func_name = "%s %s %s %d" % (stack[1], stack[3], stack[4], stack[2])
        func_name = func_name.replace("/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/", "")
        results.append(func_name)
    return "\n".join(results)
