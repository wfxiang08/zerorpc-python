# -*- coding: utf-8 -*-
# Open Source Initiative OSI - The MIT License (MIT):Licensing
#
# The MIT License (MIT)
# Copyright (c) 2012 DotCloud Inc (opensource@dotcloud.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import msgpack
import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock

from __future__ import absolute_import
from zerorpc.context import Context

import gevent_zmq as zmq


class Sender(object):

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

        # iterator的特殊处理
        for parts in self._send_queue:
            #
            # 如何发送一个parts呢?
            # 一个part, 一个part地发送
            #
            for i in xrange(len(parts) - 1):
                try:
                    self._socket.send(parts[i], flags=zmq.SNDMORE)
                except gevent.GreenletExit:
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

                    # 什么情况?
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

    __slots__ = ['_name', '_args', '_header']

    def __init__(self, name, args, context, header=None):
        self._name = name
        self._args = args

        if header is None:
            self._header = {
                # 生成新的msgid
                'message_id': context.new_msgid(),
                'v': 3
            }
        else:
            self._header = header

    @property
    def header(self):
        return self._header

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, v):
        self._name = v

    @property
    def args(self):
        return self._args

    # 将参数打包
    def pack(self):
        return msgpack.Packer().pack((self._header, self._name, self._args))

    # 解压缩参数
    @staticmethod
    def unpack(blob):
        unpacker = msgpack.Unpacker()
        unpacker.feed(blob)
        unpacked_msg = unpacker.unpack()

        try:
            (header, name, args) = unpacked_msg
        except Exception as e:
            raise Exception('invalid msg format "{0}": {1}'.format(
                unpacked_msg, e))

        # Backward compatibility
        if not isinstance(header, dict):
            header = {}

        return Event(name, args, None, header)

    def __str__(self, ignore_args=False):
        if ignore_args:
            args = '[...]'
        else:
            args = self._args
            try:
                args = '<<{0}>>'.format(str(self.unpack(self._args)))
            except:
                pass
        return '{0} {1} {2}'.format(self._name, self._header,
                args)


class Events(object):

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

    def create_event(self, name, args, xheader=None):
        """
            创建一个Event对象
        :param name:
        :param args:
        :param xheader:
        :return:
        """
        xheader = {} if xheader is None else xheader
        event = Event(name, args, context=self._context)

        # 跳过zmqid
        for k, v in xheader.items():
            if k == 'zmqid':
                continue
            event.header[k] = v
        return event

    def emit_event(self, event, identity=None):
        """
        发送Event
        :param event:
        :param identity:
        :return:
        """
        # if identity:
        #     print get_stack_info()
        #     print "identity: ", identity

        if identity is not None:
            # 带有identity的情况
            parts = list(identity)
            parts.extend(['', event.pack()])

        elif self._zmq_socket_type in (zmq.DEALER, zmq.ROUTER):
            # DEALER, ROUTER的parts包装
            parts = ('', event.pack())
        else:

            # 其他的type?
            parts = (event.pack(),)
        self._send(parts)

    def emit(self, name, args, xheader=None):
        xheader = {} if xheader is None else xheader
        event = self.create_event(name, args, xheader)

        identity = xheader.get('zmqid', None)

        return self.emit_event(event, identity)

    def recv(self):
        # 读取用户请求
        parts = self._recv()
        if len(parts) == 1:
            identity = None
            blob = parts[0]
        else:
            identity = parts[0:-2]
            blob = parts[-1]
        event = Event.unpack(blob)
        if identity is not None:
            event.header['zmqid'] = identity
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


class WrappedEvents(object):
    """
        在event的基础上再做一个封装
    """
    def __init__(self, channel):
        self._channel = channel

    def close(self):
        pass

    @property
    def recv_is_available(self):
        return self._channel.recv_is_available

    def create_event(self, name, args, xheader=None):
        xheader = {} if xheader is None else xheader
        event = Event(name, args, self._channel.context)
        event.header.update(xheader)
        return event

    def emit_event(self, event, identity=None):
        event_payload = (event.header, event.name, event.args)
        wrapper_event = self._channel.create_event('w', event_payload)
        self._channel.emit_event(wrapper_event)

    def emit(self, name, args, xheader=None):
        wrapper_event = self.create_event(name, args, xheader)
        self.emit_event(wrapper_event)

    def recv(self, timeout=None):
        wrapper_event = self._channel.recv()
        (header, name, args) = wrapper_event.args
        return Event(name, args, None, header)

    @property
    def context(self):
        return self._channel.context
