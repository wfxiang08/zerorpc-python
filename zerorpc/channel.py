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

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock

from .exceptions import TimeoutExpired

from logging import getLogger

logger = getLogger(__name__)

"""
    gevent模型中:

    1. 所有的函数都被封装成为Event
    2. Event有两种状态: 正常执行代码，等待某种资源(这种等待被转换成为两种模型，基于io fd(File Description的等待), 或者基于timeout的等待)

"""


class ChannelMultiplexer(object):
    # Events对象
    def __init__(self, events, ignore_broadcast=False):
        """
        :param events:
        :param ignore_broadcast: 服务器端为False, 客户端为True
        :return:
        """
        self._events = events
        self._active_channels = {}
        self._channel_dispatcher_task = None
        self._broadcast_queue = None

        # ignore_broadcast
        # 在Server端为false, 在Client端为True
        #
        if events.recv_is_available and not ignore_broadcast:
            # 如果接受 request, 并且响应广播，则创建一个 _broadcast_queue
            # 并且管理 _channel_dispatcher_task
            self._broadcast_queue = gevent.queue.Queue(maxsize=1)
            self._channel_dispatcher_task = gevent.spawn(self._channel_dispatcher)

    @property
    def recv_is_available(self):
        return self._events.recv_is_available

    def __del__(self):
        self.close()

    def close(self):
        if self._channel_dispatcher_task:
            self._channel_dispatcher_task.kill()

    def create_event(self, name, args, xheader=None):
        return self._events.create_event(name, args, xheader)

    def emit_event(self, event, identity=None):
        return self._events.emit_event(event, identity)

    def emit(self, name, args, xheader=None):
        return self._events.emit(name, args, xheader)

    #
    # Server模式下:
    # 1. _multiplexer.recv 不断接受新的请求, 请的请求来自: _broadcast_queue
    #
    #
    def recv(self):
        """
            主动读取event
        """

        # 正常的Server模式下, _broadcast_queue 有效，则recv从 _broadcast_queue 获取数据， 也就是event只和listening fd关联
        if self._broadcast_queue is not None:
            event = self._broadcast_queue.get() # 主要是新的连接的event
        else:
            event = self._events.recv()

        # 服务器模式下，只会返回新的连接
        return event

    """
        _channel_dispatcher 和 recv 的关系
    """
    def _channel_dispatcher(self):
        """
            从 self._events中读取event，并且dispatch到不同的Channel中, 东西直接放在 Channel对应的queue中

            和 _channel_dispatcher_task 对应，在一个Event内部执行，而: _events.recv 也是被 gevent驱动的


            _events的recv是从socket中读取数据, 这里的socket应该是经过封装的，否则底层应该有很多fd(File Descriptor)
        """

        # 异步地从_events中读取event, 并且放在不同的queue中
        while True:

            # 读取到event
            try:
                event = self._events.recv()
            except Exception as e:
                logger.error(
                    'zerorpc.ChannelMultiplexer, '
                    'ignoring error on recv: {0}'.format(e))
                continue


            channel_id = event.header.get('response_to', None)
            # 为空，表示新的请求
            # 非空，则表示是后续的跟进
            #
            queue = None
            # 1. queue/channel就是一个生产者，消费者模式的，往里面写入数据，就会触发它的相关的操作
            if channel_id is not None:
                # 处理已有的Channel(将event分给不同的Channel)
                channel = self._active_channels.get(channel_id, None)
                if channel is not None:
                    queue = channel._queue

            # 2. 如果没有: response_to, 则表示是一个新的Event, 需要创建一个新的Channel
            elif self._broadcast_queue is not None:
                queue = self._broadcast_queue

            if queue is None:
                logger.error(
                    'zerorpc.ChannelMultiplexer, '
                    'unable to route event: {0}'
                    .format(event.__str__(ignore_args=True)))
            else:
                # 将event交给对应的queue
                queue.put(event)

    def channel(self, from_event=None):

        # _channel_dispatcher_task: 和 _channel_dispatcher 关联
        if self._channel_dispatcher_task is None:
            self._channel_dispatcher_task = gevent.spawn(self._channel_dispatcher)

        # 创建一个Channel?
        return Channel(self, from_event)

    @property
    def active_channels(self):
        # Channel创建时注册，关闭时删除
        return self._active_channels

    @property
    def context(self):
        return self._events.context


class Channel(object):
    #
    # 一个Channel和一个Connection对应，有message_id, zmqid等
    #
    def __init__(self, multiplexer, from_event=None):
        self._multiplexer = multiplexer
        self._channel_id = None

        # 标志: zmqid的来源，可能和底层的socket关联
        self._zmqid = None

        self._queue = gevent.queue.Queue(maxsize=1)

        if from_event is not None:
            self._channel_id = from_event.header['message_id']
            self._zmqid = from_event.header.get('zmqid', None)
            self._multiplexer._active_channels[self._channel_id] = self
            self._queue.put(from_event)

    @property
    def recv_is_available(self):
        return self._multiplexer.recv_is_available

    def __del__(self):
        self.close()

    def close(self):
        if self._channel_id is not None:
            del self._multiplexer._active_channels[self._channel_id]
            self._channel_id = None

    def create_event(self, name, args, xheader=None):
        """
            创建Event, 并和Channel关联
        :param name:
        :param args:
        :param xheader:
        :return:
        """
        # 1. 将事件信息添加到event中
        event = self._multiplexer.create_event(name, args, xheader)

        # 2. event和channel绑定
        # 确定: _channel_id
        # 并且关联: channel和event
        #          _channel_id/message_id
        #          _channel_id/response_to
        if self._channel_id is None:
            self._channel_id = event.header['message_id']
            self._multiplexer._active_channels[self._channel_id] = self
        else:
            event.header['response_to'] = self._channel_id

        return event

    # emilt event
    def emit(self, name, args, xheader=None):
        event = self.create_event(name, args, xheader)
        self._multiplexer.emit_event(event, self._zmqid)

    def emit_event(self, event):
        self._multiplexer.emit_event(event, self._zmqid)

    # 获取数据： 数据被放在_queue中
    def recv(self, timeout=None):
        try:
            # 一直等待?
            event = self._queue.get(timeout=timeout)
        except gevent.queue.Empty:
            raise TimeoutExpired(timeout)
        return event

    @property
    def context(self):
        return self._multiplexer.context
