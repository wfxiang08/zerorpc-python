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

    def recv(self):
        """
            主动读取event
        """

        # 正常的Server模式下, _broadcast_queue 有效，则recv从 _broadcast_queue 获取数据， 也就是event只和listening fd关联
        if self._broadcast_queue is not None:
            # 服务区端通过: _broadcast_queue 来获取数据
            event = self._broadcast_queue.get()
        else:
            event = self._events.recv()
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
            queue = None
            # 1. queue/channel就是一个生产者，消费者模式的，往里面写入数据，就会触发它的相关的操作
            if channel_id is not None:
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

        return Channel(self, from_event)

    @property
    def active_channels(self):
        return self._active_channels

    @property
    def context(self):
        return self._events.context


class Channel(object):

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
        event = self._multiplexer.create_event(name, args, xheader)

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
            event = self._queue.get(timeout=timeout)
        except gevent.queue.Empty:
            raise TimeoutExpired(timeout)
        return event

    @property
    def context(self):
        return self._multiplexer.context


class BufferedChannel(object):
    """
        BufferedChannel和普通的channel之间的差别?

    """

    def __init__(self, channel, inqueue_size=100):
        self._channel = channel
        self._input_queue_size = inqueue_size
        self._remote_queue_open_slots = 1
        self._input_queue_reserved = 1
        self._remote_can_recv = gevent.event.Event()
        self._input_queue = gevent.queue.Queue()
        self._lost_remote = False
        self._verbose = False
        self._on_close_if = None
        self._recv_task = gevent.spawn(self._recver)

    @property
    def recv_is_available(self):
        return self._channel.recv_is_available

    @property
    def on_close_if(self):
        return self._on_close_if

    @on_close_if.setter
    def on_close_if(self, cb):
        self._on_close_if = cb

    def __del__(self):
        self.close()

    def close(self):
        if self._recv_task is not None:
            self._recv_task.kill()
            self._recv_task = None
        if self._channel is not None:
            self._channel.close()
            self._channel = None

    def _recver(self):
        while True:
            event = self._channel.recv()
            if event.name == '_zpc_more':
                try:
                    self._remote_queue_open_slots += int(event.args[0])
                except Exception as e:
                    logger.error(
                        'gevent_zerorpc.BufferedChannel._recver, '
                        'exception: ' + repr(e))
                if self._remote_queue_open_slots > 0:
                    self._remote_can_recv.set()
            elif self._input_queue.qsize() == self._input_queue_size:
                raise RuntimeError(
                    'BufferedChannel, queue overflow on event:', event)
            else:
                self._input_queue.put(event)
                if self._on_close_if is not None and self._on_close_if(event):
                    self._recv_task = None
                    self.close()
                    return

    def create_event(self, name, args, xheader=None):
        return self._channel.create_event(name, args, xheader)

    def emit_event(self, event, block=True, timeout=None):
        if self._remote_queue_open_slots == 0:
            if not block:
                return False
            self._remote_can_recv.clear()
            self._remote_can_recv.wait(timeout=timeout)
        self._remote_queue_open_slots -= 1
        try:
            self._channel.emit_event(event)
        except:
            self._remote_queue_open_slots += 1
            raise
        return True

    def emit(self, name, args, xheader=None, block=True, timeout=None):
        event = self.create_event(name, args, xheader)
        return self.emit_event(event, block, timeout)

    def _request_data(self):
        open_slots = self._input_queue_size - self._input_queue_reserved
        self._input_queue_reserved += open_slots
        self._channel.emit('_zpc_more', (open_slots,))

    def recv(self, timeout=None):
        if self._verbose:
            if self._input_queue_reserved < self._input_queue_size / 2:
                self._request_data()
        else:
            self._verbose = True

        try:
            event = self._input_queue.get(timeout=timeout)
        except gevent.queue.Empty:
            raise TimeoutExpired(timeout)

        self._input_queue_reserved -= 1
        return event

    @property
    def channel(self):
        return self._channel

    @property
    def context(self):
        return self._channel.context
