# -*- coding: utf-8 -*-

from logging import getLogger

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock

from .exceptions import TimeoutExpired

logger = getLogger(__name__)


class ChannelMultiplexer(object):
    def __init__(self, events, is_client=False):
        """
        :param events:
        :param is_client:
            服务器端为False
            客户端为True
        """
        self._events = events
        self._active_channels = {}
        self._channel_dispatcher_task = None
        self._broadcast_queue = None

        if events.recv_is_available and not is_client:
            # 服务器端专门启用一个greenlet 来
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
            event = self._broadcast_queue.get() # 主要是新的连接的event
        else:
            event = self._events.recv()

        # 服务器模式下，只会返回新的连接
        return event

    def _channel_dispatcher(self):

        while True:
            # 1. 读取到event
            try:
                event = self._events.recv()
            except Exception as e:
                logger.error('zerorpc.ChannelMultiplexer, ignoring error on recv: {0}'.format(e))
                continue


            # 2. 如何处理event呢?
            #    将新的event交给: channel_id对应的queue, 或者_broadcast_queue中
            channel_id = event.header.get('response_to', None)

            queue = None
            if channel_id is not None:
                channel = self._active_channels.get(channel_id, None)
                if channel is not None:
                    queue = channel._queue

            #  如果没有: response_to, 则表示是一个新的Event, 需要创建一个新的Channel
            elif self._broadcast_queue is not None:
                queue = self._broadcast_queue

            if queue is None:
                logger.error('zerorpc.ChannelMultiplexer, unable to route event: {0}'.format(event.__str__(ignore_args=True)))
            else:
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
    """
        Channel和一个Event对应
    """
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
