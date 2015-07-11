# -*- coding: utf-8 -*-
from __future__ import  absolute_import
from logging import getLogger

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock

import zerorpc.gevent_zmq as zmq
from zerorpc.core.socket import SocketBase
from zerorpc.context import Context


logger = getLogger(__name__)

class ClientBase(object):

    def __init__(self, context=None, timeout=30):

        # self._multiplexer = ChannelMultiplexer(channel, is_client=True)

        self._context = context or Context.get_instance()
        self._timeout = timeout




    def _process_response(self, request_event, bufchan, timeout):
        # client等待数据的返回
        reply_event = bufchan.recv(timeout)

        # 选择请求模式:
        # REQ/REP
        # REQ/STREAM
        pattern = self._select_pattern(reply_event)

        return pattern.process_answer(self._context, bufchan, request_event, reply_event, self._handle_remote_error)


    def __call__(self, method, *args, **kargs):
        """
            client(command, params)， 在调用RPC时参数都采用: args来传递
        """
        # 主动或者使用默认的timeout
        timeout = kargs.get('timeout', self._timeout)

        # 1. 构建Channel
        channel = self._multiplexer.channel()


        # 2. 各种hook的作用?
        xheader = self._context.hook_get_task_context()

        # 3. 如何创建event, 然后如何获得回调?
        request_event = channel.create_event(method, args, xheader)

        self._context.hook_client_before_request(request_event)

        # 4. 发送请求
        channel.emit_event(request_event)

        # 5. 处理response(同步等待，或异步等待)
        try:
            # 如果是同步的?
            if kargs.get('async', False) is False:
                return self._process_response(request_event, channel, timeout)
            else:
                # 异步如何处理呢?
                async_result = gevent.event.AsyncResult()
                gevent.spawn(self._process_response, request_event, channel, timeout).link(async_result)
                return async_result
        except:
            # XXX: This is going to be closed twice if async is false and
            # _process_response raises an exception. I wonder if the above
            # async branch can raise an exception too, if no we can just remove
            # this code.
            # 可能出现超时等异常?
            channel.close()
            raise

    def __getattr__(self, method):
        """
        直接访问client的属性 --> __call__
        :param method:
        :return:
        """
        return lambda *args, **kargs: self(method, *args, **kargs)



class Client(SocketBase, ClientBase):

    def __init__(self, connect_to=None, context=None, timeout=30, heartbeat=5,
            passive_heartbeat=False):
        # DEALER 可以同时接受多个请求?
        SocketBase.__init__(self, zmq.DEALER, context=context)
        ClientBase.__init__(self, self._events, context, timeout, heartbeat, passive_heartbeat)

        # 在初始化是创建连接
        if connect_to:
            self.connect(connect_to)

    def close(self):
        ClientBase.close(self)
        SocketBase.close(self)