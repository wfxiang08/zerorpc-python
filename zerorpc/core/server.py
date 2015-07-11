# -*- coding: utf-8 -*-
from __future__ import  absolute_import
import logging
from logging import getLogger

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock
import thrift
from thrift.protocol import TBinaryProtocol

import zerorpc.gevent_zmq as zmq
from zerorpc.core.socket import SocketBase
from zerorpc.context import Context


logger = getLogger(__name__)

class ServerBase(object):

    def __init__(self, events, processor, context=None, pool_size=None):
        self._events = events # 多次赋值也没啥关系

        # thrift
        self.processor = processor # thrift processor
        self.inputProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()
        self.outputProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()

        # zeromq
        self._context = context or Context.get_instance()

        # gevent
        self._task_pool = gevent.pool.Pool(size=pool_size)
        self._acceptor_task = None


    def close(self):
        self.stop()


    def _handle_request(self, event):
        # 得到event之后就要处理
               # 接收到请求之后，获取输入、输出
            # 为 Socket提供额外的功能

        itrans = thrift.transport.TTransport.TMemoryBuffer(event.msg)
        otrans = thrift.transport.TTransport.TMemoryBuffer()
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)

        try:
            self.processor.process(iprot, oprot)
        except Exception:
            logging.exception("Exception while processing request")

        msg = otrans.getvalue()

        # 将处理完毕的数据返回
        self._events.emit(msg, event.id)


    def _acceptor(self):
        # run
        #    ---> _acceptor
        #                   ---> _handle_request
        #
        while True:
            event = self._events.recv()
            self._task_pool.spawn(self._handle_request, event)

    def run(self):
        # 1. 异步接受新的zeromq message
        self._acceptor_task = gevent.spawn(self._acceptor)

        # 2. 等待结束
        try:
            self._acceptor_task.get()
        finally:
            self.stop()
            self._task_pool.join(raise_error=True)

    def stop(self):
        if self._acceptor_task is not None:
            self._acceptor_task.kill()
            self._acceptor_task = None

class Server(SocketBase, ServerBase):
    """
        Server是如何实现的
        主要是API, 如何将ServerBase的框架和Methods等结合起来
    """
    def __init__(self, methods=None, name=None, context=None, pool_size=None,
            heartbeat=5):
        SocketBase.__init__(self, zmq.ROUTER, context)

        # 两种做法:
        # 1. 扩展Server
        # 2. 定义一个新的对象: methods, 例如: methods = RPCObject()
        if methods is None:
            methods = self

        name = name or ServerBase._extract_name(methods)

        methods = ServerBase._filter_methods(Server, self, methods)

        ServerBase.__init__(self, self._events, methods, name, context, pool_size, heartbeat)

    def close(self):
        ServerBase.close(self)
        SocketBase.close(self)
