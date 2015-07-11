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
import time
from zerorpc import events, Events

import zerorpc.gevent_zmq as zmq
from zerorpc.core.socket import SocketBase
from zerorpc.context import Context

HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 1
INTERVAL_INIT = 1
INTERVAL_MAX = 32

#  Paranoid Pirate Protocol constants


logger = getLogger(__name__)

class Server(object):

    def __init__(self, events, processor, zmq_socket_type, context=None, pool_size=None):
        self._context = context or Context.get_instance() # 获取zeromq context
        self._events = Events(zmq_socket_type, context)


        # thrift
        self.processor = processor # thrift processor
        self.inputProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()
        self.outputProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()

        # zeromq
        self._context = context or Context.get_instance()

        # gevent
        self._task_pool = gevent.pool.Pool(size=pool_size)
        self._acceptor_task = None

        if events.mode_ppworker:
            self.liveness = HEARTBEAT_LIVENESS
            self.interval = INTERVAL_INIT
            self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL


            self._events.create_worker_socket(self.poller)

        self.endpoint = None
        self.resolve = True

        # 通过_events来connect, bind服务
    def connect(self, endpoint, resolve=True):
        self.endpoint = endpoint
        self.resolve = resolve
        return self._events.connect(endpoint, resolve)

    def bind(self, endpoint, resolve=True):
        self.endpoint = endpoint
        self.resolve = resolve
        return self._events.bind(endpoint, resolve)


    def close(self):
        self._events.close()
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
        #
        # server的工作模式:
        #   1. Demo服务器可以简单地启动一个ZeroRpcServer, 然后也不用考虑网络 io的一点点时间开销
        #   2. 线上服务器, ZeroRpcServer之前添加了一个queue或load balance, 因此网络io的时间也可以忽略
        #
        while True:
            if events.mode_ppworker:
                socks = dict(self.poller.poll(HEARTBEAT_INTERVAL * 1000))
                if socks.get(self._events.socket) == zmq.POLLIN:
                    event = self._events.recv()
                    if events.mode_ppworker and len(event.msg) == 1 and event.msg[0] == events.PPP_HEARTBEAT:
                        self.liveness = HEARTBEAT_LIVENESS
                    else:
                        self.liveness = HEARTBEAT_LIVENESS
                        self._task_pool.spawn(self._handle_request, event)
                    self.interval = INTERVAL_INIT
                else:
                    # timeout(太长时间没有回应)
                    self.liveness -= 1
                    if self.liveness == 0:
                        # 反正都没啥事了，等待就等待
                        time.sleep(self.interval)

                    if self.interval < INTERVAL_MAX:
                        self.interval *= 2

                    # 重新注册
                    self._events.reconnect()
                    self.poller.unregister(worker)

                    worker.setsockopt(zmq.LINGER, 0)
                    worker.close()
                    worker = worker_socket(context, poller)
                    self.liveness = HEARTBEAT_LIVENESS
            else:
                event = self._events.recv()
                self._task_pool.spawn(self._handle_request, event)

    def run(self):
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

