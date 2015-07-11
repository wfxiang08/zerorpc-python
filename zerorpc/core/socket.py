# -*- coding: utf-8 -*-

from __future__ import absolute_import
from zerorpc.context import Context
from zerorpc.events import Events


class SocketBase(object):
    """
    定义了Socket相关的操作: connect, bind以及_events
    """

    def __init__(self, zmq_socket_type, context=None):
        self._context = context or Context.get_instance() # 获取zeromq context
        self._events = Events(zmq_socket_type, context)

    def close(self):
        self._events.close()

    # 通过_events来connect, bind服务
    def connect(self, endpoint, resolve=True):
        return self._events.connect(endpoint, resolve)

    def bind(self, endpoint, resolve=True):
        return self._events.bind(endpoint, resolve)
