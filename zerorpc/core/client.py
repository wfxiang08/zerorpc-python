# -*- coding: utf-8 -*-
from __future__ import  absolute_import
from logging import getLogger
from StringIO import StringIO

from thrift.transport.TTransport import CReadableTransport
from thrift.transport.TTransport import TTransportBase
import zmq

from zerorpc.core.socket import SocketBase


logger = getLogger(__name__)


# 用法
# socktype = zmq.REQ
# service = None
# transport = TZmqTransport(endpoint, socktype, service=service)
# protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(transport)
#
# 然后构建Thrift Client
# client = storage.Storage.Client(protocol)
# transport.open()




class TZmqTransport(TTransportBase, CReadableTransport, SocketBase):
    def __init__(self, endpoint, sock_type = zmq.DEALER, ctx = None, service = None): # zmq.DEALER
        SocketBase.__init__(self, sock_type, context=ctx)

        self._endpoint = endpoint
        self._wbuf = StringIO()
        self._rbuf = StringIO()
        self.service = service

    def open(self):
        self.connect(self._endpoint)

    def read(self, size):
        # buf中的数据处理完毕了，说明一个message应该处理完毕了
        ret = self._rbuf.read(size)
        if len(ret) != 0:
            return ret
        self._read_message()
        return self._rbuf.read(size)

    def _read_message(self):
        event = self._events.recv()
        self._rbuf = StringIO(event.msg)

    def write(self, buf):
        self._wbuf.write(buf)

    def flush(self):
        msg = self._wbuf.getvalue()
        self._wbuf = StringIO()

        # 将Thrift转换成为zeromq
        self._events.emit(msg, self.service) # client似乎没有id

    # Implement the CReadableTransport interface.
    @property
    def cstringio_buf(self):
        return self._rbuf

    # NOTE: This will probably not actually work.
    def cstringio_refill(self, prefix, reqlen):
        while len(prefix) < reqlen:
            self.read_message()
            prefix += self._rbuf.getvalue()
        self._rbuf = StringIO(prefix)
        return self._rbuf