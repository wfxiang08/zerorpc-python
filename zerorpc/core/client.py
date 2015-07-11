# -*- coding: utf-8 -*-
from __future__ import  absolute_import
from logging import getLogger
from StringIO import StringIO

from thrift.transport.TTransport import CReadableTransport
from thrift.transport.TTransport import TTransportBase

from zerorpc.core.socket import SocketBase

logger = getLogger(__name__)


# class TProtocol(SocketBase):
#
#     def __init__(self, connect_to=None, context=None, timeout=30):
#         # DEALER 可以同时接受多个请求?
#         SocketBase.__init__(self, zmq.DEALER, context=context)
#         self.context = context
#         self.timetout = timeout
#         self.connect_to = connect_to
#
#
#         # 在初始化是创建连接
#         if connect_to:
#             self.connect(connect_to)
#
#     def close(self):
#         SocketBase.close(self)



# socktype = zmq.REQ
# ctx = zmq.Context()
# transport = TZmqTransport(ctx, endpoint, socktype)
# protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocolAccelerated(transport)
# client = storage.Storage.Client(protocol)
# transport.open()




class TZmqTransport(TTransportBase, CReadableTransport, SocketBase):
    def __init__(self, ctx, endpoint, sock_type): # zmq.DEALER
        SocketBase.__init__(self, sock_type, context=ctx)
        self._endpoint = endpoint
        self._wbuf = StringIO()
        self._rbuf = StringIO()

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
        self._events.emit(msg) # client似乎没有id

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