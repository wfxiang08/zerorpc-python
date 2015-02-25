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


from nose.tools import assert_raises
import gevent
import sys

from zerorpc import zmq
import zerorpc
from testutils import teardown, random_ipc_endpoint


def test_close_server_hbchan():
    endpoint = random_ipc_endpoint()

    # 创建Server
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    # 创建Client
    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    client_channel = client.channel()
    client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)
    client_hbchan.emit('openthat', None)

    event = server.recv()
    server_channel = server.channel(event)
    server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)
    server_hbchan.recv()

    gevent.sleep(3)
    print 'CLOSE SERVER SOCKET!!!'
    server_hbchan.close()

    # 服务器关闭服务
    if sys.version_info < (2, 7):
        assert_raises(zerorpc.LostRemote, client_hbchan.recv)
    else:
        with assert_raises(zerorpc.LostRemote):
            client_hbchan.recv()
    print 'CLIENT LOST SERVER :)'
    client_hbchan.close()
    server.close()
    client.close()


def test_close_client_hbchan():
    endpoint = random_ipc_endpoint()
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    client_channel = client.channel()
    client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)
    client_hbchan.emit('openthat', None)

    event = server.recv()
    server_channel = server.channel(event)
    server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)
    server_hbchan.recv()

    gevent.sleep(3)
    print 'CLOSE CLIENT SOCKET!!!'
    client_hbchan.close()
    if sys.version_info < (2, 7):
        assert_raises(zerorpc.LostRemote, server_hbchan.recv)
    else:
        with assert_raises(zerorpc.LostRemote):
            server_hbchan.recv()
    print 'SERVER LOST CLIENT :)'
    server_hbchan.close()
    server.close()
    client.close()


def test_heartbeat_can_open_channel_server_close():
    endpoint = random_ipc_endpoint()
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    client_channel = client.channel()
    client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)

    event = server.recv()
    server_channel = server.channel(event)
    server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)

    gevent.sleep(3)
    print 'CLOSE SERVER SOCKET!!!'
    server_hbchan.close()

    # 客户端失去心跳
    if sys.version_info < (2, 7):
        assert_raises(zerorpc.LostRemote, client_hbchan.recv)
    else:
        with assert_raises(zerorpc.LostRemote):
            client_hbchan.recv()
    print 'CLIENT LOST SERVER :)'
    client_hbchan.close()
    server.close()
    client.close()


def test_heartbeat_can_open_channel_client_close():
    endpoint = random_ipc_endpoint()
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    # channel的获取:
    # 主动创建一个Channel, 并且用该Channel发送Event
    # 被动获取一个Channel, 从已有的Event获取Channel
    client_channel = client.channel()
    client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)

    event = server.recv()
    server_channel = server.channel(event)
    server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)

    gevent.sleep(3)
    print 'CLOSE CLIENT SOCKET!!!'
    client_hbchan.close()
    client.close()
    if sys.version_info < (2, 7):
        assert_raises(zerorpc.LostRemote, server_hbchan.recv)
    else:
        with assert_raises(zerorpc.LostRemote):
            server_hbchan.recv()
    print 'SERVER LOST CLIENT :)'
    server_hbchan.close()
    server.close()


def test_do_some_req_rep():
    endpoint = random_ipc_endpoint()
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    client_channel = client.channel()
    client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)

    event = server.recv()
    server_channel = server.channel(event)
    server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)

    def client_do():
        for x in xrange(20):
            client_hbchan.emit('add', (x, x * x))
            event = client_hbchan.recv()
            assert event.name == 'OK'
            assert list(event.args) == [x + x * x]
        client_hbchan.close()

    client_task = gevent.spawn(client_do)

    def server_do():
        for x in xrange(20):
            event = server_hbchan.recv()
            assert event.name == 'add'
            server_hbchan.emit('OK', (sum(event.args),))
        server_hbchan.close()

    server_task = gevent.spawn(server_do)

    server_task.get()
    client_task.get()
    client.close()
    server.close()


def test_do_some_req_rep_lost_server():
    endpoint = random_ipc_endpoint()
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    def client_do():
        print 'running'
        client_channel = client.channel()
        client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)
        for x in xrange(10):
            client_hbchan.emit('add', (x, x * x))
            event = client_hbchan.recv()
            assert event.name == 'OK'
            assert list(event.args) == [x + x * x]
        client_hbchan.emit('add', (x, x * x))
        if sys.version_info < (2, 7):
            assert_raises(zerorpc.LostRemote, client_hbchan.recv)
        else:
            with assert_raises(zerorpc.LostRemote):
                client_hbchan.recv()
        client_hbchan.close()

    client_task = gevent.spawn(client_do)

    def server_do():
        event = server.recv()
        server_channel = server.channel(event)
        server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)
        for x in xrange(10):
            event = server_hbchan.recv()
            assert event.name == 'add'
            server_hbchan.emit('OK', (sum(event.args),))
        server_hbchan.close()

    server_task = gevent.spawn(server_do)

    server_task.get()
    client_task.get()
    client.close()
    server.close()


def test_do_some_req_rep_lost_client():
    endpoint = random_ipc_endpoint()
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    def client_do():
        client_channel = client.channel()
        client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)

        for x in xrange(10):
            client_hbchan.emit('add', (x, x * x))
            # 等待事件的发生
            event = client_hbchan.recv()
            assert event.name == 'OK'
            assert list(event.args) == [x + x * x]
        client_hbchan.close()

    client_task = gevent.spawn(client_do)

    def server_do():
        event = server.recv()
        server_channel = server.channel(event)
        server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)

        for x in xrange(10):
            # 直接测试通信的协议(不涉及RPC)
            event = server_hbchan.recv()
            assert event.name == 'add'
            server_hbchan.emit('OK', (sum(event.args),))

        if sys.version_info < (2, 7):
            assert_raises(zerorpc.LostRemote, server_hbchan.recv)
        else:
            with assert_raises(zerorpc.LostRemote):
                server_hbchan.recv()
        server_hbchan.close()

    server_task = gevent.spawn(server_do)

    # 等待两个事件的返回
    server_task.get()
    client_task.get()
    client.close()
    server.close()


def test_do_some_req_rep_client_timeout():
    endpoint = random_ipc_endpoint()
    server_events = zerorpc.Events(zmq.ROUTER)
    server_events.bind(endpoint)
    server = zerorpc.ChannelMultiplexer(server_events)

    client_events = zerorpc.Events(zmq.DEALER)
    client_events.connect(endpoint)
    client = zerorpc.ChannelMultiplexer(client_events, ignore_broadcast=True)

    def client_do():
        # client创建一个chanel, 并在上面创建 HeartBeatOnChannel
        # response_to
        # Each consecutive event on a channel will have the header field "response_to" set to the channel id:
        # 第一个Event的Id, 之后所有的Event, 无论是在服务器还是在Client, 都共享相同的数据
        #
        client_channel = client.channel()

        # 主动心跳?
        client_hbchan = zerorpc.HeartBeatOnChannel(client_channel, freq=2)

        if sys.version_info < (2, 7):
            def _do_with_assert_raises():
                for x in xrange(10):
                    # client发送Event到server, 并且等待服务器的返回
                    client_hbchan.emit('sleep', (x,))
                    event = client_hbchan.recv(timeout=3) # x: 0, 1, 2正常返回, x: 3+ 返回异常, TimeoutExpired

                    # 心跳的作用?

                    print event
                    assert event.name == 'OK'
                    assert list(event.args) == [x]
            assert_raises(zerorpc.TimeoutExpired, _do_with_assert_raises)
        else:
            with assert_raises(zerorpc.TimeoutExpired):
                for x in xrange(10):
                    client_hbchan.emit('sleep', (x,))
                    event = client_hbchan.recv(timeout=3)
                    print event
                    assert event.name == 'OK'
                    assert list(event.args) == [x]
        client_hbchan.close()

    client_task = gevent.spawn(client_do)

    def server_do():
        event = server.recv()
        server_channel = server.channel(event)
        server_hbchan = zerorpc.HeartBeatOnChannel(server_channel, freq=2)

        if sys.version_info < (2, 7):
            def _do_with_assert_raises():
                for x in xrange(20):
                    event = server_hbchan.recv()
                    print event

                    assert event.name == 'sleep'
                    gevent.sleep(event.args[0])
                    server_hbchan.emit('OK', event.args)
            assert_raises(zerorpc.LostRemote, _do_with_assert_raises)
        else:
            with assert_raises(zerorpc.LostRemote):
                for x in xrange(20):
                    # 服务器接口大量的Event, 但是中途发现: client 关闭, 心跳结束，然后服务器的请求也就终止
                    event = server_hbchan.recv()
                    print event
                    assert event.name == 'sleep'
                    gevent.sleep(event.args[0])
                    server_hbchan.emit('OK', event.args)
        server_hbchan.close()

    server_task = gevent.spawn(server_do)

    server_task.get()
    client_task.get()
    client.close()
    server.close()
