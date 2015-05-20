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


import time
import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock

from .exceptions import *  # noqa


class HeartBeatOnChannel(object):
    """
        对原来的心跳进行包装， Proxy设计模式

        工作原理:
        1. HeartBeatOnChannel 实现了 Channel的所有的接口
        2. _input_queue 实现了  _recv_task和recv之间的异步交互
        3. _heartbeat_task: 主动，被动的差别?
           在收到Heartbeat之后都会创建 heartbeat, 只不过差别在于谁先发起任务
    """
    def __init__(self, channel, freq=5, passive=False):
        # 主角: channel
        self._channel = channel


        self._heartbeat_freq = freq
        self._input_queue = gevent.queue.Channel()

        self._remote_last_hb = None
        self._lost_remote = False

        self._recv_task = gevent.spawn(self._recver)

        self._heartbeat_task = None

        # 当前执行的任务(Channel, socket fd)
        self._parent_coroutine = gevent.getcurrent()
        self._compat_v2 = None

        # 默认使用主动心跳
        if not passive:
            self._start_heartbeat()


        # 关注:
        # _start_heartbeat
        # _heartbeat_task
        # _recver
        # _heartbeat
        #
        """
            1. _heartbeat:
                定时发送心跳，并且检测是否超时
            2. _start_heartbeat
                开启心跳任务, 如果任务在心跳时间之前完成，那么: 就不会发送心跳信号，网络也没有额外的开销

            3. _recver
                异步接受信号，在实现 .recv功能之外，还能处理心跳等信号？
        """


    @property
    def recv_is_available(self):
        return self._channel.recv_is_available

    def __del__(self):
        self.close()

    def close(self):
        if self._heartbeat_task is not None:
            self._heartbeat_task.kill()
            self._heartbeat_task = None

        if self._recv_task is not None:
            self._recv_task.kill()
            self._recv_task = None

        if self._channel is not None:
            self._channel.close()
            self._channel = None

    def _heartbeat(self):
        while True:
            gevent.sleep(self._heartbeat_freq)

            if self._remote_last_hb is None:
                self._remote_last_hb = time.time()

            # 检测是否失去heartbeat, 如果失去心跳，则终止当前的连接
            if time.time() > self._remote_last_hb + self._heartbeat_freq * 2:
                self._lost_remote = True
                # 告诉Parent CoRoutine, 超时了
                gevent.kill(self._parent_coroutine, self._lost_remote_exception())
                break

            # 发送消息到 Client?
            self._channel.emit('_zpc_hb', (0,))  # 0 -> compat with protocol v2

    def _start_heartbeat(self):
        """
            创建heartbeat任务
        """
        if self._heartbeat_task is None and self._heartbeat_freq is not None:
            self._heartbeat_task = gevent.spawn(self._heartbeat)

    def _recver(self):
        while True:
            event = self._channel.recv()

            # 不考虑兼容性: _compat_v2 = None 恒成立
            if self._compat_v2 is None:
                self._compat_v2 = event.header.get('v', 0) < 3

            if event.name == '_zpc_hb':
                # 接收到心跳
                self._remote_last_hb = time.time()

                # 如果是被动心跳，则自己也要主动发起心跳
                self._start_heartbeat()

                # Skip
                if self._compat_v2:
                    event.name = '_zpc_more'
                    self._input_queue.put(event)
            else:
                self._input_queue.put(event)

    def _lost_remote_exception(self):
        return LostRemote('Lost remote after {0}s heartbeat'.format(
            self._heartbeat_freq * 2))

    # create_event, emit_event, emit, recv都是必须实现的，大部分实现只是简单地通过代理模式来处理
    def create_event(self, name, args, xheader=None):

        # 特殊处理心跳信号
        if self._compat_v2 and name == '_zpc_more':
            name = '_zpc_hb'

        return self._channel.create_event(name, args, xheader)


    def emit_event(self, event):
        # 特殊处理超时信号
        if self._lost_remote:
            raise self._lost_remote_exception()
        self._channel.emit_event(event)

    def emit(self, name, args, xheader=None):
        event = self.create_event(name, args, xheader)
        self.emit_event(event)

    def recv(self, timeout=None):
        # 如果超时，也就不再等待
        if self._lost_remote:
            raise self._lost_remote_exception()

        # 生产者，消费者模式
        try:
            event = self._input_queue.get(timeout=timeout)
        except gevent.queue.Empty:
            raise TimeoutExpired(timeout)

        return event

    @property
    def channel(self):
        return self._channel

    @property
    def context(self):
        return self._channel.context
