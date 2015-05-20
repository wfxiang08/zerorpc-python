#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Simple request-reply broker
#
# Author: Lev Givon <lev(at)columbia(dot)edu>

# 参考: http://zguide.zeromq.org/py:rrbroker

import zmq

# Prepare our context and sockets
context = zmq.Context()
frontend = context.socket(zmq.ROUTER)
backend = context.socket(zmq.DEALER)

frontend.bind("tcp://*:5559")
backend.bind("tcp://*:5560")

# Initialize poll set
poller = zmq.Poller()
poller.register(frontend, zmq.POLLIN)
poller.register(backend, zmq.POLLIN)

# Switch messages between sockets
while True:
    socks = dict(poller.poll())

    if socks.get(frontend) == zmq.POLLIN:
        # 如果有来来自: frontend的message, 则转发给后端?
        message = frontend.recv_multipart()
        backend.send_multipart(message)

    if socks.get(backend) == zmq.POLLIN:
        # 如果有来自后端的消息，则转发给前端
        message = backend.recv_multipart()
        frontend.send_multipart(message)

# zerohub通过zmq自带的函数来实现了? zerohub