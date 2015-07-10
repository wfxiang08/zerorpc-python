# -*- coding: utf-8 -*-
import sys
import traceback
from logging import getLogger

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock
from thrift.protocol import TBinaryProtocol

import gevent_zmq as zmq
from .exceptions import TimeoutExpired, RemoteError, LostRemote
from .channel import ChannelMultiplexer
from .socket import SocketBase
from .context import Context
import patterns


logger = getLogger(__name__)

class ServerBase(object):

    def __init__(self, channel, processor, name=None, context=None, pool_size=None):
        self._multiplexer = ChannelMultiplexer(channel)
        self.processor = processor # thrift processor

        self._context = context or Context.get_instance()
        self._name = name or self._extract_name()

        self._task_pool = gevent.pool.Pool(size=pool_size)
        self._acceptor_task = None

        self.inputProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()
        self.outputProtocolFactory = TBinaryProtocol.TBinaryProtocolFactory()


    def close(self):
        self.stop()
        self._multiplexer.close()



    def __call__(self, method, *args):
        """
        如何执行方法调用:
        """
        if method not in self._methods:
            raise NameError(method)

        # 访问dict的对应方法
        return self._methods[method](*args)

    def _print_traceback(self, protocol_v1, exc_infos):
        logger.exception('')

        exc_type, exc_value, exc_traceback = exc_infos
        if protocol_v1:
            return (repr(exc_value),)
        human_traceback = traceback.format_exc()
        name = exc_type.__name__
        human_msg = str(exc_value)
        return (name, human_msg, human_traceback)

    def _async_task(self, initial_event):
        """
            读取到一个event之后, 就创建一个_async_task(task执行完毕，则自动删除)

            该Event会使用一个独立的 fd 和client进行通信
            fd = socket_fd.listen()


            同时在该链接上创建一个: HeartBeatOnChannel的Event, 通过心跳保持连接
        """
        protocol_v1 = initial_event.header.get('v', 1) < 2

        # 将event和channel关联
        #

        # 新的Event?
        # 这个地方是不是有bug, 后?
        channel = self._multiplexer.channel(initial_event)

        exc_infos = None

        # 或者这个地方?
        event = channel.recv()

        try:
            self._context.hook_load_task_context(event.header)

            # 一般的Task是调用某个函数
            functor = self._methods.get(event.name, None)
            if functor is None:
                raise NameError(event.name)

            # ReqRep#process_call
            # 将functor交给: bufchan去处理
            #
            functor.pattern.process_call(self._context, channel, event, functor)

        except LostRemote:
            # 失去Client心跳
            exc_infos = list(sys.exc_info())
            self._print_traceback(protocol_v1, exc_infos)
        except Exception:
            exc_infos = list(sys.exc_info())
            human_exc_infos = self._print_traceback(protocol_v1, exc_infos)

            reply_event = channel.create_event('ERR', human_exc_infos, self._context.hook_get_task_context())
            self._context.hook_server_inspect_exception(event, reply_event, exc_infos)
            channel.emit_event(reply_event)
        finally:
            del exc_infos
            channel.close()

    def _acceptor(self):

        # 不停地接受Event, 并且"并发地"处理Event
        while True:
            # 服务器模式下，只会返回新的连接
            initial_event = self._multiplexer.recv()

            # 读取到event, 将: (_async_task, initial_event）封装成为一个Greenlet, 放入Pool
            #
            # Greenlet可以认为类似一个thread, 在spawn之后就会被自动执行
            # _task_pool 将部分greenlet自动管理，限制最大的并发度
            #

            # 对于Server, 监听新的events
            # 对于Client, 监听所有的数据(SKIP, 因为在Server中)
            self._task_pool.spawn(self._async_task, initial_event)

    def run(self):
        """
            _acceptor作为一个fd的操作，被封装成为一个event

            大致的工作模式:
                _acceptor不定地运转， 直接由 gevent来调度，然后得到的initial_event交给 _task_pool去统一调度

        """
        self._acceptor_task = gevent.spawn(self._acceptor)
        try:
            # 返回: greenlet的结果
            #      一般_acceptor为死循环，因此会一直堵在这个地方
            self._acceptor_task.get()
        finally:
            self.stop()

            # 等待所有的事情做完了，则关闭服务
            self._task_pool.join(raise_error=True)

    def stop(self):
        if self._acceptor_task is not None:
            self._acceptor_task.kill()
            self._acceptor_task = None


class ClientBase(object):

    # 默认： passive_heartbeat = False， 被动心跳
    def __init__(self, channel, context=None, timeout=30, heartbeat=5, passive_heartbeat=False):

        self._multiplexer = ChannelMultiplexer(channel, ignore_broadcast=True)

        self._context = context or Context.get_instance()
        self._timeout = timeout
        self._heartbeat_freq = heartbeat
        self._passive_heartbeat = passive_heartbeat

    def close(self):
        self._multiplexer.close()

    def _handle_remote_error(self, event):
        exception = self._context.hook_client_handle_remote_error(event)
        if not exception:
            if event.header.get('v', 1) >= 2:
                (name, msg, traceback) = event.args
                exception = RemoteError(name, msg, traceback)
            else:
                (msg,) = event.args
                exception = RemoteError('RemoteError', msg, None)

        return exception

    def _select_pattern(self, event):
        for pattern in patterns.patterns_list:
            if pattern.accept_answer(event):
                return pattern
        msg = 'Unable to find a pattern for: {0}'.format(event)
        raise RuntimeError(msg)

    def _process_response(self, request_event, bufchan, timeout):
        try:
            # client等待数据的返回
            reply_event = bufchan.recv(timeout)

            # 选择请求模式:
            # REQ/REP
            # REQ/STREAM
            pattern = self._select_pattern(reply_event)

            return pattern.process_answer(self._context, bufchan, request_event, reply_event, self._handle_remote_error)

        except TimeoutExpired:
            bufchan.close()
            ex = TimeoutExpired(timeout, 'calling remote method {0}'.format(request_event.name))
            self._context.hook_client_after_request(request_event, None, ex)
            raise ex
        except:
            bufchan.close()
            raise

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


class Pusher(SocketBase):

    def __init__(self, context=None, zmq_socket=zmq.PUSH):
        super(Pusher, self).__init__(zmq_socket, context=context)

    def __call__(self, method, *args):
        self._events.emit(method, args,
                self._context.hook_get_task_context())

    def __getattr__(self, method):
        return lambda *args: self(method, *args)


class Puller(SocketBase):

    def __init__(self, methods=None, context=None, zmq_socket=zmq.PULL):
        super(Puller, self).__init__(zmq_socket, context=context)

        if methods is None:
            methods = self

        self._methods = ServerBase._filter_methods(Puller, self, methods)
        self._receiver_task = None

    def close(self):
        self.stop()
        super(Puller, self).close()

    def __call__(self, method, *args):
        if method not in self._methods:
            raise NameError(method)
        return self._methods[method](*args)

    def _receiver(self):
        while True:
            event = self._events.recv()
            try:
                if event.name not in self._methods:
                    raise NameError(event.name)
                self._context.hook_load_task_context(event.header)
                self._context.hook_server_before_exec(event)
                self._methods[event.name](*event.args)
                # In Push/Pull their is no reply to send, hence None for the
                # reply_event argument
                self._context.hook_server_after_exec(event, None)
            except Exception:
                exc_infos = sys.exc_info()
                try:
                    logger.exception('')
                    self._context.hook_server_inspect_exception(event, None, exc_infos)
                finally:
                    del exc_infos

    def run(self):
        self._receiver_task = gevent.spawn(self._receiver)
        try:
            self._receiver_task.get()
        finally:
            self._receiver_task = None

    def stop(self):
        if self._receiver_task is not None:
            self._receiver_task.kill(block=False)


class Publisher(Pusher):

    def __init__(self, context=None):
        super(Publisher, self).__init__(context=context, zmq_socket=zmq.PUB)


class Subscriber(Puller):

    def __init__(self, methods=None, context=None):
        super(Subscriber, self).__init__(methods=methods, context=context,
                zmq_socket=zmq.SUB)
        self._events.setsockopt(zmq.SUBSCRIBE, '')


def fork_task_context(functor, context=None):
    '''Wrap a functor to transfer context.

        Usage example:
            gevent.spawn(zerorpc.fork_task_context(myfunction), args...)

        The goal is to permit context "inheritance" from a task to another.
        Consider the following example:

            zerorpc.Server receive a new event
              - task1 is created to handle this event this task will be linked
                to the initial event context. zerorpc.Server does that for you.
              - task1 make use of some zerorpc.Client instances, the initial
                event context is transfered on every call.

              - task1 spawn a new task2.
              - task2 make use of some zerorpc.Client instances, it's a fresh
                context. Thus there is no link to the initial context that
                spawned task1.

              - task1 spawn a new fork_task_context(task3).
              - task3 make use of some zerorpc.Client instances, the initial
                event context is transfered on every call.

        A real use case is a distributed tracer. Each time a new event is
        created, a trace_id is injected in it or copied from the current task
        context. This permit passing the trace_id from a zerorpc.Server to
        another via zerorpc.Client.

        The simple rule to know if a task need to be wrapped is:
            - if the new task will make any zerorpc call, it should be wrapped.
    '''
    context = context or Context.get_instance()
    header = context.hook_get_task_context()

    def wrapped(*args, **kargs):
        context.hook_load_task_context(header)
        return functor(*args, **kargs)
    return wrapped


