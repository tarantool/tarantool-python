# -*- coding: utf-8 -*-

import abc
import itertools
import queue
import threading
import time
import typing
from dataclasses import dataclass, field
from enum import Enum

from tarantool.connection import Connection, ConnectionInterface
from tarantool.const import (
    CONNECTION_TIMEOUT,
    POOL_INSTANCE_RECONNECT_DELAY,
    POOL_INSTANCE_RECONNECT_MAX_ATTEMPTS,
    POOL_REFRESH_DELAY,
    SOCKET_TIMEOUT
)
from tarantool.error import (
    ClusterConnectWarning,
    PoolTolopogyError,
    PoolTolopogyWarning,
    ConfigurationError,
    DatabaseError,
    NetworkError,
    NetworkWarning,
    tnt_strerror,
    warn
)
from tarantool.utils import ENCODING_DEFAULT
from tarantool.mesh_connection import validate_address


class Mode(Enum):
    ANY = 1
    RW = 2
    RO = 3
    PREFER_RW = 4
    PREFER_RO = 5


class Status(Enum):
    HEALTHY = 1
    UNHEALTHY = 2


@dataclass
class InstanceState():
    status: Status = Status.UNHEALTHY
    ro: typing.Optional[bool] = None


def QueueFactory():
    return queue.Queue(maxsize=1)


@dataclass
class PoolUnit():
    addr: dict
    conn: Connection
    input_queue: queue.Queue = field(default_factory=QueueFactory)
    output_queue: queue.Queue = field(default_factory=QueueFactory)
    thread: typing.Optional[threading.Thread] = None
    state: InstanceState = field(default_factory=InstanceState)
    # request_processing_enabled is used to stop requests processing
    # in background thread on close or destruction.
    request_processing_enabled: bool = False


# Based on https://realpython.com/python-interface/
class StrategyInterface(metaclass=abc.ABCMeta):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, '__init__') and
                callable(subclass.__init__) and
                hasattr(subclass, 'update') and
                callable(subclass.update) and
                hasattr(subclass, 'getnext') and
                callable(subclass.getnext) or
                NotImplemented)

    @abc.abstractmethod
    def __init__(self, pool):
        raise NotImplementedError

    @abc.abstractmethod
    def update(self):
        raise NotImplementedError

    @abc.abstractmethod
    def getnext(self, mode):
        raise NotImplementedError

class RoundRobinStrategy(StrategyInterface):
    """
    Simple round-robin connection rotation
    """
    def __init__(self, pool):
        self.ANY_iter = None
        self.RW_iter = None
        self.RO_iter = None
        self.pool = pool
        self.rebuild_needed = True

    def build(self):
        ANY_pool = []
        RW_pool = []
        RO_pool = []

        for key in self.pool:
            state = self.pool[key].state

            if state.status == Status.UNHEALTHY:
                continue

            ANY_pool.append(key)

            if state.ro == False:
                RW_pool.append(key)
            else:
                RO_pool.append(key)

        if len(ANY_pool) > 0:
            self.ANY_iter = itertools.cycle(ANY_pool)
        else:
            self.ANY_iter = None

        if len(RW_pool) > 0:
            self.RW_iter = itertools.cycle(RW_pool)
        else:
            self.RW_iter = None

        if len(RO_pool) > 0:
            self.RO_iter = itertools.cycle(RO_pool)
        else:
            self.RO_iter = None

        self.rebuild_needed = False

    def update(self):
        self.rebuild_needed = True

    def getnext(self, mode):
        if self.rebuild_needed:
            self.build()

        if mode == Mode.ANY:
            if self.ANY_iter is not None:
                return next(self.ANY_iter)
            else:
                raise PoolTolopogyError("Can't find healthy instance in pool")
        elif mode == Mode.RW:
            if self.RW_iter is not None:
                return next(self.RW_iter)
            else:
                raise PoolTolopogyError("Can't find healthy rw instance in pool")
        elif mode == Mode.RO:
            if self.RO_iter is not None:
                return next(self.RO_iter)
            else:
                raise PoolTolopogyError("Can't find healthy ro instance in pool")
        elif mode == Mode.PREFER_RO:
            if self.RO_iter is not None:
                return next(self.RO_iter)
            elif self.RW_iter is not None:
                return next(self.RW_iter)
            else:
                raise PoolTolopogyError("Can't find healthy instance in pool")
        elif mode == Mode.PREFER_RW:
            if self.RW_iter is not None:
                return next(self.RW_iter)
            elif self.RO_iter is not None:
                return next(self.RO_iter)
            else:
                raise PoolTolopogyError("Can't find healthy instance in pool")


@dataclass
class PoolTask():
    method_name: str
    args: tuple
    kwargs: dict


class ConnectionPool(ConnectionInterface):
    '''
    Represents pool of connections to the cluster of Tarantool servers.

    ConnectionPool API is the same as a plain Connection API.
    On each request, a connection is chosen to execute this request.
    Connection is selected based on request mode:
    * Mode.ANY chooses any instance.
    * Mode.RW chooses an RW instance.
    * Mode.RO chooses an RO instance.
    * Mode.PREFER_RW chooses an RW instance, if possible, RO instance
      otherwise.
    * Mode.PREFER_RO chooses an RO instance, if possible, RW instance
      otherwise.
    All requests that are guaranteed to write (insert, replace, delete,
    upsert, update) use RW mode by default. select uses ANY by default. You
    can set the mode explicitly. call, eval, execute and ping requests
    require to set the mode explicitly.
    '''
    def __init__(self,
                 addrs,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=POOL_INSTANCE_RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=POOL_INSTANCE_RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 call_16=False,
                 connection_timeout=CONNECTION_TIMEOUT,
                 strategy_class=RoundRobinStrategy,
                 refresh_delay=POOL_REFRESH_DELAY):
        '''
        Initialize connections to the cluster of servers.

        :param list addrs: List of {host: , port:} dictionaries,
        describing server addresses.
        :user str Username used to authenticate. User must be able
        to call box.info function. For example, to give grants to 
        'guest' user, evaluate
          box.schema.func.create('box.info')
          box.schema.user.grant('guest', 'execute', 'function', 'box.info')
        on Tarantool instances.
        :param int reconnect_max_attempts: Max attempts to reconnect
        for each connection in the pool. Be careful with reconnect
        parameters in ConnectionPool since every status refresh is
        also a request with reconnection. Default is 0 (fail after
        first attempt).
        :param float reconnect_delay: Time between reconnect
        attempts for each connection in the pool. Be careful with
        reconnect parameters in ConnectionPool since every status
        refresh is also a request with reconnection. Default is 0.
        :param StrategyInterface strategy_class: Class for choosing
        instance based on request mode. By default, round-robin
        strategy is used.
        :param int refresh_delay: Minimal time between RW/RO status
        refreshes.
        '''

        if not isinstance(addrs, list) or len(addrs) == 0:
            raise ConfigurationError("addrs must be non-empty list")

        # Verify addresses.
        for addr in addrs:
            ok, msg = validate_address(addr)
            if not ok:
                raise ConfigurationError(msg)
        self.addrs = addrs

        # Create connections
        self.pool = {}
        self.refresh_delay = refresh_delay
        self.strategy = strategy_class(self.pool)

        for addr in self.addrs:
            key = self._make_key(addr)
            self.pool[key] = PoolUnit(
                addr=addr,
                conn=Connection(
                    host=addr['host'],
                    port=addr['port'],
                    user=user,
                    password=password,
                    socket_timeout=socket_timeout,
                    reconnect_max_attempts=reconnect_max_attempts,
                    reconnect_delay=reconnect_delay,
                    connect_now=False, # Connect in ConnectionPool.connect()
                    encoding=encoding,
                    call_16=call_16,
                    connection_timeout=connection_timeout)
            )

        if connect_now:
            self.connect()

    def __del__(self):
        self.close()

    def _make_key(self, addr):
        return '{0}:{1}'.format(addr['host'], addr['port'])

    def _get_new_state(self, unit):
        conn = unit.conn

        if conn.is_closed():
            try:
                conn.connect()
            except NetworkError as e:
                msg = "Failed to connect to {0}:{1}".format(
                    unit.addr['host'], unit.addr['port'])
                warn(msg, ClusterConnectWarning)
                return InstanceState(Status.UNHEALTHY)

        try:
            resp = conn.call('box.info')
        except NetworkError as e:
            msg = "Failed to get box.info for {0}:{1}, reason: {2}".format(
                unit.addr['host'], unit.addr['port'], repr(e))
            warn(msg, PoolTolopogyWarning)
            return InstanceState(Status.UNHEALTHY)

        try:
            ro = resp.data[0]['ro']
        except (IndexError, KeyError) as e:
            msg = "Incorrect box.info response from {0}:{1}".format(
                unit.addr['host'], unit.addr['port'])
            warn(msg, PoolTolopogyWarning)
            return InstanceState(Status.UNHEALTHY)

        try:
            status = resp.data[0]['status']

            if status != 'running':
                msg = "{0}:{1} instance status is not 'running'".format(
                    unit.addr['host'], unit.addr['port'])
                warn(msg, PoolTolopogyWarning)
                return InstanceState(Status.UNHEALTHY)
        except (IndexError, KeyError) as e:
            msg = "Incorrect box.info response from {0}:{1}".format(
                unit.addr['host'], unit.addr['port'])
            warn(msg, PoolTolopogyWarning)
            return InstanceState(Status.UNHEALTHY)

        return InstanceState(Status.HEALTHY, ro)

    def _refresh_state(self, key):
        unit = self.pool[key]

        state = self._get_new_state(unit)
        if state != unit.state:
            unit.state = state
            self.strategy.update()

    def close(self):
        for unit in self.pool.values():
            unit.request_processing_enabled = False
            unit.thread.join()

            if not unit.conn.is_closed():
                unit.conn.close()

    def is_closed(self):
        return all(unit.request_processing_enabled == False for unit in self.pool.values())

    def _request_process_loop(self, key, unit, last_refresh):
        while unit.request_processing_enabled:
            if not unit.input_queue.empty():
                task = unit.input_queue.get()
                method = getattr(Connection, task.method_name)
                try:
                    resp = method(unit.conn, *task.args, **task.kwargs)
                except Exception as e:
                    unit.output_queue.put(e)
                else:
                    unit.output_queue.put(resp)

            now = time.time()

            if now - last_refresh > self.refresh_delay:
                self._refresh_state(key)
                last_refresh = time.time()

    def connect(self):
        for key in self.pool:
            unit = self.pool[key]

            self._refresh_state(key)
            last_refresh = time.time()

            unit.thread = threading.Thread(
                target=self._request_process_loop,
                args=(key, unit, last_refresh),
                daemon=True,
            )
            unit.request_processing_enabled = True
            unit.thread.start()

    def _send(self, mode, method_name, *args, **kwargs):
        key = self.strategy.getnext(mode)
        unit = self.pool[key]

        task = PoolTask(method_name=method_name, args=args, kwargs=kwargs)

        unit.input_queue.put(task)
        resp = unit.output_queue.get()

        if isinstance(resp, Exception):
            raise resp

        return resp

    def call(self, func_name, *args, mode=None):
        '''
        :param tarantool.Mode mode: Request mode.
        '''

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'call', func_name, *args)

    def eval(self, expr, *args, mode=None):
        '''
        :param tarantool.Mode mode: Request mode.
        '''

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'eval', expr, *args)

    def replace(self, space_name, values, *, mode=Mode.RW):
        '''
        :param tarantool.Mode mode: Request mode (default is RW).
        '''

        return self._send(mode, 'replace', space_name, values)

    def insert(self, space_name, values, *, mode=Mode.RW):
        '''
        :param tarantool.Mode mode: Request mode (default is RW).
        '''

        return self._send(mode, 'insert', space_name, values)

    def delete(self, space_name, key, *, mode=Mode.RW, **kwargs):
        '''
        :param tarantool.Mode mode: Request mode (default is RW).
        '''

        return self._send(mode, 'delete', space_name, key, **kwargs)

    def upsert(self, space_name, tuple_value, op_list, *, mode=Mode.RW, **kwargs):
        '''
        :param tarantool.Mode mode: Request mode (default is RW).
        '''

        return self._send(mode, 'upsert', space_name, tuple_value,
            op_list, **kwargs)

    def update(self, space_name, key, op_list, *, mode=Mode.RW, **kwargs):
        '''
        :param tarantool.Mode mode: Request mode (default is RW).
        '''

        return self._send(mode, 'update', space_name, key, 
            op_list, **kwargs)

    def ping(self, *, mode=None, **kwargs):
        '''
        :param tarantool.Mode mode: Request mode.
        '''

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'ping', **kwargs)

    def select(self, space_name, key, *, mode=Mode.ANY, **kwargs):
        '''
        :param tarantool.Mode mode: Request mode (default is
        ANY).
        '''

        return self._send(mode, 'select', space_name, key, **kwargs)

    def execute(self, query, params=None, *, mode=None):
        '''
        :param tarantool.Mode mode: Request mode (default is RW).
        '''

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'execute', query, params)
