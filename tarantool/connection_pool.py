"""
This module provides API for interaction with Tarantool servers cluster.
"""

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
    SOCKET_TIMEOUT,
    DEFAULT_SSL_PASSWORD,
    DEFAULT_SSL_PASSWORD_FILE,
)
from tarantool.error import (
    ClusterConnectWarning,
    PoolTolopogyError,
    PoolTolopogyWarning,
    ConfigurationError,
    NetworkError,
    warn
)
from tarantool.utils import ENCODING_DEFAULT
from tarantool.mesh_connection import prepare_address


class Mode(Enum):
    """
    Request mode.
    """

    ANY = 1
    """
    Send a request to any server.
    """

    RW = 2
    """
    Send a request to RW server.
    """

    RO = 3
    """
    Send a request to RO server.
    """

    PREFER_RW = 4
    """
    Send a request to RW server, if possible, RO server otherwise.
    """

    PREFER_RO = 5
    """
    Send a request to RO server, if possible, RW server otherwise.
    """


class Status(Enum):
    """
    Cluster single server status.
    """

    HEALTHY = 1
    """
    Server is healthy: connection is successful,
    `box.info.ro`_ could be extracted, `box.info.status`_ is "running".
    """

    UNHEALTHY = 2
    """
    Server is unhealthy: either connection is failed,
    `box.info`_ cannot be extracted, `box.info.status`_ is not
    "running".
    """


@dataclass
class InstanceState():
    """
    Cluster single server state.
    """

    status: Status = Status.UNHEALTHY
    """
    :type: :class:`~tarantool.connection_pool.Status`
    """
    ro: typing.Optional[bool] = None
    """
    :type: :obj:`bool`, optional
    """


def QueueFactory():
    """
    Build a queue-based channel.
    """

    return queue.Queue(maxsize=1)


@dataclass
class PoolUnit():
    """
    Class to store a Tarantool server metainfo and
    to work with it as a part of connection pool.
    """

    addr: dict
    """
    ``{"host": host, "port": port}`` info.

    :type: :obj:`dict`
    """

    conn: Connection
    """
    :type: :class:`~tarantool.Connection`
    """

    input_queue: queue.Queue = field(default_factory=QueueFactory)
    """
    Channel to pass requests for the server thread.

    :type: :obj:`queue.Queue`
    """

    output_queue: queue.Queue = field(default_factory=QueueFactory)
    """
    Channel to receive responses from the server thread.

    :type: :obj:`queue.Queue`
    """

    thread: typing.Optional[threading.Thread] = None
    """
    Background thread to process requests for the server.

    :type: :obj:`threading.Thread`
    """

    state: InstanceState = field(default_factory=InstanceState)
    """
    Current server state.

    :type: :class:`~tarantool.connection_pool.InstanceState`
    """

    request_processing_enabled: bool = False
    """
    Flag used to stop requests processing requests in the background
    thread on connection close or destruction.

    :type: :obj:`bool`
    """

# Based on https://realpython.com/python-interface/
class StrategyInterface(metaclass=abc.ABCMeta):
    """
    Defines strategy to choose a pool server based on a request mode.
    """

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
        """
        :type: :obj:`list` of
            :class:`~tarantool.connection_pool.PoolUnit` objects
        """

        raise NotImplementedError

    @abc.abstractmethod
    def update(self):
        """
        Refresh the strategy state.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def getnext(self, mode):
        """
        Get a pool server based on a request mode.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`
        """

        raise NotImplementedError

class RoundRobinStrategy(StrategyInterface):
    """
    Simple round-robin pool servers rotation.
    """

    def __init__(self, pool):
        """
        :type: :obj:`list` of
            :class:`~tarantool.connection_pool.PoolUnit` objects
        """

        self.ANY_iter = None
        self.RW_iter = None
        self.RO_iter = None
        self.pool = pool
        self.rebuild_needed = True

    def build(self):
        """
        Initialize (or re-initialize) internal pools to rotate servers
        based on `box.info.ro`_ state.
        """

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
        """
        Set flag to re-initialize internal pools on next
        :meth:`~tarantool.connection_pool.RoundRobinStrategy.getnext`
        call.
        """

        self.rebuild_needed = True

    def getnext(self, mode):
        """
        Get server based on the request mode.

        :param mode: Request mode
        :type mode: :class:`~tarantool.Mode`

        :rtype: :class:`~tarantool.connection_pool.PoolUnit`

        :raise: :exc:`~tarantool.error.PoolTolopogyError`
        """

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
    """
    Store request type and arguments to pass them to some server thread.
    """

    method_name: str
    """
    :class:`~tarantool.Connection` method name.

    :type: :obj:`str`
    """

    args: tuple
    """
    :class:`~tarantool.Connection` method args.

    :type: :obj:`tuple`
    """

    kwargs: dict
    """
    :class:`~tarantool.Connection` method kwargs.

    :type: :obj:`dict`
    """


class ConnectionPool(ConnectionInterface):
    """
    Represents the pool of connections to a cluster of Tarantool
    servers.

    To work with :class:`~tarantool.connection_pool.ConnectionPool`,
    `box.info`_ must be callable for the user on each server.

    :class:`~tarantool.ConnectionPool` is best suited to work with
    a single replicaset. Its API is the same as a single server
    :class:`~tarantool.Connection`, but requests support ``mode``
    parameter (a :class:`tarantool.Mode` value) to choose between
    read-write and read-only pool instances:

    .. code-block:: python

        >>> resp = conn.select('demo', 'AAAA', mode=tarantool.Mode.PREFER_RO)
        >>> resp
        - ['AAAA', 'Alpha']
    """

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
        """
        :param addrs: List of dictionaries describing server addresses:

            .. code-block:: python

                {
                    "host': "str" or None,     # mandatory
                    "port": int or "str",      # mandatory
                    "transport": "str",        # optional
                    "ssl_key_file": "str",     # optional
                    "ssl_cert_file": "str",    # optional
                    "ssl_ca_file": "str",      # optional
                    "ssl_ciphers": "str"       # optional
                    "ssl_password": "str",     # optional
                    "ssl_password_file": "str" # optional
                    "auth_type": "str"         # optional
                }

            Refer to corresponding :class:`~tarantool.Connection`
            parameters.
        :type addrs: :obj:`list`

        :param user: Refer to
            :paramref:`~tarantool.Connection.params.user`.
            The value is used for each connection in the pool.

        :param password: Refer to
            :paramref:`~tarantool.Connection.params.password`.
            The value is used for each connection in the pool.

        :param socket_timeout: Refer to
            :paramref:`~tarantool.Connection.params.socket_timeout`.
            The value is used for each connection in the pool.

        :param reconnect_max_attempts: Refer to
            :paramref:`~tarantool.Connection.params.reconnect_max_attempts`.
            The value is used for each connection in the pool.
            Be careful: it is internal :class:`~tarantool.Connection`
            reconnect unrelated to pool reconnect mechanisms.

        :param reconnect_delay: Refer to
            :paramref:`~tarantool.Connection.params.reconnect_delay`.
            The value is used for each connection in the pool.
            Be careful: it is internal :class:`~tarantool.Connection`
            reconnect unrelated to pool reconnect mechanisms.

        :param connect_now: If ``True``, connect to all pool servers on
            initialization. Otherwise, you have to call
            :meth:`~tarantool.connection_pool.ConnectionPool.connect`
            manually after initialization.
        :type connect_now: :obj:`bool`, optional

        :param encoding: Refer to
            :paramref:`~tarantool.Connection.params.encoding`.
            The value is used for each connection in the pool.

        :param call_16: Refer to
            :paramref:`~tarantool.Connection.params.call_16`.
            The value is used for each connection in the pool.

        :param connection_timeout: Refer to
            :paramref:`~tarantool.Connection.params.connection_timeout`.
            The value is used for each connection in the pool.

        :param strategy_class: Strategy for choosing a server based on a
            request mode. Defaults to the round-robin strategy.
        :type strategy_class: :class:`~tarantool.connection_pool.StrategyInterface`,
            optional

        :param refresh_delay: Minimal time between pool server
            `box.info.ro`_ status background refreshes, in seconds.
        :type connection_timeout: :obj:`float`, optional

        :raise: :exc:`~tarantool.error.ConfigurationError`,
            :class:`~tarantool.Connection` exceptions

        .. _box.info.ro:
        .. _box.info.status:
        .. _box.info: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_info/
        """

        if not isinstance(addrs, list) or len(addrs) == 0:
            raise ConfigurationError("addrs must be non-empty list")

        # Prepare addresses for usage.
        new_addrs = []
        for addr in addrs:
            new_addr, msg = prepare_address(addr)
            if not new_addr:
                raise ConfigurationError(msg)
            new_addrs.append(new_addr)
        self.addrs = new_addrs

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
                    connection_timeout=connection_timeout,
                    transport=addr['transport'],
                    ssl_key_file=addr['ssl_key_file'],
                    ssl_cert_file=addr['ssl_cert_file'],
                    ssl_ca_file=addr['ssl_ca_file'],
                    ssl_ciphers=addr['ssl_ciphers'],
                    ssl_password=addr['ssl_password'],
                    ssl_password_file=addr['ssl_password_file'],
                    auth_type=addr['auth_type'])
            )

        if connect_now:
            self.connect()

    def __del__(self):
        self.close()

    def _make_key(self, addr):
        """
        Make a unique key for a server based on its address.

        :param addr: `{"host": host, "port": port}` dictionary.
        :type addr: :obj:`dict`

        :rtype: :obj:`str`

        :meta private:
        """

        return '{0}:{1}'.format(addr['host'], addr['port'])

    def _get_new_state(self, unit):
        """
        Get new pool server state.

        :param unit: Server metainfo.
        :type unit: :class:`~tarantool.connection_pool.PoolUnit`

        :rtype: :class:`~tarantool.connection_pool.InstanceState`

        :meta private:
        """

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
        """
        Refresh pool server state.

        :param key: Result of
            :meth:`~tarantool.connection_pool._make_key`.
        :type key: :obj:`str`

        :meta private:
        """

        unit = self.pool[key]

        state = self._get_new_state(unit)
        if state != unit.state:
            unit.state = state
            self.strategy.update()

    def close(self):
        """
        Stop request processing, close each connection in the pool.
        """
        for unit in self.pool.values():
            unit.request_processing_enabled = False
            unit.thread.join()

            if not unit.conn.is_closed():
                unit.conn.close()

    def is_closed(self):
        """
        Returns ``False`` if at least one connection is not closed and
        is ready to process requests. Otherwise, returns ``True``.

        :rtype: :obj:`bool`
        """

        return all(unit.request_processing_enabled == False for unit in self.pool.values())

    def _request_process_loop(self, key, unit, last_refresh):
        """
        Request process background loop for a pool server. Started in
        a separate thread, one thread per server.

        :param key: Result of
            :meth:`~tarantool.connection_pool._make_key`.
        :type key: :obj:`str`

        :param unit: Server metainfo.
        :type unit: :class:`~tarantool.connection_pool.PoolUnit`

        :param last_refresh: Time of last metainfo refresh.
        :type last_refresh: :obj:`float`
        """

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
        """
        Create a connection to each address specified on
        initialization and start background process threads for them.
        There is no need to call this method explicitly until you have
        set ``connect_now=False`` on initialization.

        If some connections have failed to connect successfully or
        provide `box.info`_ status (including the case when all of them
        have failed), no exceptions are raised. Attempts to reconnect
        and refresh the info would be processed in the background.
        """

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
        """
        Request wrapper. Choose a pool server based on mode and send
        a request with arguments.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`

        :param method_name: :class:`~tarantool.Connection`
            method name.
        :type method_name: :obj:`str`

        :param args: Method args.
        :type args: :obj:`tuple`

        :param kwargs: Method kwargs.
        :type kwargs: :obj:`dict`

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        :meta private:
        """

        key = self.strategy.getnext(mode)
        unit = self.pool[key]

        task = PoolTask(method_name=method_name, args=args, kwargs=kwargs)

        unit.input_queue.put(task)
        resp = unit.output_queue.get()

        if isinstance(resp, Exception):
            raise resp

        return resp

    def call(self, func_name, *args, mode=None, on_push=None, on_push_ctx=None):
        """
        Execute a CALL request on the pool server: call a stored Lua
        function. Refer to :meth:`~tarantool.Connection.call`.

        :param func_name: Refer to
            :paramref:`~tarantool.Connection.call.params.func_name`.

        :param args: Refer to
            :paramref:`~tarantool.Connection.call.params.args`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.call.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.call.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~ValueError`,
            :meth:`~tarantool.Connection.call` exceptions
        """

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'call', func_name, *args, on_push=on_push, on_push_ctx=on_push_ctx)

    def eval(self, expr, *args, mode=None, on_push=None, on_push_ctx=None):
        """
        Execute an EVAL request on the pool server: evaluate a Lua
        expression. Refer to :meth:`~tarantool.Connection.eval`.

        :param expr: Refer to
            :paramref:`~tarantool.Connection.eval.params.expr`.

        :param args: Refer to
            :paramref:`~tarantool.Connection.eval.params.args`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.eval.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.eval.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~ValueError`,
            :meth:`~tarantool.Connection.eval` exceptions
        """

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'eval', expr, *args, on_push=on_push, on_push_ctx=on_push_ctx)

    def replace(self, space_name, values, *, mode=Mode.RW, on_push=None, on_push_ctx=None):
        """
        Execute a REPLACE request on the pool server: `replace`_ a tuple
        in the space. Refer to :meth:`~tarantool.Connection.replace`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.replace.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.replace.params.values`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.replace.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.replace.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :meth:`~tarantool.Connection.replace` exceptions

        .. _replace: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/replace/
        """

        return self._send(mode, 'replace', space_name, values, on_push=on_push, on_push_ctx=on_push_ctx)

    def insert(self, space_name, values, *, mode=Mode.RW, on_push=None, on_push_ctx=None):
        """
        Execute an INSERT request on the pool server: `insert`_ a tuple
        to the space. Refer to :meth:`~tarantool.Connection.insert`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.insert.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.insert.params.values`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.insert.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.insert.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :meth:`~tarantool.Connection.insert` exceptions

        .. _insert: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/insert/
        """

        return self._send(mode, 'insert', space_name, values, on_push=on_push, on_push_ctx=on_push_ctx)

    def delete(self, space_name, key, *, index=0, mode=Mode.RW, on_push=None, on_push_ctx=None):
        """
        Execute an DELETE request on the pool server: `delete`_ a tuple
        in the space. Refer to :meth:`~tarantool.Connection.delete`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.delete.params.space_name`.

        :param key: Refer to
            :paramref:`~tarantool.Connection.delete.params.key`.

        :param index: Refer to
            :paramref:`~tarantool.Connection.delete.params.index`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.delete.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.delete.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :meth:`~tarantool.Connection.delete` exceptions

        .. _delete: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/delete/
        """

        return self._send(mode, 'delete', space_name, key, index=index, on_push=on_push, on_push_ctx=on_push_ctx)

    def upsert(self, space_name, tuple_value, op_list, *, index=0, mode=Mode.RW, on_push=None, on_push_ctx=None):
        """
        Execute an UPSERT request on the pool server: `upsert`_ a tuple to
        the space. Refer to :meth:`~tarantool.Connection.upsert`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.upsert.params.space_name`.

        :param tuple_value: Refer to
            :paramref:`~tarantool.Connection.upsert.params.tuple_value`.

        :param op_list: Refer to
            :paramref:`~tarantool.Connection.upsert.params.op_list`.

        :param index: Refer to
            :paramref:`~tarantool.Connection.upsert.params.index`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.upsert.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.upsert.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :meth:`~tarantool.Connection.upsert` exceptions

        .. _upsert: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/upsert/
        """

        return self._send(mode, 'upsert', space_name, tuple_value,
            op_list, index=index, on_push=on_push, on_push_ctx=on_push_ctx)

    def update(self, space_name, key, op_list, *, index=0, mode=Mode.RW, on_push=None, on_push_ctx=None):
        """
        Execute an UPDATE request on the pool server: `update`_ a tuple
        in the space. Refer to :meth:`~tarantool.Connection.update`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.update.params.space_name`.

        :param key: Refer to
            :paramref:`~tarantool.Connection.update.params.key`.

        :param op_list: Refer to
            :paramref:`~tarantool.Connection.update.params.op_list`.

        :param index: Refer to
            :paramref:`~tarantool.Connection.update.params.index`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.update.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.update.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :meth:`~tarantool.Connection.upsert` exceptions

        .. _update: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/update/
        """

        return self._send(mode, 'update', space_name, key, 
            op_list, index=index, on_push=on_push, on_push_ctx=on_push_ctx)

    def ping(self, notime=False, *, mode=None):
        """
        Execute a PING request on the pool server: send an empty request
        and receive an empty response from the server. Refer to
        :meth:`~tarantool.Connection.ping`.

        :param notime: Refer to
            :paramref:`~tarantool.Connection.ping.params.notime`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`

        :return: Refer to :meth:`~tarantool.Connection.ping`.

        :raise: :exc:`~ValueError`,
            :meth:`~tarantool.Connection.ping` exceptions
        """

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'ping', notime)

    def select(self, space_name, key, *, offset=0, limit=0xffffffff,
               index=0, iterator=None, mode=Mode.ANY, on_push=None, on_push_ctx=None):
        """
        Execute a SELECT request on the pool server: `update`_ a tuple
        from the space. Refer to :meth:`~tarantool.Connection.select`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.select.params.space_name`.

        :param key: Refer to
            :paramref:`~tarantool.Connection.select.params.key`.

        :param offset: Refer to
            :paramref:`~tarantool.Connection.select.params.offset`.

        :param limit: Refer to
            :paramref:`~tarantool.Connection.select.params.limit`.

        :param index: Refer to
            :paramref:`~tarantool.Connection.select.params.index`.

        :param iterator: Refer to
            :paramref:`~tarantool.Connection.select.params.iterator`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :param on_push: Refer to
            :paramref:`~tarantool.Connection.select.params.on_push`.

        :param on_push_ctx: Refer to
            :paramref:`~tarantool.Connection.select.params.on_push_ctx`.

        :rtype: :class:`~tarantool.response.Response`

        :raise: :meth:`~tarantool.Connection.select` exceptions

        .. _select: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/select/
        """

        return self._send(mode, 'select', space_name, key, offset=offset, limit=limit,
                          index=index, iterator=iterator, on_push=on_push, on_push_ctx=on_push_ctx)

    def execute(self, query, params=None, *, mode=None):
        """
        Execute an SQL request on the pool server. Refer to
        :meth:`~tarantool.Connection.execute`.

        :param query: Refer to
            :paramref:`~tarantool.Connection.execute.params.query`.

        :param params: Refer to
            :paramref:`~tarantool.Connection.execute.params.params`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~ValueError`,
            :meth:`~tarantool.Connection.execute` exceptions
        """

        if mode is None:
            raise ValueError("Please, specify 'mode' keyword argument")

        return self._send(mode, 'execute', query, params)

    def crud_insert(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_insert request on the pool server: 
        inserts row through the 
        `crud <https://github.com/tarantool/crud#insert>`__.
        Refer to :meth:`~tarantool.Connection.crud_insert`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_insert.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_insert.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_insert.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_insert', space_name, values, opts)

    def crud_insert_object(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_insert_object request on the pool server: 
        inserts object row through the 
        `crud <https://github.com/tarantool/crud#insert>`__.
        Refer to :meth:`~tarantool.Connection.crud_insert_object`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_insert_object.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_insert_object.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_insert_object.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_insert_object', space_name, values, opts)

    def crud_insert_many(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_insert_many request on the pool server: 
        inserts batch rows through the 
        `crud <https://github.com/tarantool/crud#insert-many>`__.
        Refer to :meth:`~tarantool.Connection.crud_insert_many`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_insert_many.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_insert_many.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_insert_many.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_insert_many', space_name, values, opts)

    def crud_insert_object_many(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_insert_object_many request on the pool server: 
        inserts batch object rows through the
        `crud <https://github.com/tarantool/crud#insert-many>`__.
        Refer to :meth:`~tarantool.Connection.crud_insert_object_many`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_insert_object_many.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_insert_object_many.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_insert_object_many.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_insert_object_many', space_name, values, opts)

    def crud_get(self, space_name, key, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_get request on the pool server: 
        gets row through the 
        `crud <https://github.com/tarantool/crud#get>`__.
        Refer to :meth:`~tarantool.Connection.crud_get`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_get.params.space_name`.

        :param key: Refer to
            :paramref:`~tarantool.Connection.crud_get.params.key`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_get.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_get', space_name, key, opts)

    def crud_update(self, space_name, key, operations=[], opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_update request on the pool server: 
        updates row through the 
        `crud <https://github.com/tarantool/crud#update>`__.
        Refer to :meth:`~tarantool.Connection.crud_update`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_update.params.space_name`.

        :param key: Refer to
            :paramref:`~tarantool.Connection.crud_update.params.key`.

        :param operations: Refer to
            :paramref:`~tarantool.Connection.crud_update.params.operations`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_update.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_update', space_name, key, operations, opts)

    def crud_delete(self, space_name, key, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_delete request on the pool server: 
        deletes row through the 
        `crud <https://github.com/tarantool/crud#delete>`__.
        Refer to :meth:`~tarantool.Connection.crud_delete`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_delete.params.space_name`.

        :param key: Refer to
            :paramref:`~tarantool.Connection.crud_delete.params.key`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_delete.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_delete', space_name, key, opts)

    def crud_replace(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_replace request on the pool server: 
        replaces row through the 
        `crud <https://github.com/tarantool/crud#replace>`__.
        Refer to :meth:`~tarantool.Connection.crud_replace`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_replace.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_replace.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_replace.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_replace', space_name, values, opts)

    def crud_replace_object(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_replace_object request on the pool server: 
        replaces object row through the 
        `crud <https://github.com/tarantool/crud#replace>`__.
        Refer to :meth:`~tarantool.Connection.crud_replace_object`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_replace_object.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_replace_object.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_replace_object.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_replace_object', space_name, values, opts)

    def crud_replace_many(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_replace_many request on the pool server: 
        replaces batch rows through the 
        `crud <https://github.com/tarantool/crud#replace-many>`__.
        Refer to :meth:`~tarantool.Connection.crud_replace_many`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_replace_many.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_replace_many.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_replace_many.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_replace_many', space_name, values, opts)

    def crud_replace_object_many(self, space_name, values, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_replace_object_many request on the pool server: 
        replaces batch object rows through the 
        `crud <https://github.com/tarantool/crud#replace-many>`__.
        Refer to :meth:`~tarantool.Connection.crud_replace_object_many`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_replace_object_many.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_replace_object_many.params.values`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_replace_object_many.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_replace_object_many', space_name, values, opts)

    def crud_upsert(self, space_name, values, operations=[], opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_upsert request on the pool server: 
        upserts row through the 
        `crud <https://github.com/tarantool/crud#upsert>`__.
        Refer to :meth:`~tarantool.Connection.crud_upsert`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_upsert.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_upsert.params.values`.

        :param operations: Refer to
            :paramref:`~tarantool.Connection.crud_upsert.params.operations`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_upsert.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_upsert', space_name, values, operations, opts)

    def crud_upsert_object(self, space_name, values, operations=[], opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_upsert_object request on the pool server: 
        upserts object row through the 
        `crud <https://github.com/tarantool/crud#upsert>`__.
        Refer to :meth:`~tarantool.Connection.crud_upsert_object`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_object.params.space_name`.

        :param values: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_object.params.values`.

        :param operations: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_object.params.operations`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_object.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_upsert_object', space_name, values, operations, opts)

    def crud_upsert_many(self, space_name, values_operation, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_upsert_many request on the pool server: 
        upserts batch rows through the 
        `crud <https://github.com/tarantool/crud#upsert-many>`__.
        Refer to :meth:`~tarantool.Connection.crud_upsert_many`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_many.params.space_name`.

        :param values_operation: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_many.params.values_operation`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_many.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_upsert_many', space_name, values_operation, opts)

    def crud_upsert_object_many(self, space_name, values_operation, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_upsert_object_many request on the pool server: 
        upserts batch object rows through the 
        `crud <https://github.com/tarantool/crud#upsert-many>`__.
        Refer to :meth:`~tarantool.Connection.crud_upsert_object_many`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_object_many.params.space_name`.

        :param values_operation: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_object_many.params.values_operation`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_upsert_object_many.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_upsert_object_many', space_name, values_operation, opts)

    def crud_select(self, space_name, conditions=[], opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_select request on the pool server: 
        selects rows through the 
        `crud <https://github.com/tarantool/crud#select>`__.
        Refer to :meth:`~tarantool.Connection.crud_select`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_select.params.space_name`.

        :param conditions: Refer to
            :paramref:`~tarantool.Connection.crud_select.params.conditions`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_select.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_select', space_name, conditions, opts)

    def crud_min(self, space_name, index_name, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_min request on the pool server: 
        gets rows with minimum value in the specified index through 
        `crud <https://github.com/tarantool/crud#min-and-max>`__.
        Refer to :meth:`~tarantool.Connection.crud_min`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_min.params.space_name`.

        :param index_name: Refer to
            :paramref:`~tarantool.Connection.crud_min.params.index_name`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_min.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_min', space_name, index_name, opts)

    def crud_max(self, space_name, index_name, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_max request on the pool server: 
        gets rows with maximum value in the specified index through 
        `crud <https://github.com/tarantool/crud#min-and-max>`__.
        Refer to :meth:`~tarantool.Connection.crud_max`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_max.params.space_name`.

        :param index_name: Refer to
            :paramref:`~tarantool.Connection.crud_max.params.index_name`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_max.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_max', space_name, index_name, opts)

    def crud_len(self, space_name, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_len request on the pool server: 
        gets the number of tuples in the space through 
        `crud <https://github.com/tarantool/crud#len>`__.
        Refer to :meth:`~tarantool.Connection.crud_len`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_len.params.space_name`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_len.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_len', space_name, opts)

    def crud_storage_info(self, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_storage_info request on the pool server: 
        gets storages status through the 
        `crud <https://github.com/tarantool/crud#storage-info>`__.
        Refer to :meth:`~tarantool.Connection.crud_storage_info`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_storage_info.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_storage_info', opts)

    def crud_count(self, space_name, conditions=[], opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_count request on the pool server: 
        gets rows count through the 
        `crud <https://github.com/tarantool/crud#count>`__.
        Refer to :meth:`~tarantool.Connection.crud_count`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_count.params.space_name`.

        :param conditions: Refer to
            :paramref:`~tarantool.Connection.crud_count.params.conditions`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_count.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_count', space_name, conditions, opts)

    def crud_stats(self, space_name=None, *, mode=Mode.ANY):
        """
        Execute an crud_stats request on the pool server: 
        gets statistics through the 
        `crud <https://github.com/tarantool/crud#statistics>`__.
        Refer to :meth:`~tarantool.Connection.crud_stats`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_stats.params.space_name`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_stats', space_name)

    def crud_unflatten_rows(self, rows, metadata, *, mode=Mode.ANY):
        """
        Makes rows unflatten through the 
        `crud <https://github.com/tarantool/crud#api>`__.
        Refer to :meth:`~tarantool.Connection.crud_unflatten_rows`.

        :param rows: Refer to
            :paramref:`~tarantool.Connection.crud_unflatten_rows.params.rows`.

        :param metadata: Refer to
            :paramref:`~tarantool.Connection.crud_unflatten_rows.params.metadata`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_unflatten_rows', rows, metadata)

    def crud_truncate(self, space_name, opts={}, *, mode=Mode.ANY):
        """
        Execute an crud_truncate request on the pool server: 
        truncates rows through 
        `crud <https://github.com/tarantool/crud#truncate>`__.
        Refer to :meth:`~tarantool.Connection.crud_truncate`.

        :param space_name: Refer to
            :paramref:`~tarantool.Connection.crud_truncate.params.space_name`.

        :param opts: Refer to
            :paramref:`~tarantool.Connection.crud_truncate.params.opts`.

        :param mode: Request mode.
        :type mode: :class:`~tarantool.Mode`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        return self._send(mode, 'crud_truncate', space_name, opts)
