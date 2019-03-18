# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
This module provides low-level API for Tarantool
'''

import os
import time
import errno
import socket

import ctypes
import ctypes.util
try:
    from ctypes import c_ssize_t
except ImportError:
    from ctypes import c_longlong as c_ssize_t

import msgpack

import tarantool.error
from tarantool.response import Response
from tarantool.request import (
    Request,
    # RequestOK,
    RequestCall,
    RequestDelete,
    RequestEval,
    RequestInsert,
    RequestJoin,
    RequestReplace,
    RequestPing,
    RequestSelect,
    RequestSubscribe,
    RequestUpdate,
    RequestUpsert,
    RequestAuthenticate
)
from tarantool.space import Space
from tarantool.const import (
    CONNECTION_TIMEOUT,
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    REQUEST_TYPE_OK,
    REQUEST_TYPE_ERROR,
    IPROTO_GREETING_SIZE,
    ITERATOR_EQ,
    ITERATOR_ALL
)
from tarantool.error import (
    NetworkError,
    DatabaseError,
    InterfaceError,
    SchemaError,
    NetworkWarning,
    SchemaReloadException,
    warn
)
from tarantool.schema import Schema
from tarantool.utils import (
    check_key,
    greeting_decode,
    version_id,
    string_types,
    ENCODING_DEFAULT,
)


class Connection(object):
    '''
    Represents connection to the Tarantool server.

    This class is responsible for connection and network exchange with
    the server.
    Also this class provides low-level interface to data manipulation
    (insert/delete/update/select).
    '''
    Error = tarantool.error
    DatabaseError = DatabaseError
    InterfaceError = InterfaceError
    SchemaError = SchemaError
    NetworkError = NetworkError

    def __init__(self, host, port,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 call_16=False,
                 connection_timeout=CONNECTION_TIMEOUT):
        '''
        Initialize a connection to the server.

        :param str host: Server hostname or IP-address
        :param int port: Server port
        :param bool connect_now: if True (default) than __init__() actually
        creates network connection.
                             if False than you have to call connect() manualy.
        '''
        if os.name == 'nt':
            libc = ctypes.WinDLL(
                ctypes.util.find_library('Ws2_32'), use_last_error=True
            )
        else:
            libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
        recv = self._sys_recv = libc.recv
        recv.argtypes = [
            ctypes.c_int, ctypes.c_void_p, c_ssize_t, ctypes.c_int]
        recv.restype = ctypes.c_int
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.socket_timeout = socket_timeout
        self.reconnect_delay = reconnect_delay
        self.reconnect_max_attempts = reconnect_max_attempts
        self.schema = Schema(self)
        self.schema_version = 1
        self._socket = None
        self.connected = False
        self.error = True
        self.encoding = encoding
        self.call_16 = call_16
        self.connection_timeout = connection_timeout
        if connect_now:
            self.connect()

    def close(self):
        '''
        Close connection to the server
        '''
        self._socket.close()
        self._socket = None

    def connect_basic(self):
        if self.host == None:
            self.connect_unix()
        else:
            self.connect_tcp()

    def connect_tcp(self):
        '''
        Create connection to the host and port specified in __init__().
        :raise: `NetworkError`
        '''

        try:
            # If old socket already exists - close it and re-create
            self.connected = True
            if self._socket:
                self._socket.close()
            self._socket = socket.create_connection(
                (self.host, self.port), timeout=self.connection_timeout)
            self._socket.settimeout(self.socket_timeout)
            self._socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        except socket.error as e:
            self.connected = False
            raise NetworkError(e)

    def connect_unix(self):
        '''
        Create connection to the host and port specified in __init__().
        :raise: `NetworkError`
        '''

        try:
            # If old socket already exists - close it and re-create
            self.connected = True
            if self._socket:
                self._socket.close()
            self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._socket.settimeout(self.connection_timeout)
            self._socket.connect(self.port)
            self._socket.settimeout(self.socket_timeout)
        except socket.error as e:
            self.connected = False
            raise NetworkError(e)

    def handshake(self):
        greeting_buf = self._recv(IPROTO_GREETING_SIZE)
        greeting = greeting_decode(greeting_buf)
        if greeting.protocol != "Binary":
            raise NetworkError("Unsupported protocol: " + greeting.protocol)
        self.version_id = greeting.version_id
        self.uuid = greeting.uuid
        self._salt = greeting.salt
        if self.user:
            self.authenticate(self.user, self.password)

    def connect(self):
        '''
        Create connection to the host and port specified in __init__().
        Usually there is no need to call this method directly,
        since it is called when you create an `Connection` instance.

        :raise: `NetworkError`
        '''
        try:
            self.connect_basic()
            self.handshake()
            self.load_schema()
        except Exception as e:
            self.connected = False
            raise NetworkError(e)

    def _recv(self, to_read):
        buf = b""
        while to_read > 0:
            try:
                tmp = self._socket.recv(to_read)
            except OverflowError:
                self._socket.close()
                err = socket.error(
                    errno.ECONNRESET,
                    "Too big packet. Closing connection to server"
                )
                raise NetworkError(err)
            except socket.error:
                err = socket.error(
                    errno.ECONNRESET,
                    "Lost connection to server during query"
                )
                raise NetworkError(err)
            else:
                if len(tmp) == 0:
                    err = socket.error(
                        errno.ECONNRESET,
                        "Lost connection to server during query"
                    )
                    raise NetworkError(err)
                to_read -= len(tmp)
                buf += tmp
        return buf

    def _read_response(self):
        '''
        Read response from the transport (socket)

        :return: tuple of the form (header, body)
        :rtype: tuple of two byte arrays
        '''
        # Read packet length
        length = msgpack.unpackb(self._recv(5))
        # Read the packet
        return self._recv(length)

    def _send_request_wo_reconnect(self, request):
        '''
        :rtype: `Response` instance

        :raise: NetworkError
        '''
        assert isinstance(request, Request)

        response = None
        while True:
            try:
                self._socket.sendall(bytes(request))
                response = Response(self, self._read_response())
                break
            except SchemaReloadException as e:
                self.update_schema(e.schema_version)
                continue

        return response

    def _opt_reconnect(self):
        '''
        Check that connection is alive using low-level recv from libc(ctypes)
        **Due to bug in python - timeout is internal python construction.
        '''
        if not self._socket:
            return self.connect()

        def check():  # Check that connection is alive
            buf = ctypes.create_string_buffer(2)
            try:
                sock_fd = self._socket.fileno()
            except socket.error as e:
                if e.errno == errno.EBADF:
                    return errno.ECONNRESET
            else:
                if os.name == 'nt':
                    flag = socket.MSG_PEEK
                    self._socket.setblocking(False)
                else:
                    flag = socket.MSG_DONTWAIT | socket.MSG_PEEK
                retbytes = self._sys_recv(sock_fd, buf, 1, flag)

                err = 0
                if os.name!= 'nt':
                    err = ctypes.get_errno()
                else:
                    err = ctypes.get_last_error()
                    self._socket.setblocking(True)


                WWSAEWOULDBLOCK = 10035
                if (retbytes < 0) and (err == errno.EAGAIN or
                                       err == errno.EWOULDBLOCK or
                                       err == WWSAEWOULDBLOCK):
                    ctypes.set_errno(0)
                    return errno.EAGAIN
                else:
                    return errno.ECONNRESET

        last_errno = check()
        if self.connected and last_errno == errno.EAGAIN:
            return

        attempt = 0
        last_errno = errno.ECONNRESET
        while True:
            time.sleep(self.reconnect_delay)
            try:
                self.connect_basic()
            except NetworkError:
                pass
            else:
                if self.connected:
                    break
            warn("Reconnect attempt %d of %d" %
                 (attempt, self.reconnect_max_attempts), NetworkWarning)
            if attempt == self.reconnect_max_attempts:
                raise NetworkError(
                    socket.error(last_errno, errno.errorcode[last_errno]))
            attempt += 1
        self.handshake()

    def _send_request(self, request):
        '''
        Send the request to the server through the socket.
        Return an instance of `Response` class.

        :param request: object representing a request
        :type request: `Request` instance

        :rtype: `Response` instance
        '''
        assert isinstance(request, Request)

        self._opt_reconnect()

        return self._send_request_wo_reconnect(request)

    def load_schema(self):
        self.schema.fetch_space_all()
        self.schema.fetch_index_all()

    def update_schema(self, schema_version):
        self.schema_version = schema_version
        self.flush_schema()

    def flush_schema(self):
        self.schema.flush()
        self.load_schema()

    def call(self, func_name, *args):
        '''
        Execute CALL request. Call stored Lua function.

        :param func_name: stored Lua function name
        :type func_name: str
        :param args: list of function arguments
        :type args: list or tuple

        :rtype: `Response` instance
        '''
        assert isinstance(func_name, str)

        # This allows to use a tuple or list as an argument
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        request = RequestCall(self, func_name, args, self.call_16)
        response = self._send_request(request)
        return response

    def eval(self, expr, *args):
        '''
        Execute EVAL request. Eval Lua expression.

        :param expr: Lua expression
        :type expr: str
        :param args: list of function arguments
        :type args: list or tuple

        :rtype: `Response` instance
        '''
        assert isinstance(expr, str)

        # This allows to use a tuple or list as an argument
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        request = RequestEval(self, expr, args)
        response = self._send_request(request)
        return response

    def replace(self, space_name, values):
        '''
        Execute REPLACE request.
        It won't throw error if there's no tuple with this PK exists

        :param int space_name: space id to insert a record
        :type space_name: int or str
        :param values: record to be inserted. The tuple must contain
            only scalar (integer or strings) values
        :type values: tuple

        :rtype: `Response` instance
        '''
        if isinstance(space_name, string_types):
            space_name = self.schema.get_space(space_name).sid
        request = RequestReplace(self, space_name, values)
        return self._send_request(request)

    def authenticate(self, user, password):
        '''
        Execute AUTHENTICATE request.

        :param string user: user to authenticate with
        :param string password: password for the user

        :rtype: `Response` instance
        '''
        self.user = user
        self.password = password
        if not self._socket:
            return self._opt_reconnect()

        request = RequestAuthenticate(self, self._salt, self.user,
                                      self.password)
        auth_response = self._send_request_wo_reconnect(request)
        if auth_response.return_code == 0:
            self.flush_schema()
        return auth_response

    def _join_v16(self, server_uuid):
        request = RequestJoin(self, server_uuid)
        self._socket.sendall(bytes(request))

        while True:
            resp = Response(self, self._read_response())
            yield resp
            if resp.code == REQUEST_TYPE_OK or resp.code >= REQUEST_TYPE_ERROR:
                return
        self.close()  # close connection after JOIN

    def _join_v17(self, server_uuid):
        class JoinState:
            Handshake, Initial, Final, Done = range(4)

        request = RequestJoin(self, server_uuid)
        self._socket.sendall(bytes(request))
        state = JoinState.Handshake
        while True:
            resp = Response(self, self._read_response())
            yield resp
            if resp.code >= REQUEST_TYPE_ERROR:
                return
            elif resp.code == REQUEST_TYPE_OK:
                state = state + 1
                if state == JoinState.Done:
                    return

    def _ops_process(self, space, update_ops):
        new_ops = []
        for op in update_ops:
            if isinstance(op[1], string_types):
                op = list(op)
                op[1] = self.schema.get_field(space, op[1])['id']
            new_ops.append(op)
        return new_ops

    def join(self, server_uuid):
        self._opt_reconnect()
        if self.version_id < version_id(1, 7, 0):
            return self._join_v16(server_uuid)
        return self._join_v17(server_uuid)

    def subscribe(self, cluster_uuid, server_uuid, vclock=None):
        vclock = vclock or {}
        request = RequestSubscribe(self, cluster_uuid, server_uuid, vclock)
        self._socket.sendall(bytes(request))
        while True:
            resp = Response(self, self._read_response())
            yield resp
            if resp.code >= REQUEST_TYPE_ERROR:
                return
        self.close()  # close connection after SUBSCRIBE

    def insert(self, space_name, values):
        '''
        Execute INSERT request.
        It will throw error if there's tuple with same PK exists.

        :param int space_name: space id to insert a record
        :type space_name: int or str
        :param values: record to be inserted. The tuple must contain
            only scalar (integer or strings) values
        :type values: tuple

        :rtype: `Response` instance
        '''
        if isinstance(space_name, string_types):
            space_name = self.schema.get_space(space_name).sid
        request = RequestInsert(self, space_name, values)
        return self._send_request(request)

    def delete(self, space_name, key, **kwargs):
        '''
        Execute DELETE request.
        Delete single record identified by `key`. If you're using secondary
        index, it must be unique.

        :param space_name: space number or name to delete a record
        :type space_name: int or name
        :param key: key that identifies a record
        :type key: int or str

        :rtype: `Response` instance
        '''
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, string_types):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index_name, string_types):
            index_name = self.schema.get_index(space_name, index_name).iid
        request = RequestDelete(self, space_name, index_name, key)
        return self._send_request(request)

    def upsert(self, space_name, tuple_value, op_list, **kwargs):
        '''
        Execute UPSERT request.

        If there is an existing tuple which matches the key fields of
        `tuple_value`, then the request has the same effect as UPDATE
        and the [(field_1, symbol_1, arg_1), ...] parameter is used.

        If there is no existing tuple which matches the key fields of
        `tuple_value`, then the request has the same effect as INSERT
        and the `tuple_value` parameter is used. However, unlike insert
        or update, upsert will not read a tuple and perform error checks
        before returning -- this is a design feature which enhances
        throughput but requires more caution on the part of the user.

        If you're using secondary index, it must be unique.

        List of operations allows to update individual fields.

        *Allowed operations:*

        (For every operation you must provide field number, to apply this
        operation to)

        * `+` for addition (values must be numeric)
        * `-` for subtraction (values must be numeric)
        * `&` for bitwise AND (values must be unsigned numeric)
        * `|` for bitwise OR (values must be unsigned numeric)
        * `^` for bitwise XOR (values must be unsigned numeric)
        * `:` for string splice (you must provide `offset`, `count` and `value`
          for this operation)
        * `!` for insertion (provide any element to insert)
        * `=` for assignment (provide any element to assign)
        * `#` for deletion (provide count of fields to delete)

        :param space_name: space number or name to update a record
        :type space_name: int or str
        :param index: index number or name to update a record
        :type index: int or str
        :param tuple_value: tuple, that
        :type tuple_value:
        :param op_list: list of operations. Each operation
            is tuple of three (or more) values
        :type op_list: a list of the form [(symbol_1, field_1, arg_1),
            (symbol_2, field_2, arg_2_1, arg_2_2, arg_2_3),...]

        :rtype: `Response` instance

        Operation examples:

        .. code-block:: python

            # 'ADD' 55 to second field
            # Assign 'x' to third field
            [('+', 2, 55), ('=', 3, 'x')]
            # 'OR' third field with '1'
            # Cut three symbols starting from second and replace them with '!!'
            # Insert 'hello, world' field before fifth element of tuple
            [('|', 3, 1), (':', 2, 2, 3, '!!'), ('!', 5, 'hello, world')]
            # Delete two fields starting with second field
            [('#', 2, 2)]
        '''
        index_name = kwargs.get("index", 0)

        if isinstance(space_name, string_types):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index_name, string_types):
            index_name = self.schema.get_index(space_name, index_name).iid
        op_list = self._ops_process(space_name, op_list)
        request = RequestUpsert(self, space_name, index_name, tuple_value,
                                op_list)
        return self._send_request(request)

    def update(self, space_name, key, op_list, **kwargs):
        '''
        Execute UPDATE request.

        The `update` function supports operations on fields — assignment,
        arithmetic (if the field is unsigned numeric), cutting and pasting
        fragments of a field, deleting or inserting a field. Multiple
        operations can be combined in a single update request, and in this
        case they are performed atomically and sequentially. Each operation
        requires specification of a field number. When multiple operations are
        present, the field number for each operation is assumed to be relative
        to the most recent state of the tuple, that is, as if all previous
        operations in a multi-operation update have already been applied.
        In other words, it is always safe to merge multiple update invocations
        into a single invocation, with no change in semantics.

        Update single record identified by `key`.

        List of operations allows to update individual fields.

        *Allowed operations:*

        (For every operation you must provide field number, to apply this
        operation to)

        * `+` for addition (values must be numeric)
        * `-` for subtraction (values must be numeric)
        * `&` for bitwise AND (values must be unsigned numeric)
        * `|` for bitwise OR (values must be unsigned numeric)
        * `^` for bitwise XOR (values must be unsigned numeric)
        * `:` for string splice (you must provide `offset`, `count` and `value`
          for this operation)
        * `!` for insertion (before) (provide any element to insert)
        * `=` for assignment (provide any element to assign)
        * `#` for deletion (provide count of fields to delete)

        :param space_name: space number or name to update a record
        :type space_name: int or str
        :param index: index number or name to update a record
        :type index: int or str
        :param key: key that identifies a record
        :type key: int or str
        :param op_list: list of operations. Each operation
            is tuple of three (or more) values
        :type op_list: a list of the form [(symbol_1, field_1, arg_1),
            (symbol_2, field_2, arg_2_1, arg_2_2, arg_2_3), ...]

        :rtype: ``Response`` instance

        Operation examples:

        .. code-block:: python

            # 'ADD' 55 to second field
            # Assign 'x' to third field
            [('+', 2, 55), ('=', 3, 'x')]
            # 'OR' third field with '1'
            # Cut three symbols starting from second and replace them with '!!'
            # Insert 'hello, world' field before fifth element of tuple
            [('|', 3, 1), (':', 2, 2, 3, '!!'), ('!', 5, 'hello, world')]
            # Delete two fields starting with second field
            [('#', 2, 2)]
        '''
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, string_types):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index_name, string_types):
            index_name = self.schema.get_index(space_name, index_name).iid
        op_list = self._ops_process(space_name, op_list)
        request = RequestUpdate(self, space_name, index_name, key, op_list)
        return self._send_request(request)

    def ping(self, notime=False):
        '''
        Execute PING request.
        Send empty request and receive empty response from server.

        :return: response time in seconds
        :rtype: float
        '''

        request = RequestPing(self)
        t0 = time.time()
        self._send_request(request)
        t1 = time.time()

        if notime:
            return "Success"
        return t1 - t0

    def select(self, space_name, key=None, **kwargs):
        '''
        Execute SELECT request.
        Select and retrieve data from the database.

        :param space_name: specifies which space to query
        :type space_name: int or str
        :param values: values to search over the index
        :type values: list, tuple, set, frozenset of tuples
        :param index: specifies which index to use (default is **0** which
            means that the **primary index** will be used)
        :type index: int or str
        :param offset: offset in the resulting tuple set
        :type offset: int
        :param limit: limits the total number of returned tuples
        :type limit: int

        :rtype: `Response` instance

        You may use names for index/space. Matching id's -> names connector
        will get from server.

        Select one single record (from space=0 and using index=0)
        >>> select(0, 1)

        Select single record from space=0 (with name='space') using
        composite index=1 (with name '_name').
        >>> select(0, [1,'2'], index=1)
        # OR
        >>> select(0, [1,'2'], index='_name')
        # OR
        >>> select('space', [1,'2'], index='_name')
        # OR
        >>> select('space', [1,'2'], index=1)

        Select all records
        >>> select(0)
        # OR
        >>> select(0, [])
        '''

        # Initialize arguments and its defaults from **kwargs
        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", 0xffffffff)
        index_name = kwargs.get("index", 0)
        iterator_type = kwargs.get("iterator")

        if iterator_type is None:
            iterator_type = ITERATOR_EQ
            if key is None or (isinstance(key, (list, tuple)) and
                               len(key) == 0):
                iterator_type = ITERATOR_ALL

        # Perform smart type checking (scalar / list of scalars / list of
        # tuples)
        key = check_key(key, select=True)

        if isinstance(space_name, string_types):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index_name, string_types):
            index_name = self.schema.get_index(space_name, index_name).iid
        request = RequestSelect(self, space_name, index_name, key, offset,
                                limit, iterator_type)
        response = self._send_request(request)
        return response

    def space(self, space_name):
        '''
        Create `Space` instance for particular space

        `Space` instance encapsulates the identifier of the space and provides
        more convenient syntax for accessing the database space.

        :param space_name: identifier of the space
        :type space_name: int or str

        :rtype: `Space` instance
        '''
        return Space(self, space_name)

    def generate_sync(self):
        '''
        Need override for async io connection
        '''
        return 0
