# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
This module provides low-level API for Tarantool
'''

import time
import errno
import ctypes
import ctypes.util
import socket
import msgpack

import base64
from const import IPROTO_GREETING_SIZE

try:
    from ctypes import c_ssize_t
except ImportError:
    from ctypes import c_longlong as c_ssize_t

from tarantool.response import Response
from tarantool.request import (
    Request,
    RequestCall,
    RequestDelete,
    RequestInsert,
    RequestReplace,
    RequestPing,
    RequestSelect,
    RequestUpdate,
    RequestAuthenticate)

from tarantool.space import Space
from tarantool.const import (
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    RETRY_MAX_ATTEMPTS,
    IPROTO_GREETING_SIZE)
from tarantool.error import (
    NetworkError,
    DatabaseError,
    warn,
    RetryWarning,
    NetworkWarning)
from tarantool.schema import Schema
from tarantool.utils import check_key

class Connection(object):

    '''\
    Represents connection to the Tarantool server.

    This class is responsible for connection and network exchange with
    the server.
    Also this class provides low-level interface to data manipulation
    (insert/delete/update/select).
    '''
    _libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
    _sys_recv = ctypes.CFUNCTYPE(c_ssize_t, ctypes.c_int, ctypes.c_void_p,
                                 c_ssize_t, ctypes.c_int,
                                 use_errno=True)(_libc.recv)

    def __init__(self, host, port,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True):
        '''\
        Initialize a connection to the server.

        :param str host: Server hostname or IP-address
        :param int port: Server port
        :param bool connect_now: if True (default) than __init__() actually
        creates network connection.
                             if False than you have to call connect() manualy.
        '''
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.socket_timeout = socket_timeout
        self.reconnect_delay = reconnect_delay
        self.reconnect_max_attempts = reconnect_max_attempts
        self.schema = Schema(self)
        self._socket = None
        self.connected = False
        self.error = True
        if connect_now:
            self.connect()


    def close(self):
        '''\
        Close connection to the server
        '''
        self._socket.close()
        self._socket = None

    def connect_basic(self):
        '''\
        Create connection to the host and port specified in __init__().
        :raise: `NetworkError`
        '''

        try:
            # If old socket already exists - close it and re-create
            self.connected = True
            if self._socket:
                self._socket.close()
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self._socket.connect((self.host, self.port))
        except socket.error as e:
            self.connected = False
            raise NetworkError(e)

    def handshake(self):
            greeting = self._recv(IPROTO_GREETING_SIZE)
            self._salt = base64.decodestring(greeting[64:])[:20]
            if self.user:
                self.authenticate(self.user, self.password)

    def connect(self):
        '''\
        Create connection to the host and port specified in __init__().
        Usually there is no need to call this method directly,
        since it is called when you create an `Connection` instance.

        :raise: `NetworkError`
        '''
        try:
            self.connect_basic()
            self.handshake()
            # It is important to set socket timeout *after* connection.
            # Otherwise the timeout exception will be raised, even when
            # the connection fails because the server is simply
            # not bound to port
            self._socket.settimeout(self.socket_timeout)
        except socket.error as e:
            self.connected = False
            raise NetworkError(e)

    def _recv(self, to_read):
        buf = ''
        while to_read > 0:
            tmp = self._socket.recv(to_read)
            if not tmp:
                raise NetworkError(socket.error(errno.ECONNRESET,
                      "Lost connection to server during query"))
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
        '''\
        :rtype: `Response` instance

        :raise: NetworkError
        '''
        assert isinstance(request, Request)

        # Repeat request in a loop if the server returns completion_status == 1
        # (try again)
        for attempt in xrange(RETRY_MAX_ATTEMPTS):    # pylint: disable=W0612
            self._socket.sendall(bytes(request))
            response = Response(self, self._read_response())

            if response.completion_status != 1:
                return response
            warn(response.return_message, RetryWarning)

        # Raise an error if the maximum number of attempts have been made
        raise DatabaseError(response.return_code, response.return_message)

    def _opt_reconnect(self):
        '''\
        Check that connection is alive using low-level recv from libc(ctypes)
        **Due to bug in python - timeout is internal python construction.
        '''
        if not self._socket:
            return self.connect()

        def check():  # Check that connection is alive
            buf = ctypes.create_string_buffer(2)
            self._sys_recv(self._socket.fileno(), buf, 1,
                    socket.MSG_DONTWAIT | socket.MSG_PEEK)
            if ctypes.get_errno() == errno.EAGAIN:
                ctypes.set_errno(0)
                return errno.EAGAIN
            return (ctypes.get_errno() if ctypes.get_errno()
                    else errno.ECONNRESET)

        last_errno = check()
        if self.connected and last_errno == errno.EAGAIN:
            return 

        attempt = 0
        last_errno = 0
        while True:
            time.sleep(self.reconnect_delay)
            try:
                self.connect_basic()
            except NetworkError as e:
                last_errno = e.errno
            if last_errno == 0:
                break
            warn("Reconnect attempt %d of %d" %
                 (attempt, self.reconnect_max_attempts), NetworkWarning)
            if attempt == self.reconnect_max_attempts:
                raise NetworkError(socket.error(last_errno, errno.errorcode[last_errno]))
            attempt += 1

        self.handshake()
        # It is important to set socket timeout *after* connection.
        # Otherwise the timeout exception will be raised, even when
        # the connection fails because the server is simply
        # not bound to port
        self._socket.settimeout(self.socket_timeout)

    def _send_request(self, request):
        '''\
        Send the request to the server through the socket.
        Return an instance of `Response` class.

        :param request: object representing a request
        :type request: `Request` instance

        :rtype: `Response` instance
        '''
        assert isinstance(request, Request)

        self._opt_reconnect()
        response = self._send_request_wo_reconnect(
            request)

        return response

    def flush_schema(self):
        self.schema.flush()

    def call(self, func_name, *args):
        '''\
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

        request = RequestCall(self, func_name, args)
        response = self._send_request(request)
        return response

    def replace(self, space_name, values):
        '''
        Execute REPLACE request.
        It will throw error if there's no tuple with this PK exists

        :param int space_name: space id to insert a record
        :type space_name: int or str
        :param values: record to be inserted. The tuple must contain
            only scalar (integer or strings) values
        :type values: tuple

        :rtype: `Response` instance
        '''
        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid
        request = RequestReplace(self, space_name, values)
        return self._send_request(request)

    def authenticate(self, user, password):
        self.user = user;
        self.password = password
        if not self._socket:
            return self._opt_reconnect()

        request = RequestAuthenticate(self, self._salt, self.user, \
                                      self.password)
        return self._send_request_wo_reconnect(request)

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
        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid
        request = RequestInsert(self, space_name, values)
        return self._send_request(request)

    def delete(self, space_name, key, **kwargs):
        '''\
        Execute DELETE request.
        Delete single record identified by `key` (using primary index).

        :param space_name: space number or name to delete a record
        :type space_name: int or name
        :param key: key that identifies a record
        :type key: int or str

        :rtype: `Response` instance
        '''
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index_name, basestring):
            index_name = self.schema.get_index(space_name, index_name).iid
        request = RequestDelete(self, space_name, index_name, key)
        return self._send_request(request)

    def update(self, space_name, key, op_list, **kwargs):
        '''\
        Execute UPDATE request.
        Update single record identified by `key` (using primary index).

        List of operations allows to update individual fields.

        :param space_name: space number or name to update a record
        :type space_name: int or str
        :param key: key that identifies a record
        :type key: int or str
        :param op_list: list of operations. Each operation
            is tuple of three values
        :type op_list: a list of the form
            [(field_1, symbol_1, arg_1), (field_2, symbol_2, arg_2),...]

        :rtype: `Response` instance
        '''
        index = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index_name, basestring):
            index_name = self.schema.get_index(space_name, index_name).iid
        request = RequestUpdate(self, space_name, index_name, key, op_list)
        return self._send_request(request)

    def ping(self, notime=False):
        '''\
        Execute PING request.
        Send empty request and receive empty response from server.

        :return: response time in seconds
        :rtype: float
        '''

        request = RequestPing(self)
        t0 = time.time()
        response = self._send_request(request)
        t1 = time.time()

        if notime:
            return "Success"
        return t1 - t0

    def select(self, space_name, key=None, **kwargs):
        '''\
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
        iterator_type = kwargs.get("iterator", 0)

        # Perform smart type checking (scalar / list of scalars / list of
        # tuples)
        key = check_key(key, select=True)

        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index_name, basestring):
            index_name = self.schema.get_index(space_name, index_name).iid
        request = RequestSelect(
                self, space_name, index_name, key, offset, limit, iterator_type)
        response = self._send_request(request)
        return response

    def space(self, space_name):
        '''\
        Create `Space` instance for particular space

        `Space` instance encapsulates the identifier of the space and provides
        more convenient syntax for accessing the database space.

        :param space_name: identifier of the space
        :type space_name: int or str

        :rtype: `Space` instance
        '''
        return Space(self, space_name)
