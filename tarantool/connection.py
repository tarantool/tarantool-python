# pylint: disable=C0301,W0105,W0401,W0614
"""
This module provides API for interaction with a Tarantool server
"""

import os
import time
import errno
import socket
try:
    import ssl
    is_ssl_supported = True
except ImportError:
    is_ssl_supported = False
import sys
import abc

import ctypes
import ctypes.util
from ctypes import c_ssize_t

import msgpack

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
    RequestAuthenticate,
    RequestExecute
)
from tarantool.space import Space
from tarantool.const import (
    CONNECTION_TIMEOUT,
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    DEFAULT_TRANSPORT,
    SSL_TRANSPORT,
    DEFAULT_SSL_KEY_FILE,
    DEFAULT_SSL_CERT_FILE,
    DEFAULT_SSL_CA_FILE,
    DEFAULT_SSL_CIPHERS,
    REQUEST_TYPE_OK,
    REQUEST_TYPE_ERROR,
    IPROTO_GREETING_SIZE,
    ITERATOR_EQ,
    ITERATOR_ALL
)
from tarantool.error import (
    Error,
    NetworkError,
    SslError,
    DatabaseError,
    InterfaceError,
    ConfigurationError,
    SchemaError,
    NetworkWarning,
    OperationalError,
    DataError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
    SchemaReloadException,
    Warning,
    warn
)
from tarantool.schema import Schema
from tarantool.utils import (
    check_key,
    greeting_decode,
    version_id,
    ENCODING_DEFAULT,
)

# Based on https://realpython.com/python-interface/
class ConnectionInterface(metaclass=abc.ABCMeta):
    """
    Represents a connection to single or multiple Tarantool servers.

    Interface requires that connection objects has methods to open
    and close a connection, check its status, call procedures and
    evaluate Lua code on server, make simple data manipulations and
    execute SQL queries.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'connect') and
                callable(subclass.connect) and
                hasattr(subclass, 'close') and
                callable(subclass.close) and
                hasattr(subclass, 'is_closed') and
                callable(subclass.is_closed) and
                hasattr(subclass, 'ping') and
                callable(subclass.ping) and
                hasattr(subclass, 'call') and
                callable(subclass.call) and
                hasattr(subclass, 'eval') and
                callable(subclass.eval) and
                hasattr(subclass, 'replace') and
                callable(subclass.replace) and
                hasattr(subclass, 'insert') and
                callable(subclass.insert) and
                hasattr(subclass, 'delete') and
                callable(subclass.delete) and
                hasattr(subclass, 'upsert') and
                callable(subclass.upsert) and
                hasattr(subclass, 'update') and
                callable(subclass.update) and
                hasattr(subclass, 'select') and
                callable(subclass.select) and
                hasattr(subclass, 'execute') and
                callable(subclass.execute) or
                NotImplemented)

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError

    @abc.abstractmethod
    def is_closed(self):
        raise NotImplementedError

    @abc.abstractmethod
    def ping(self, notime):
        raise NotImplementedError

    @abc.abstractmethod
    def call(self, func_name, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def eval(self, expr, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def replace(self, space_name, values):
        raise NotImplementedError

    @abc.abstractmethod
    def insert(self, space_name, values):
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, space_name, key, *, index):
        raise NotImplementedError

    @abc.abstractmethod
    def upsert(self, space_name, tuple_value, op_list, *, index=None):
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, space_name, key, op_list, *, index=None):
        raise NotImplementedError

    @abc.abstractmethod
    def select(self, space_name, key, *, offset=None, limit=None, index=None, iterator=None):
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, query, params):
        raise NotImplementedError


class Connection(ConnectionInterface):
    """
    Represents a connection to a Tarantool server.

    Сonnection objects has methods to open and close a connection,
    check its status, call procedures and evaluate Lua code on server,
    make simple data manipulations and execute SQL queries.
    """
    # DBAPI Extension: supply exceptions as attributes on the connection
    Error = Error
    DatabaseError = DatabaseError
    InterfaceError = InterfaceError
    ConfigurationError = ConfigurationError
    SchemaError = SchemaError
    NetworkError = NetworkError
    Warning = Warning
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
    SslError = SslError

    def __init__(self, host, port,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 use_list=True,
                 call_16=False,
                 connection_timeout=CONNECTION_TIMEOUT,
                 transport=DEFAULT_TRANSPORT,
                 ssl_key_file=DEFAULT_SSL_KEY_FILE,
                 ssl_cert_file=DEFAULT_SSL_CERT_FILE,
                 ssl_ca_file=DEFAULT_SSL_CA_FILE,
                 ssl_ciphers=DEFAULT_SSL_CIPHERS):
        """
        Initialize a connection to the Tarantool server.

        :param host: server hostname or IP address. Use ``None`` for UNIX sockets
        :type host: :obj:`str` or :obj:`None`

        :param port: server port or UNIX socket path
        :type port: :obj:`int` or :obj:`str`

        :param user: user name for authentication on the Tarantool server.
            Defaults to ``None``
        :type user: :obj:`str` or :obj:`None`, optional

        :param password: user password for authentication on the Tarantool server.
            Defaults to ``None``
        :type password: :obj:`str` or :obj:`None`, optional

        :param socket_timeout: timeout on blocking socket operations, in seconds
            (see `socket.settimeout() <https://docs.python.org/3/library/socket.html#socket.socket.settimeout>`_).
            Defaults to ``None``
        :type socket_timeout: :obj:`float` or :obj:`None`, optional

        :param reconnect_max_attempts: count of maximum attempts to reconnect
            on API call if connection is lost.
            Defaults to ``10``
        :type reconnect_max_attempts: :obj:`int`, optional

        :param reconnect_delay: delay between attempts to reconnect
            on API call if connection is lost, in seconds.
            Defaults to ``0.1``
        :type reconnect_delay: :obj:`float`, optional

        :param bool connect_now: If ``True``, connect to server on initialization.
            Otherwise, you have to call
            :py:meth:`~tarantool.Connection.connect`
            manually after initialization.
            Defaults to ``True``
        :type connect_now: :obj:`bool`, optional

        :param encoding: ``'utf-8'`` or ``None``. Use ``None`` to work with non-UTF8 strings.

            If ``'utf-8'``, pack Unicode string (:obj:`str`) to
            msgpack string (`mp_str`_) and unpack msgpack string (`mp_str`_)
            Unicode string (:obj:`str`),
            pack :obj:`bytes` to msgpack binary (`mp_bin`_)
            and unpack msgpack binary (`mp_bin`_) to :obj:`bytes`.

                +--------------+----+-----------+----+--------------+
                | Python       | -> | Tarantool | -> | Python       |
                +--------------+----+-----------+----+--------------+
                | :obj:`str`   | -> | `mp_str`_ | -> | :obj:`str`   |
                +--------------+----+-----------+----+--------------+
                | :obj:`bytes` | -> | `mp_bin`_ | -> | :obj:`bytes` |
                +--------------+----+-----------+----+--------------+

            If ``None``, pack Unicode string (:obj:`str`) and :obj:`bytes`
            to msgpack string (`mp_str`_), unpacked msgpack string (`mp_str`_)
            and msgpack binary (`mp_bin`_) to :obj:`bytes`.

                +--------------+----+-----------+----+--------------+
                | Python       | -> | Tarantool | -> | Python       |
                +--------------+----+-----------+----+--------------+
                | :obj:`bytes` | -> | `mp_str`_ | -> | :obj:`bytes` |
                +--------------+----+-----------+----+--------------+
                | :obj:`str`   | -> | `mp_str`_ | -> | :obj:`bytes` |
                +--------------+----+-----------+----+--------------+
                |              |    | `mp_bin`_ | -> | :obj:`bytes` |
                +--------------+----+-----------+----+--------------+

            Defaults to ``'utf-8'``
        :type encoding: :obj:`str` or :obj:`None`, optional

        :param use_list:
            If ``True``, unpack msgpack array (`mp_array`_) to :obj:`list`.
            Otherwise, unpack to :obj:`tuple`.
            Defaults to ``True``
        :type use_list: :obj:`bool`, optional

        :param call_16:
            If ``True``, enables compatibility mode with Tarantool 1.6 and
            older for `call` operations.
            Defaults to ``False``
        :type call_16: :obj:`bool`, optional

        :param connection_timeout: time to establish initial socket connection,
            in seconds.
            Defaults to ``None``
        :type connection_timeout: :obj:`float` or :obj:`None`, optional

        :param transport: ``''`` or ``'ssl'``. Set to ``'ssl'`` to enable
            SSL encryption for a connection (requires Python >= 3.5).
            Defaults to ``''``
        :type transport: :obj:`str`, optional

        :param ssl_key_file: path to a private SSL key file. Mandatory,
            if server uses a trusted certificate authorities (CA) file.
            Defaults to ``None``
        :type ssl_key_file: :obj:`str` or :obj:`None`, optional

        :param str ssl_cert_file: path to a SSL certificate file. Mandatory,
            if server uses a trusted certificate authorities (CA) file.
            Defaults to ``None``
        :type ssl_cert_file: :obj:`str` or :obj:`None`, optional

        :param ssl_ca_file: path to a trusted certificate authority
            (CA) file.
            Defaults to ``None``
        :type ssl_ca_file: :obj:`str` or :obj:`None`, optional

        :param ssl_ciphers: colon-separated (:) list of SSL cipher
            suites the connection can use.
            Defaults to ``None``
        :type ssl_ciphers: :obj:`str` or :obj:`None`, optional

        :raise: :py:exc:`~tarantool.error.ConfigurationError`
        :raise: :py:exc:`~tarantool.error.NetworkError`
        :raise: :py:exc:`~tarantool.error.SslError`
        :raise: :py:exc:`~tarantool.error.SchemaError`
        :raise: :py:exc:`~tarantool.error.DatabaseError`
        :raise: :py:exc:`~ValueError`

        .. _mp_str: https://github.com/msgpack/msgpack/blob/master/spec.md#str-format-family
        .. _mp_bin: https://github.com/msgpack/msgpack/blob/master/spec.md#bin-format-family
        .. _mp_array: https://github.com/msgpack/msgpack/blob/master/spec.md#array-format-family
        """

        if msgpack.version >= (1, 0, 0) and encoding not in (None, 'utf-8'):
            raise ConfigurationError("msgpack>=1.0.0 only supports None and " +
                                     "'utf-8' encoding option values")

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
        self.use_list = use_list
        self.call_16 = call_16
        self.connection_timeout = connection_timeout
        self.transport = transport
        self.ssl_key_file = ssl_key_file
        self.ssl_cert_file = ssl_cert_file
        self.ssl_ca_file = ssl_ca_file
        self.ssl_ciphers = ssl_ciphers
        if connect_now:
            self.connect()

    def close(self):
        """
        Close a connection to the server.
        """
        self._socket.close()
        self._socket = None

    def is_closed(self):
        """
        Returns ``True`` if connection is closed. ``False`` otherwise.

        :rtype: :obj:`bool`
        """
        return self._socket is None

    def connect_basic(self):
        if self.host is None:
            self.connect_unix()
        else:
            self.connect_tcp()

    def connect_tcp(self):
        """
        Create a connection to the host and port specified in __init__().

        :raise: `NetworkError`
        """

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
        """
        Create a connection to the host and port specified in __init__().

        :raise: `NetworkError`
        """

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

    def wrap_socket_ssl(self):
        """
        Wrap an existing socket with an SSL socket.

        :raise: SslError
        """
        if not is_ssl_supported:
            raise SslError("Your version of Python doesn't support SSL")

        ver = sys.version_info
        if ver[0] < 3 or (ver[0] == 3 and ver[1] < 5):
            raise SslError("SSL transport is supported only since " +
                           "python 3.5")

        if ((self.ssl_cert_file is None and self.ssl_key_file is not None)
           or (self.ssl_cert_file is not None and self.ssl_key_file is None)):
            raise SslError("Both ssl_cert_file and ssl_key_file should be " +
                           "configured or unconfigured")

        try:
            if hasattr(ssl, 'TLSVersion'):
                # Since python 3.7
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                # Reset to default OpenSSL values.
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                # Require TLSv1.2, because other protocol versions don't seem
                # to support the GOST cipher.
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                context.maximum_version = ssl.TLSVersion.TLSv1_2
            else:
                # Deprecated, but it works for python < 3.7
                context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)

            if self.ssl_cert_file:
                # If the password argument is not specified and a password is
                # required, OpenSSL’s built-in password prompting mechanism
                # will be used to interactively prompt the user for a password.
                #
                # We should disable this behaviour, because a python
                # application that uses the connector unlikely assumes
                # interaction with a human + a Tarantool implementation does
                # not support this at least for now.
                def password_raise_error():
                    raise SslError("Password for decrypting the private " +
                                   "key is unsupported")
                context.load_cert_chain(certfile=self.ssl_cert_file,
                                        keyfile=self.ssl_key_file,
                                        password=password_raise_error)

            if self.ssl_ca_file:
                context.load_verify_locations(cafile=self.ssl_ca_file)
                context.verify_mode = ssl.CERT_REQUIRED
                # A Tarantool implementation does not check hostname. We don't
                # do that too. As a result we don't set here:
                # context.check_hostname = True

            if self.ssl_ciphers:
                context.set_ciphers(self.ssl_ciphers)

            self._socket = context.wrap_socket(self._socket)
        except SslError as e:
            raise e
        except Exception as e:
            raise SslError(e)

    def handshake(self):
        """
        Process greeting with Tarantool server.

        :raise: ValueError
        :raise: NetworkError
        """
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
        """
        Create a connection to the host and port specified on initialization.
        There is no need to call this method explicitly until you
        have set ``connect_now=False`` on initialization.

        :raise: :py:exc:`~tarantool.error.NetworkError`
        :raise: :py:exc:`~tarantool.error.SslError`
        :raise: :py:exc:`~tarantool.error.SchemaError`
        :raise: :py:exc:`~tarantool.error.DatabaseError`
        """
        try:
            self.connect_basic()
            if self.transport == SSL_TRANSPORT:
                self.wrap_socket_ssl()
            self.handshake()
            self.load_schema()
        except SslError as e:
            raise e
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
                    "Packet too large. Closing connection to server"
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
        """
        Read response from the transport (socket).

        :return: tuple of the form (header, body)
        :rtype: tuple of two byte arrays
        """
        # Read packet length
        length = msgpack.unpackb(self._recv(5))
        # Read the packet
        return self._recv(length)

    def _send_request_wo_reconnect(self, request):
        """
        :rtype: `Response` instance or subclass

        :raise: NetworkError
        """
        assert isinstance(request, Request)

        response = None
        while True:
            try:
                self._socket.sendall(bytes(request))
                response = request.response_class(self, self._read_response())
                break
            except SchemaReloadException as e:
                self.update_schema(e.schema_version)
                continue

        return response

    def _opt_reconnect(self):
        """
        Check that the connection is alive
        using low-level recv from libc(ctypes).
        **Bug in Python: timeout is an internal Python construction.
        """
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
            warn("Reconnecting, attempt %d of %d" %
                 (attempt, self.reconnect_max_attempts), NetworkWarning)
            if attempt == self.reconnect_max_attempts:
                raise NetworkError(
                    socket.error(last_errno, errno.errorcode[last_errno]))
            attempt += 1
        if self.transport == SSL_TRANSPORT:
            self.wrap_socket_ssl()
        self.handshake()

    def _send_request(self, request):
        """
        Send a request to the server through the socket.
        Return an instance of the `Response` class.

        :param request: object representing a request
        :type request: `Request` instance

        :rtype: `Response` instance
        """
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
        """
        Execute a CALL request. Call a stored Lua function.

        :param func_name: stored Lua function name
        :type func_name: str
        :param args: list of function arguments
        :type args: list or tuple

        :rtype: `Response` instance
        """
        assert isinstance(func_name, str)

        # This allows to use a tuple or list as an argument
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        request = RequestCall(self, func_name, args, self.call_16)
        response = self._send_request(request)
        return response

    def eval(self, expr, *args):
        """
        Execute an EVAL request. Eval a Lua expression.

        :param expr: Lua expression
        :type expr: str
        :param args: list of function arguments
        :type args: list or tuple

        :rtype: `Response` instance
        """
        assert isinstance(expr, str)

        # This allows to use a tuple or list as an argument
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        request = RequestEval(self, expr, args)
        response = self._send_request(request)
        return response

    def replace(self, space_name, values):
        """
        Execute a REPLACE request.
        Doesn't throw an error if there is no tuple with the specified PK.

        :param int space_name: space id to insert a record
        :type space_name: int or str
        :param values: record to be inserted. The tuple must contain
            only scalar (integer or strings) values
        :type values: tuple

        :rtype: `Response` instance
        """
        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        request = RequestReplace(self, space_name, values)
        return self._send_request(request)

    def authenticate(self, user, password):
        """
        Execute an AUTHENTICATE request.

        :param string user: user to authenticate
        :param string password: password for the user

        :rtype: `Response` instance
        """
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
            if isinstance(op[1], str):
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
        """
        Execute an INSERT request.
        Throws an error if there is a tuple with the same PK.

        :param int space_name: space id to insert the record
        :type space_name: int or str
        :param values: record to be inserted. The tuple must contain
            only scalar (integer or strings) values
        :type values: tuple

        :rtype: `Response` instance
        """
        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        request = RequestInsert(self, space_name, values)
        return self._send_request(request)

    def delete(self, space_name, key, *, index=0):
        """
        Execute DELETE request.
        Delete a single record identified by `key`. If you're using a secondary
        index, it must be unique.

        :param space_name: space number or name to delete a record
        :type space_name: int or name
        :param key: key that identifies a record
        :type key: int or str

        :rtype: `Response` instance
        """

        key = check_key(key)
        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        request = RequestDelete(self, space_name, index, key)
        return self._send_request(request)

    def upsert(self, space_name, tuple_value, op_list, *, index=0):
        """
        Execute UPSERT request.

        If an existing tuple matches the key fields of
        `tuple_value`, then the request has the same effect as UPDATE
        and the [(field_1, symbol_1, arg_1), ...] parameter is used.

        If there is no tuple matching the key fields of
        `tuple_value`, then the request has the same effect as INSERT
        and the `tuple_value` parameter is used. However, unlike insert
        or update, upsert will neither read the tuple nor perform error checks
        before returning -- this is a design feature which enhances
        throughput but requires more caution on the part of the user.

        If you're using a secondary index, it must be unique.

        The list of operations allows updating individual fields.
        
        For every operation, you must provide the field number to apply this
        operation to.

        *Allowed operations:*

        * `+` for addition (values must be numeric)
        * `-` for subtraction (values must be numeric)
        * `&` for bitwise AND (values must be unsigned numeric)
        * `|` for bitwise OR (values must be unsigned numeric)
        * `^` for bitwise XOR (values must be unsigned numeric)
        * `:` for string splice (you must provide `offset`, `count`,
          and `value` for this operation)
        * `!` for insertion (provide any element to insert)
        * `=` for assignment (provide any element to assign)
        * `#` for deletion (provide count of fields to delete)

        :param space_name: space number or name to update a record
        :type space_name: int or str
        :param index: index number or name to update a record
        :type index: int or str
        :param tuple_value: tuple
        :type tuple_value:
        :param op_list: list of operations. Each operation
            is a tuple of three (or more) values
        :type op_list: list of the form [(symbol_1, field_1, arg_1),
            (symbol_2, field_2, arg_2_1, arg_2_2, arg_2_3),...]

        :rtype: `Response` instance

        Operation examples:

        .. code-block:: python

            # 'ADD' 55 to the second field
            # Assign 'x' to the third field
            [('+', 2, 55), ('=', 3, 'x')]
            # 'OR' the third field with '1'
            # Cut three symbols, starting from the second,
            # and replace them with '!!'
            # Insert 'hello, world' field before the fifth element of the tuple
            [('|', 3, 1), (':', 2, 2, 3, '!!'), ('!', 5, 'hello, world')]
            # Delete two fields, starting with the second field
            [('#', 2, 2)]
        """
        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        op_list = self._ops_process(space_name, op_list)
        request = RequestUpsert(self, space_name, index, tuple_value,
                                op_list)
        return self._send_request(request)

    def update(self, space_name, key, op_list, *, index=0):
        """
        Execute an UPDATE request.

        The `update` function supports operations on fields — assignment,
        arithmetic (if the field is unsigned numeric), cutting and pasting
        fragments of the field, deleting or inserting a field. Multiple
        operations can be combined in a single update request, and in this
        case they are performed atomically and sequentially. Each operation
        requires that you specify a field number. With multiple operations,
        the field number for each operation is assumed to be relative
        to the most recent state of the tuple, that is, as if all previous
        operations in the multi-operation update have already been applied.
        In other words, it is always safe to merge multiple update invocations
        into a single invocation, with no change in semantics.

        Update a single record identified by `key`.

        The list of operations allows updating individual fields.
        
        For every operation, you must provide the field number to apply this
        operation to.

        *Allowed operations:*

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

        :param space_name: space number or name to update the record
        :type space_name: int or str
        :param index: index number or name to update the record
        :type index: int or str
        :param key: key that identifies the record
        :type key: int or str
        :param op_list: list of operations. Each operation
            is a tuple of three (or more) values
        :type op_list: list of the form [(symbol_1, field_1, arg_1),
            (symbol_2, field_2, arg_2_1, arg_2_2, arg_2_3), ...]

        :rtype: ``Response`` instance

        Operation examples:

        .. code-block:: python

            # 'ADD' 55 to second field
            # Assign 'x' to the third field
            [('+', 2, 55), ('=', 3, 'x')]
            # 'OR' the third field with '1'
            # Cut three symbols, starting from second,
            # and replace them with '!!'
            # Insert 'hello, world' field before the fifth element of the tuple
            [('|', 3, 1), (':', 2, 2, 3, '!!'), ('!', 5, 'hello, world')]
            # Delete two fields, starting with the second field
            [('#', 2, 2)]
        """

        key = check_key(key)
        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        op_list = self._ops_process(space_name, op_list)
        request = RequestUpdate(self, space_name, index, key, op_list)
        return self._send_request(request)

    def ping(self, notime=False):
        """
        Execute a PING request.
        Send an empty request and receive an empty response from the server.

        :return: response time in seconds
        :rtype: float
        """

        request = RequestPing(self)
        t0 = time.time()
        self._send_request(request)
        t1 = time.time()

        if notime:
            return "Success"
        return t1 - t0

    def select(self, space_name, key=None, *, offset=0, limit=0xffffffff, index=0, iterator=None):
        """
        Execute a SELECT request.
        Select and retrieve data from the database.

        :param space_name: space to query
        :type space_name: int or str
        :param values: values to search by index
        :type values: list, tuple, set, frozenset of tuples
        :param index: index to search by (default is **0**, which
            means that the **primary index** is used)
        :type index: int or str
        :param offset: offset in the resulting tuple set
        :type offset: int
        :param limit: limits the total number of returned tuples
        :type limit: int

        :rtype: `Response` instance

        You can use index/space names. The driver will get
        the matching id's -> names from the server.

        Select a single record (from space=0 and using index=0)
        >>> select(0, 1)

        Select a single record from space=0 (with name='space') using
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
        """

        if iterator is None:
            iterator = ITERATOR_EQ
            if key is None or (isinstance(key, (list, tuple)) and
                               len(key) == 0):
                iterator = ITERATOR_ALL

        # Perform smart type checking (scalar / list of scalars / list of
        # tuples)
        key = check_key(key, select=True)

        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        request = RequestSelect(self, space_name, index, key, offset,
                                limit, iterator)
        response = self._send_request(request)
        return response

    def space(self, space_name):
        """
        Create a `Space` instance for a particular space.

        A `Space` instance encapsulates the identifier
        of the space and provides a more convenient syntax
        for accessing the database space.

        :param space_name: identifier of the space
        :type space_name: int or str

        :rtype: `Space` instance
        """
        return Space(self, space_name)

    def generate_sync(self):
        """
        Need override for async io connection.
        """
        return 0

    def execute(self, query, params=None):
        """
        Execute an SQL request.

        The Tarantool binary protocol for SQL requests
        supports "qmark" and "named" param styles.
        A sequence of values can be used for "qmark" style.
        A mapping is used for "named" param style
        without the leading colon in the keys.

        Example for "qmark" arguments:
        >>> args = ['email@example.com']
        >>> c.execute('select * from "users" where "email"=?', args)

        Example for "named" arguments:
        >>> args = {'email': 'email@example.com'}
        >>> c.execute('select * from "users" where "email"=:email', args)

        :param query: SQL syntax query
        :type query: str

        :param params: bind values to use in the query
        :type params: list, dict

        :return: query result data
        :rtype: `Response` instance
        """
        if not params:
            params = []
        request = RequestExecute(self, query, params)
        response = self._send_request(request)
        return response
