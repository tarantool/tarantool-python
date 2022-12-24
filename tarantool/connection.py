# pylint: disable=C0301,W0105,W0401,W0614
"""
This module provides API for interaction with a Tarantool server.
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

from tarantool.response import (
    unpacker_factory as default_unpacker_factory,
    Response,
)
from tarantool.request import (
    packer_factory as default_packer_factory,
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
    RequestExecute,
    RequestProtocolVersion,
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
    DEFAULT_SSL_PASSWORD,
    DEFAULT_SSL_PASSWORD_FILE,
    REQUEST_TYPE_OK,
    REQUEST_TYPE_ERROR,
    IPROTO_GREETING_SIZE,
    ITERATOR_EQ,
    ITERATOR_ALL,
    CONNECTOR_IPROTO_VERSION,
    CONNECTOR_FEATURES,
    IPROTO_FEATURE_STREAMS,
    IPROTO_FEATURE_TRANSACTIONS,
    IPROTO_FEATURE_ERROR_EXTENSION,
    IPROTO_FEATURE_WATCHERS,
    IPROTO_AUTH_TYPE,
    IPROTO_CHUNK,
    AUTH_TYPE_CHAP_SHA1,
    AUTH_TYPE_PAP_SHA256,
    AUTH_TYPES,
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
    CrudModuleError,
    CrudModuleManyError,
    SchemaReloadException,
    Warning,
    warn
)
from tarantool.schema import Schema
from tarantool.utils import (
    greeting_decode,
    version_id,
    wrap_key,
    ENCODING_DEFAULT,
)
from tarantool.crud import (
    CrudResult,
    CrudError,
    call_crud,
)
from typing import Union

# Based on https://realpython.com/python-interface/
class ConnectionInterface(metaclass=abc.ABCMeta):
    """
    Represents a connection to single or multiple Tarantool servers.

    Interface requires that a connection object has methods to open and
    close a connection, check its status, call procedures and evaluate
    Lua code on server, make simple data manipulations and execute SQL
    queries.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'close') and
                callable(subclass.close) and
                hasattr(subclass, 'is_closed') and
                callable(subclass.is_closed) and
                hasattr(subclass, 'connect') and
                callable(subclass.connect) and
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
                hasattr(subclass, 'ping') and
                callable(subclass.ping) and
                hasattr(subclass, 'select') and
                callable(subclass.select) and
                hasattr(subclass, 'execute') and
                callable(subclass.execute) or
                NotImplemented)

    @abc.abstractmethod
    def close(self):
        """
        Reference implementation: :meth:`~tarantool.Connection.close`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def is_closed(self):
        """
        Reference implementation
        :meth:`~tarantool.Connection.is_closed`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def connect(self):
        """
        Reference implementation: :meth:`~tarantool.Connection.connect`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def call(self, func_name, *args, on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.call`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def eval(self, expr, *args, on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.eval`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def replace(self, space_name, values, on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.replace`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def insert(self, space_name, values, on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.insert`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, space_name, key, *, index=None, on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.delete`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def upsert(self, space_name, tuple_value, op_list, *, index=None, 
               on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.upsert`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def update(self, space_name, key, op_list, *, index=None, on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.update`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def ping(self, notime):
        """
        Reference implementation: :meth:`~tarantool.Connection.ping`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def select(self, space_name, key, *, offset=None, limit=None,
               index=None, iterator=None, on_push=None, on_push_ctx=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.select`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, query, params):
        """
        Reference implementation: :meth:`~tarantool.Connection.execute`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_insert(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_insert`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_insert_object(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_insert_object`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_insert_many(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_insert_many`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_insert_object_many(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_insert_object_many`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_get(self, space_name, key, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_get`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_update(self, space_name, key, operations=[], opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_update`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_delete(self, space_name, key, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_delete`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_replace(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_replace`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_replace_object(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_replace_object`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_replace_many(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_replace_many`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_replace_object_many(self, space_name, values, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_replace_object_many`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_upsert(self, space_name, values, operations=[], opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_upsert`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_upsert_object(self, space_name, values, operations=[], opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_upsert_object`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_upsert_many(self, space_name, values_operation, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_upsert_many`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_upsert_object_many(self, space_name, values_operation, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_upsert_object_many`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_select(self, space_name: str, conditions=[], opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_select`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_min(self, space_name, index_name, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_min`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_max(self, space_name, index_name, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_max`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_truncate(self, space_name, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_truncate`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_len(self, space_name, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_len`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_storage_info(self, opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_storage_info`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_count(self, space_name, conditions=[], opts={}):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_count`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_stats(self, space_name=None):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_stats`.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def crud_unflatten_rows(self, rows, metadata):
        """
        Reference implementation: :meth:`~tarantool.Connection.crud_unflatten_rows`.
        """

        raise NotImplementedError


class Connection(ConnectionInterface):
    """
    Represents a connection to the Tarantool server.

    A connection object has methods to open and close a connection,
    check its status, call procedures and evaluate Lua code on server,
    make simple data manipulations and execute SQL queries.
    """

    # DBAPI Extension: supply exceptions as attributes on the connection
    Error = Error
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.Error`
    """

    DatabaseError = DatabaseError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.DatabaseError`
    """

    InterfaceError = InterfaceError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.InterfaceError`
    """

    ConfigurationError = ConfigurationError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.ConfigurationError`
    """

    SchemaError = SchemaError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.SchemaError`
    """

    NetworkError = NetworkError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.NetworkError`
    """

    Warning = Warning
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.Warning`
    """

    DataError = DataError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.DataError`
    """

    OperationalError = OperationalError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.OperationalError`
    """

    IntegrityError = IntegrityError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.IntegrityError`
    """

    InternalError = InternalError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.InternalError`
    """

    ProgrammingError = ProgrammingError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.ProgrammingError`
    """

    NotSupportedError = NotSupportedError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.NotSupportedError`
    """

    CrudModuleError = CrudModuleError
    """
    DBAPI compatibility.

    :value: :exc:`~tarantool.error.CrudModuleError`
    """

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
                 ssl_ciphers=DEFAULT_SSL_CIPHERS,
                 ssl_password=DEFAULT_SSL_PASSWORD,
                 ssl_password_file=DEFAULT_SSL_PASSWORD_FILE,
                 packer_factory=default_packer_factory,
                 unpacker_factory=default_unpacker_factory,
                 auth_type=None):
        """
        :param host: Server hostname or IP address. Use ``None`` for
            Unix sockets.
        :type host: :obj:`str` or :obj:`None`

        :param port: Server port or Unix socket path.
        :type port: :obj:`int` or :obj:`str`

        :param user: User name for authentication on the Tarantool
            server.
        :type user: :obj:`str` or :obj:`None`, optional

        :param password: User password for authentication on the
            Tarantool server.
        :type password: :obj:`str` or :obj:`None`, optional

        :param socket_timeout: Timeout on blocking socket operations,
            in seconds (see `socket.settimeout()`_).
        :type socket_timeout: :obj:`float` or :obj:`None`, optional

        :param reconnect_max_attempts: Count of maximum attempts to
            reconnect on API call if connection is lost.
        :type reconnect_max_attempts: :obj:`int`, optional

        :param reconnect_delay: Delay between attempts to reconnect on
            API call if connection is lost, in seconds.
        :type reconnect_delay: :obj:`float`, optional

        :param bool connect_now: If ``True``, connect to server on
            initialization. Otherwise, you have to call
            :meth:`~tarantool.Connection.connect` manually after
            initialization.
        :type connect_now: :obj:`bool`, optional

        :param encoding: ``'utf-8'`` or ``None``. Use ``None`` to work
            with non-UTF8 strings.

            If non-default
            :paramref:`~tarantool.Connection.packer_factory` option is
            used, :paramref:`~tarantool.Connection.encoding` option
            value is ignored on encode until the factory explicitly uses
            its value. If non-default
            :paramref:`~tarantool.Connection.unpacker_factory` option is
            used, :paramref:`~tarantool.Connection.encoding` option
            value is ignored on decode until the factory explicitly uses
            its value.

            If ``'utf-8'``, pack Unicode string (:obj:`str`) to
            MessagePack string (`mp_str`_) and unpack MessagePack string
            (`mp_str`_) Unicode string (:obj:`str`), pack :obj:`bytes`
            to MessagePack binary (`mp_bin`_) and unpack MessagePack
            binary (`mp_bin`_) to :obj:`bytes`.

                +--------------+----+----------------------------------+----+--------------+
                | Python       | -> | MessagePack (Tarantool/Lua)      | -> | Python       |
                +==============+====+==================================+====+==============+
                | :obj:`str`   | -> | `mp_str`_ (``string``)           | -> | :obj:`str`   |
                +--------------+----+----------------------------------+----+--------------+
                | :obj:`bytes` | -> | `mp_bin`_ (``binary``/``cdata``) | -> | :obj:`bytes` |
                +--------------+----+----------------------------------+----+--------------+

            If ``None``, pack Unicode string (:obj:`str`) and
            :obj:`bytes` to MessagePack string (`mp_str`_), unpack
            MessagePack string (`mp_str`_) and MessagePack binary
            (`mp_bin`_) to :obj:`bytes`.

                +--------------+----+----------------------------------+----+--------------+
                | Python       | -> | MessagePack (Tarantool/Lua)      | -> | Python       |
                +==============+====+==================================+====+==============+
                | :obj:`bytes` | -> | `mp_str`_ (``string``)           | -> | :obj:`bytes` |
                +--------------+----+----------------------------------+----+--------------+
                | :obj:`str`   | -> | `mp_str`_ (``string``)           | -> | :obj:`bytes` |
                +--------------+----+----------------------------------+----+--------------+
                |              | -> | `mp_bin`_ (``binary``/``cdata``) | -> | :obj:`bytes` |
                +--------------+----+----------------------------------+----+--------------+

        :type encoding: :obj:`str` or :obj:`None`, optional

        :param use_list:
            If ``True``, unpack MessagePack array (`mp_array`_) to
            :obj:`list`. Otherwise, unpack to :obj:`tuple`.

            If non-default
            :paramref:`~tarantool.Connection.unpacker_factory` option is
            used,
            :paramref:`~tarantool.Connection.use_list` option value is
            ignored on decode until the factory explicitly uses its
            value.
        :type use_list: :obj:`bool`, optional

        :param call_16:
            If ``True``, enables compatibility mode with Tarantool 1.6
            and older for `call` operations.
        :type call_16: :obj:`bool`, optional

        :param connection_timeout: Time to establish initial socket
            connection, in seconds.
        :type connection_timeout: :obj:`float` or :obj:`None`, optional

        :param transport: ``''`` or ``'ssl'``. Set to ``'ssl'`` to
            enable SSL encryption for a connection (requires
            Python >= 3.5).
        :type transport: :obj:`str`, optional

        :param ssl_key_file: Path to a private SSL key file. Mandatory,
            if the server uses a trusted certificate authorities (CA)
            file.
        :type ssl_key_file: :obj:`str` or :obj:`None`, optional

        :param str ssl_cert_file: Path to a SSL certificate file.
            Mandatory, if the server uses a trusted certificate
            authorities (CA) file.
        :type ssl_cert_file: :obj:`str` or :obj:`None`, optional

        :param ssl_ca_file: Path to a trusted certificate authority (CA)
            file.
        :type ssl_ca_file: :obj:`str` or :obj:`None`, optional

        :param ssl_ciphers: Colon-separated (``:``) list of SSL cipher
            suites the connection can use.
        :type ssl_ciphers: :obj:`str` or :obj:`None`, optional

        :param ssl_password: Password for decrypting
            :paramref:`~tarantool.Connection.ssl_key_file`.
        :type ssl_password: :obj:`str` or :obj:`None`, optional

        :param ssl_password_file: File with password for decrypting
            :paramref:`~tarantool.Connection.ssl_key_file`. Connection
            tries every line from the file as a password.
        :type ssl_password_file: :obj:`str` or :obj:`None`, optional

        :param packer_factory: Request MessagePack packer factory.
            Supersedes :paramref:`~tarantool.Connection.encoding`. See
            :func:`~tarantool.request.packer_factory` for example of
            a packer factory.
        :type packer_factory:
            callable[[:obj:`~tarantool.Connection`], :obj:`~msgpack.Packer`],
            optional

        :param unpacker_factory: Response MessagePack unpacker factory.
            Supersedes :paramref:`~tarantool.Connection.encoding` and
            :paramref:`~tarantool.Connection.use_list`. See
            :func:`~tarantool.response.unpacker_factory` for example of
            an unpacker factory.
        :type unpacker_factory:
            callable[[:obj:`~tarantool.Connection`], :obj:`~msgpack.Unpacker`],
            optional

        :param auth_type: Authentication method: ``"chap-sha1"`` (supported in
            Tarantool CE and EE) or ``"pap-sha256"`` (supported in Tarantool EE,
            ``"ssl"`` :paramref:`~tarantool.Connection.transport` must be used).
            If `None`, use authentication method provided by server in IPROTO_ID
            exchange. If server does not provide an authentication method, use
            ``"chap-sha1"``.
        :type auth_type: :obj:`None` or :obj:`str`, optional

        :raise: :exc:`~tarantool.error.ConfigurationError`,
            :meth:`~tarantool.Connection.connect` exceptions

        .. _socket.settimeout(): https://docs.python.org/3/library/socket.html#socket.socket.settimeout
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
        self.ssl_password = ssl_password
        self.ssl_password_file = ssl_password_file
        self._protocol_version = None
        self._features = {
            IPROTO_FEATURE_STREAMS: False,
            IPROTO_FEATURE_TRANSACTIONS: False,
            IPROTO_FEATURE_ERROR_EXTENSION: False,
            IPROTO_FEATURE_WATCHERS: False,
        }
        self._packer_factory_impl = packer_factory
        self._unpacker_factory_impl = unpacker_factory
        self._client_auth_type = auth_type
        self._server_auth_type = None

        if connect_now:
            self.connect()

    def close(self):
        """
        Close a connection to the server. The method is idempotent.
        """

        if self._socket is not None:
            self._socket.close()
        self._socket = None

    def is_closed(self):
        """
        Returns ``True`` if connection is closed. ``False`` otherwise.

        :rtype: :obj:`bool`
        """

        return self._socket is None

    def connect_basic(self):
        """
        Establish a connection to the host and port specified on
        initialization.

        :raise: :exc:`~tarantool.error.NetworkError`

        :meta private:
        """

        if self.host is None:
            self.connect_unix()
        else:
            self.connect_tcp()

    def connect_tcp(self):
        """
        Establish a TCP connection to the host and port specified on
        initialization.

        :raise: :exc:`~tarantool.error.NetworkError`

        :meta private:
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
        Create a connection to the Unix socket specified on
        initialization.

        :raise: :exc:`~tarantool.error.NetworkError`

        :meta private:
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

        :raise: :exc:`~tarantool.error.SslError`

        :meta private:
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
                self._ssl_load_cert_chain(context)
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

    def _ssl_load_cert_chain(self, context):
        """
        Decrypt and load SSL certificate and private key files.
        Mimic Tarantool EE approach here: see `SSL commit`_.

        :param context: SSL context.
        :type context: :obj:`ssl.SSLContext`

        :raise: :exc:`~tarantool.error.SslError`

        :meta private:

        .. _SSL commit: https://github.com/tarantool/tarantool-ee/commit/e1f47dd4adbc6657159c611298aad225883a536b
        """

        exc_list = []

        if self.ssl_password is not None:
            try:
                context.load_cert_chain(certfile=self.ssl_cert_file,
                                        keyfile=self.ssl_key_file,
                                        password=self.ssl_password)
                return
            except Exception as e:
                exc_list.append(e)


        if self.ssl_password_file is not None:
            with open(self.ssl_password_file) as file:
                for line in file:
                    try:
                        context.load_cert_chain(certfile=self.ssl_cert_file,
                                                keyfile=self.ssl_key_file,
                                                password=line.rstrip())
                        return
                    except Exception as e:
                        exc_list.append(e)


        try:
            def password_raise_error():
                raise SslError("Password prompt for decrypting the private " +
                               "key is unsupported, use ssl_password or " +
                               "ssl_password_file")
            context.load_cert_chain(certfile=self.ssl_cert_file,
                                    keyfile=self.ssl_key_file,
                                    password=password_raise_error)

            return
        except Exception as e:
            exc_list.append(e)

        raise SslError(exc_list)

    def handshake(self):
        """
        Process greeting with Tarantool server.

        :raise: :exc:`~ValueError`,
            :exc:`~tarantool.error.NetworkError`

        :meta private:
        """

        greeting_buf = self._recv(IPROTO_GREETING_SIZE)
        greeting = greeting_decode(greeting_buf)
        if greeting.protocol != "Binary":
            raise NetworkError("Unsupported protocol: " + greeting.protocol)
        self.version_id = greeting.version_id
        self._check_features()
        self.uuid = greeting.uuid
        self._salt = greeting.salt
        if self.user:
            self.authenticate(self.user, self.password)

    def connect(self):
        """
        Create a connection to the host and port specified on
        initialization. There is no need to call this method explicitly
        until you have set ``connect_now=False`` on initialization.

        :raise: :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.DatabaseError`
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
        """
        Receive binary data from connection socket.

        :param to_read: Amount of data to read, in bytes.
        :type to_read: :obj:`int`

        :return: Buffer with read data
        :rtype: :obj:`bytes`

        :meta private:
        """

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

        :return: Tuple of the form ``(header, body)``.
        :rtype: :obj:`tuple`

        :meta private:
        """

        # Read packet length
        length = msgpack.unpackb(self._recv(5))
        # Read the packet
        return self._recv(length)

    def _send_request_wo_reconnect(self, request, on_push=None, on_push_ctx=None):
        """
        Send request without trying to reconnect.
        Reload schema, if required.

        :param request: Request to send.
        :type request: :class:`~tarantool.request.Request`

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`

        :meta private:
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

        while response._code == IPROTO_CHUNK:
            if on_push is not None: 
                on_push(response._data, on_push_ctx)
            response = request.response_class(self, self._read_response())

        return response

    def _opt_reconnect(self):
        """
        Check that the connection is alive using low-level recv from
        libc(ctypes).

        :raise: :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        :meta private:
        """

        # **Bug in Python: timeout is an internal Python construction (???).
        if not self._socket:
            return self.connect()

        def check():  # Check that connection is alive
            buf = ctypes.create_string_buffer(2)
            try:
                sock_fd = self._socket.fileno()
            except socket.error as e:
                if e.errno == errno.EBADF:
                    return errno.ECONNRESETtuple_value
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

    def _send_request(self, request, on_push=None, on_push_ctx=None):
        """
        Send a request to the server through the socket.

        :param request: Request to send.
        :type request: :class:`~tarantool.request.Request`

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        :meta private:
        """
        assert isinstance(request, Request)

        self._opt_reconnect()

        return self._send_request_wo_reconnect(request, on_push, on_push_ctx)

    def load_schema(self):
        """
        Fetch space and index schema.

        :raise: :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.DatabaseError`

        :meta private:
        """

        self.schema.fetch_space_all()
        self.schema.fetch_index_all()

    def update_schema(self, schema_version):
        """
        Set new schema version metainfo, reload space and index schema.

        :param schema_version: New schema version metainfo.
        :type schema_version: :obj:`int`

        :raise: :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.DatabaseError`

        :meta private:
        """

        self.schema_version = schema_version
        self.flush_schema()

    def flush_schema(self):
        """
        Reload space and index schema.

        :raise: :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        self.schema.flush()
        self.load_schema()

    def call(self, func_name, *args, on_push=None, on_push_ctx=None):
        """
        Execute a CALL request: call a stored Lua function.

        :param func_name: Stored Lua function name.
        :type func_name: :obj:`str`

        :param args: Stored Lua function arguments.
        :type args: :obj:`tuple`

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`
        """

        assert isinstance(func_name, str)
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        # This allows to use a tuple or list as an argument
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        request = RequestCall(self, func_name, args, self.call_16)
        response = self._send_request(request, on_push, on_push_ctx)
        return response

    def eval(self, expr, *args, on_push=None, on_push_ctx=None):
        """
        Execute an EVAL request: evaluate a Lua expression.

        :param expr: Lua expression.
        :type expr: :obj:`str`

        :param args: Lua expression arguments.
        :type args: :obj:`tuple`

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`
        """

        assert isinstance(expr, str)
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        # This allows to use a tuple or list as an argument
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        request = RequestEval(self, expr, args)
        response = self._send_request(request, on_push, on_push_ctx)
        return response

    def replace(self, space_name, values, on_push=None, on_push_ctx=None):
        """
        Execute a REPLACE request: `replace`_ a tuple in the space.
        Doesn't throw an error if there is no tuple with the specified
        primary key.

        :param space_name: Space name or space id.
        :type space_name: :obj:`str` or :obj:`int`

        :param values: Tuple to be replaced.
        :type values: :obj:`tuple` or :obj:`list`

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _replace: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/replace/
        """

        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        request = RequestReplace(self, space_name, values)
        return self._send_request(request, on_push, on_push_ctx)

    def authenticate(self, user, password):
        """
        Execute an AUTHENTICATE request: authenticate a connection.
        There is no need to call this method explicitly until you want
        to reauthenticate with different parameters.

        :param user: User to authenticate.
        :type user: :obj:`str`

        :param password: Password for the user.
        :type password: :obj:`str`

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`
        """

        self.user = user
        self.password = password
        if not self._socket:
            return self._opt_reconnect()

        request = RequestAuthenticate(self,
                              salt=self._salt,
                              user=self.user,
                              password=self.password,
                              auth_type=self._get_auth_type())
        auth_response = self._send_request_wo_reconnect(request)
        if auth_response.return_code == 0:
            self.flush_schema()
        return auth_response

    def _get_auth_type(self):
        """
        Get authentication method based on client and server settings.

        :rtype: :obj:`str`

        :raise: :exc:`~tarantool.error.DatabaseError`

        :meta private:
        """

        if self._client_auth_type is None:
            if self._server_auth_type is None:
                auth_type = AUTH_TYPE_CHAP_SHA1
            else:
                if self._server_auth_type not in AUTH_TYPES:
                    raise ConfigurationError(f'Unknown server authentication type {self._server_auth_type}')
                auth_type = self._server_auth_type
        else:
            if self._client_auth_type not in AUTH_TYPES:
                raise ConfigurationError(f'Unknown client authentication type {self._client_auth_type}')
            auth_type = self._client_auth_type

        if auth_type == AUTH_TYPE_PAP_SHA256 and self.transport != SSL_TRANSPORT:
            raise ConfigurationError('Use PAP-SHA256 only with ssl transport')

        return auth_type

    def _join_v16(self, server_uuid):
        """
        Execute a JOIN request for Tarantool 1.6 and older.

        :param server_uuid: UUID of Tarantool server to join.
        :type server_uuid: :obj:`str`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`
        """

        request = RequestJoin(self, server_uuid)
        self._socket.sendall(bytes(request))

        while True:
            resp = Response(self, self._read_response())
            yield resp
            if resp.code == REQUEST_TYPE_OK or resp.code >= REQUEST_TYPE_ERROR:
                return
        self.close()  # close connection after JOIN

    def _join_v17(self, server_uuid):
        """
        Execute a JOIN request for Tarantool 1.7 and newer.

        :param server_uuid: UUID of Tarantool server to join.
        :type server_uuid: :obj:`str`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`
        """

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
        """
        Execute a JOIN request: `join`_ a replicaset.

        :param server_uuid: UUID of connector "server".
        :type server_uuid: :obj:`str`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _join: https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#iproto-join-0x41
        """

        self._opt_reconnect()
        if self.version_id < version_id(1, 7, 0):
            return self._join_v16(server_uuid)
        return self._join_v17(server_uuid)

    def subscribe(self, cluster_uuid, server_uuid, vclock=None):
        """
        Execute a SUBSCRIBE request: `subscribe`_ to a replicaset
        updates. Connection is closed after subscribing.

        :param cluster_uuid: UUID of replicaset cluster.
        :type cluster_uuid: :obj:`str`

        :param server_uuid: UUID of connector "server".
        :type server_uuid: :obj:`str`

        :param vclock: Connector "server" vclock.
        :type vclock: :obj:`dict` or :obj:`None`, optional

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _subscribe: https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#iproto-subscribe-0x42
        """

        vclock = vclock or {}
        request = RequestSubscribe(self, cluster_uuid, server_uuid, vclock)
        self._socket.sendall(bytes(request))
        while True:
            resp = Response(self, self._read_response())
            yield resp
            if resp.code >= REQUEST_TYPE_ERROR:
                return
        self.close()  # close connection after SUBSCRIBE

    def insert(self, space_name, values, on_push=None, on_push_ctx=None):
        """
        Execute an INSERT request: `insert`_ a tuple to the space.
        Throws an error if there is already a tuple with the same
        primary key.

        :param space_name: Space name or space id.
        :type space_name: :obj:`str` or :obj:`int`

        :param values: Record to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _insert: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/insert/
        """

        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        request = RequestInsert(self, space_name, values)
        return self._send_request(request, on_push, on_push_ctx)

    def delete(self, space_name, key, *, index=0, on_push=None, on_push_ctx=None):
        """
        Execute a DELETE request: `delete`_ a tuple in the space.

        :param space_name: Space name or space id.
        :type space_name: :obj:`str` or :obj:`int`

        :param key: Key of a tuple to be deleted.

        :param index: Index name or index id. If you're using a
            secondary index, it must be unique. Defaults to primary
            index.
        :type index: :obj:`str` or :obj:`int`, optional

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _delete: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/delete/
        """

        key = wrap_key(key)
        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        request = RequestDelete(self, space_name, index, key)
        return self._send_request(request, on_push, on_push_ctx)

    def upsert(self, space_name, tuple_value, op_list, *, index=0, on_push=None, on_push_ctx=None):
        """
        Execute an UPSERT request: `upsert`_ a tuple to the space.

        If an existing tuple matches the key fields of
        ``tuple_value``, then the request has the same effect as UPDATE
        and the ``[(field_1, symbol_1, arg_1), ...]`` parameter is used.

        If there is no tuple matching the key fields of ``tuple_value``,
        then the request has the same effect as INSERT and the
        ``tuple_value`` parameter is used. However, unlike insert or
        update, upsert will neither read the tuple nor perform error
        checks before returning -- this is a design feature which
        enhances throughput but requires more caution on the part of the
        user.

        :param space_name: Space name or space id.
        :type space_name: :obj:`str` or :obj:`int`

        :param tuple_value: Tuple to be upserted.
        :type tuple_value: :obj:`tuple` or :obj:`list`

        :param op_list: Refer to :meth:`~tarantool.Connection.update`
            :paramref:`~tarantool.Connection.update.params.op_list`.
        :type op_list: :obj:`tuple` or :obj:`list`

        :param index: Index name or index id. If you're using a
            secondary index, it must be unique. Defaults to primary
            index.
        :type index: :obj:`str` or :obj:`int`, optional

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _upsert: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/upsert/
        """

        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        op_list = self._ops_process(space_name, op_list)
        request = RequestUpsert(self, space_name, index, tuple_value,
                                op_list)
        return self._send_request(request, on_push, on_push_ctx)

    def update(self, space_name, key, op_list, *, index=0, on_push=None, on_push_ctx=None):
        """
        Execute an UPDATE request: `update`_ a tuple in the space.

        :param space_name: Space name or space id.
        :type space_name: :obj:`str` or :obj:`int`

        :param key: Key of a tuple to be updated.

        :param op_list: The list of operations to update individual
            fields. Each operation is a :obj:`tuple` of three (or more)
            values: ``(operator, field_identifier, value)``.

            Possible operators are:

            * ``'+'`` for addition. values must be numeric
            * ``'-'`` for subtraction. values must be numeric
            * ``'&'`` for bitwise AND. values must be unsigned numeric
            * ``'|'`` for bitwise OR. values must be unsigned numeric
            * ``'^'`` for bitwise XOR. values must be unsigned numeric
            * ``':'`` for string splice. you must provide ``offset``,
              ``count``, and ``value`` for this operation
            * ``'!'`` for insertion. provide any element to insert)
            * ``'='`` for assignment. (provide any element to assign)
            * ``'#'`` for deletion. provide count of fields to delete)

            Possible field_identifiers are:

            * Positive field number. The first field is 1, the second
              field is 2, and so on.
            * Negative field number. The last field is -1, the
              second-last field is -2, and so on.
              In other words: ``(#tuple + negative field number + 1)``.
            * Name. If the space was formatted with
              ``space_object:format()``, then this can be a string for
              the field ``name`` (Since Tarantool 2.3.1).

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

        :type op_list: :obj:`tuple` or :obj:`list`

        :param index: Index name or index id. If you're using a
            secondary index, it must be unique. Defaults to primary
            index.
        :type index: :obj:`str` or :obj:`int`, optional

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _update: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/update/
        """

        key = wrap_key(key)
        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        op_list = self._ops_process(space_name, op_list)
        request = RequestUpdate(self, space_name, index, key, op_list)
        return self._send_request(request, on_push, on_push_ctx)

    def ping(self, notime=False):
        """
        Execute a PING request: send an empty request and receive
        an empty response from the server.

        :param notime: If ``False``, returns response time.
            Otherwise, it returns ``'Success'``.
        :type notime: :obj:`bool`, optional

        :return: Response time or ``'Success'``.
        :rtype: :obj:`float` or :obj:`str`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`
        """

        request = RequestPing(self)
        t0 = time.time()
        self._send_request(request)
        t1 = time.time()

        if notime:
            return "Success"
        return t1 - t0

    def select(self, space_name, key=None, *, offset=0, limit=0xffffffff, index=0, iterator=None, on_push=None, on_push_ctx=None):
        """
        Execute a SELECT request: `select`_ a tuple from the space.

        :param space_name: Space name or space id.
        :type space_name: :obj:`str` or :obj:`int`

        :param key: Key of a tuple to be selected.
        :type key: optional

        :param offset: Number of tuples to skip.
        :type offset: :obj:`int`, optional

        :param limit: Maximum number of tuples to select.
        :type limit: :obj:`int`, optional

        :param index: Index name or index id to select.
            Defaults to primary index.
        :type limit: :obj:`str` or :obj:`int`, optional

        :param iterator: Index iterator type.

            Iterator types for TREE indexes:

                +---------------+-----------+---------------------------------------------+
                | Iterator type | Arguments | Description                                 |
                +===============+===========+=============================================+
                | ``'EQ'``      | search    | The comparison operator is '==' (equal to). |
                |               | value     | If an index key is equal to a search value, |
                |               |           | it matches.                                 |
                |               |           | Tuples are returned in ascending order by   |
                |               |           | index key. This is the default.             |
                +---------------+-----------+---------------------------------------------+
                | ``'REQ'``     | search    | Matching is the same as for ``'EQ'``.       |
                |               | value     | Tuples are returned in descending order by  |
                |               |           | index key.                                  |
                +---------------+-----------+---------------------------------------------+
                | ``'GT'``      | search    | The comparison operator is '>' (greater     |
                |               | value     | than).                                      |
                |               |           | If an index key is greater than a search    |
                |               |           | value, it matches.                          |
                |               |           | Tuples are returned in ascending order by   |
                |               |           | index key.                                  |
                +---------------+-----------+---------------------------------------------+
                | ``'GE'``      | search    | The comparison operator is '>=' (greater    |
                |               | value     | than or equal to).                          |
                |               |           | If an index key is greater than or equal to |
                |               |           | a search value, it matches.                 |
                |               |           | Tuples are returned in ascending order by   |
                |               |           | index key.                                  |
                +---------------+-----------+---------------------------------------------+
                | ``'ALL'``     | search    | Same as ``'GE'``                            |
                |               | value     |                                             |
                |               |           |                                             |
                +---------------+-----------+---------------------------------------------+
                | ``'LT'``      | search    | The comparison operator is '<' (less than). |
                |               | value     | If an index key is less than a search       |
                |               |           | value, it matches.                          |
                |               |           | Tuples are returned in descending order by  |
                |               |           | index key.                                  |
                +---------------+-----------+---------------------------------------------+
                | ``'LE'``      | search    | The comparison operator is '<=' (less than  |
                |               | value     | or equal to).                               |
                |               |           | If an index key is less than or equal to a  |
                |               |           | search value, it matches.                   |
                |               |           | Tuples are returned in descending order by  |
                |               |           | index key.                                  |
                +---------------+-----------+---------------------------------------------+

            Iterator types for HASH indexes:

                +---------------+-----------+------------------------------------------------+
                | Type          | Arguments | Description                                    |
                +===============+===========+================================================+
                | ``'ALL'``     | none      | All index keys match.                          |
                |               |           | Tuples are returned in ascending order by      |
                |               |           | hash of index key, which will appear to be     |
                |               |           | random.                                        |
                +---------------+-----------+------------------------------------------------+
                | ``'EQ'``      | search    | The comparison operator is '==' (equal to).    |
                |               | value     | If an index key is equal to a search value,    |
                |               |           | it matches.                                    |
                |               |           | The number of returned tuples will be 0 or 1.  |
                |               |           | This is the default.                           |
                +---------------+-----------+------------------------------------------------+
                | ``'GT'``      | search    | The comparison operator is '>' (greater than). |
                |               | value     | If a hash of an index key is greater than a    |
                |               |           | hash of a search value, it matches.            |
                |               |           | Tuples are returned in ascending order by hash |
                |               |           | of index key, which will appear to be random.  |
                |               |           | Provided that the space is not being updated,  |
                |               |           | one can retrieve all the tuples in a space,    |
                |               |           | N tuples at a time, by using                   |
                |               |           | ``iterator='GT',limit=N``                      |
                |               |           | in each search, and using the last returned    |
                |               |           | value from the previous result as the start    |
                |               |           | search value for the next search.              |
                +---------------+-----------+------------------------------------------------+

            Iterator types for BITSET indexes:

                +----------------------------+-----------+----------------------------------------------+
                | Type                       | Arguments | Description                                  |
                +============================+===========+==============================================+
                | ``'ALL'``                  | none      | All index keys match.                        |
                |                            |           | Tuples are returned in their order within    |
                |                            |           | the space.                                   |
                +----------------------------+-----------+----------------------------------------------+
                | ``'EQ'``                   | bitset    | If an index key is equal to a bitset value,  |
                |                            | value     | it matches.                                  |
                |                            |           | Tuples are returned in their order within    |
                |                            |           | the space. This is the default.              |
                +----------------------------+-----------+----------------------------------------------+
                | ``'BITS_ALL_SET'``         | bitset    | If all of the bits which are 1 in the bitset |
                |                            | value     | value are 1 in the index key, it matches.    |
                |                            |           | Tuples are returned in their order within    |
                |                            |           | the space.                                   |
                +----------------------------+-----------+----------------------------------------------+
                | ``'BITS_ANY_SET'``         | bitset    | If any of the bits which are 1 in the bitset |
                |                            | value     | value are 1 in the index key, it matches.    |
                |                            |           | Tuples are returned in their order within    |
                |                            |           | the space.                                   |
                +----------------------------+-----------+----------------------------------------------+
                | ``'BITS_ALL_NOT_SET'``     | bitset    | If all of the bits which are 1 in the bitset |
                |                            | value     | value are 0 in the index key, it matches.    |
                |                            |           | Tuples are returned in their order within    |
                |                            |           | the space.                                   |
                +----------------------------+-----------+----------------------------------------------+

        :param on_push: Сallback for processing out-of-band messages.
        :type on_push: :obj:`function`, optional

        :param on_push_ctx: Сontext for working with on_push callback.
        :type on_push_ctx: optional
        
        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _select: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/select/
        """

        if iterator is None:
            iterator = ITERATOR_EQ
            if key is None or (isinstance(key, (list, tuple)) and
                               len(key) == 0):
                iterator = ITERATOR_ALL

        # Perform smart type checking (scalar / list of scalars / list of
        # tuples)
        key = wrap_key(key, select=True)

        if isinstance(space_name, str):
            space_name = self.schema.get_space(space_name).sid
        if isinstance(index, str):
            index = self.schema.get_index(space_name, index).iid
        if on_push is not None and not callable(on_push):
            raise TypeError('The on_push callback must be callable')

        request = RequestSelect(self, space_name, index, key, offset,
                                limit, iterator)
        response = self._send_request(request, on_push, on_push_ctx)
        return response

    def space(self, space_name):
        """
        Create a :class:`~tarantool.space.Space` instance for a
        particular space.

        :param space_name: Space name or space id.
        :type space_name: :obj:`str` or :obj:`int`

        :rtype: :class:`~tarantool.space.Space`

        :raise: :exc:`~tarantool.error.SchemaError`
        """

        return Space(self, space_name)

    def generate_sync(self):
        """
        Generate IPROTO_SYNC code for a request. Since the connector is
        synchronous, any constant value would be sufficient.

        :return: ``0``
        :rtype: :obj:`int`

        :meta private:
        """

        return 0

    def execute(self, query, params=None):
        """
        Execute an SQL request: see `documentation`_ for syntax
        reference.

        The Tarantool binary protocol for SQL requests supports "qmark"
        and "named" param styles. A sequence of values can be used for
        "qmark" style. A mapping is used for "named" param style
        without the leading colon in the keys.

        Example for "qmark" arguments:

        .. code-block:: python

            args = ['email@example.com']
            c.execute('select * from "users" where "email"=?', args)

        Example for "named" arguments:

        .. code-block:: python

            args = {'email': 'email@example.com'}
            c.execute('select * from "users" where "email"=:email', args)

        :param query: SQL query.
        :type query: :obj:`str`

        :param params: SQL query bind values.
        :type params: :obj:`dict` or :obj:`list` or :obj:`None`,
            optional

        :rtype: :class:`~tarantool.response.Response`

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`

        .. _documentation: https://www.tarantool.io/en/doc/latest/how-to/sql/
        """

        if not params:
            params = []
        request = RequestExecute(self, query, params)
        response = self._send_request(request)
        return response

    def _check_features(self):
        """
        Execute an ID request: inform the server about the protocol
        version and features connector support and get server-side
        information about it.

        After executing this request, the connector will choose a
        protocol version and features supported both by connector and
        server.

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :exc:`~tarantool.error.SslError`
        """

        try:
            request = RequestProtocolVersion(self,
                                             CONNECTOR_IPROTO_VERSION,
                                             CONNECTOR_FEATURES)
            response = self._send_request(request)
            server_protocol_version = response.protocol_version
            server_features = response.features
            server_auth_type = response.auth_type
        except DatabaseError as exc:
            ER_UNKNOWN_REQUEST_TYPE = 48
            if exc.code == ER_UNKNOWN_REQUEST_TYPE:
                server_protocol_version = None
                server_features = []
                server_auth_type = None
            else:
                raise exc

        if server_protocol_version is not None:
            self._protocol_version = min(server_protocol_version,
                                         CONNECTOR_IPROTO_VERSION)

        # Intercept lists of features
        features_list = [val for val in CONNECTOR_FEATURES if val in server_features]
        for val in features_list:
            self._features[val] = True

        self._server_auth_type = server_auth_type

    def _packer_factory(self):
        return self._packer_factory_impl(self)

    def _unpacker_factory(self):
        return self._unpacker_factory_impl(self)

    def crud_insert(self, space_name: str, values: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Inserts row through the 
        `crud <https://github.com/tarantool/crud#insert>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.insert", space_name, values, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_insert_object(self, space_name: str, values: dict, opts: dict={}) -> CrudResult:
        """
        Inserts object row through the 
        `crud <https://github.com/tarantool/crud#insert>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`dict`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, dict)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.insert_object", space_name, values, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_insert_many(self, space_name: str, values: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Inserts batch rows through the 
        `crud <https://github.com/tarantool/crud#insert-many>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.insert_many", space_name, values, opts)

        res = None
        if crud_resp[0] is not None:
            res = CrudResult(crud_resp[0])

        if crud_resp[1] is not None:
            errs = list()
            for err in crud_resp[1]:
                errs.append(CrudError(err))
            raise CrudModuleManyError(res, errs)

        return res

    def crud_insert_object_many(self, space_name: str, values: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Inserts batch object rows through the 
        `crud <https://github.com/tarantool/crud#insert-many>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.insert_object_many", space_name, values, opts)

        res = None
        if crud_resp[0] is not None:
            res = CrudResult(crud_resp[0])

        if crud_resp[1] is not None:
            errs = list()
            for err in crud_resp[1]:
                errs.append(CrudError(err))
            raise CrudModuleManyError(res, errs)

        return res

    def crud_get(self, space_name: str, key: int, opts: dict={}) -> CrudResult:
        """
        Gets row through the 
        `crud <https://github.com/tarantool/crud#get>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param key: The primary key value.
        :type key: :obj:`int`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.get", space_name, key, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_update(self, space_name: str, key: int, operations: list=[], opts: dict={}) -> CrudResult:
        """
        Updates row through the 
        `crud <https://github.com/tarantool/crud#update>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param key: The primary key value.
        :type key: :obj:`int`

        :param operations: The update operations for the crud module.
        :type operations: :obj:`list`, optional

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(operations, list)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.update", space_name, key, operations, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_delete(self, space_name: str, key: int, opts: dict={}) -> CrudResult:
        """
        Deletes row through the 
        `crud <https://github.com/tarantool/crud#delete>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param key: The primary key value.
        :type key: :obj:`int`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.delete", space_name, key, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_replace(self, space_name: str, values: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Replaces row through the 
        `crud <https://github.com/tarantool/crud#replace>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.replace", space_name, values, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_replace_object(self, space_name: str, values: dict, opts: dict={}) -> CrudResult:
        """
        Replaces object row through the 
        `crud <https://github.com/tarantool/crud#replace>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`dict`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, dict)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.replace_object", space_name, values, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_replace_many(self, space_name: str, values: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Replaces batch rows through the 
        `crud <https://github.com/tarantool/crud#replace-many>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.replace_many", space_name, values, opts)

        res = None
        if crud_resp[0] is not None:
            res = CrudResult(crud_resp[0])

        if crud_resp[1] is not None:
            errs = list()
            for err in crud_resp[1]:
                errs.append(CrudError(err))
            raise CrudModuleManyError(res, errs)

        return res

    def crud_replace_object_many(self, space_name: str, values: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Replaces batch object rows through the 
        `crud <https://github.com/tarantool/crud#replace-many>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.replace_object_many", space_name, values, opts)

        res = None
        if crud_resp[0] is not None:
            res = CrudResult(crud_resp[0])

        if crud_resp[1] is not None:
            errs = list()
            for err in crud_resp[1]:
                errs.append(CrudError(err))
            raise CrudModuleManyError(res, errs)

        return res

    def crud_upsert(self, space_name: str, values: Union[tuple, list], operations: list=[], opts: dict={}) -> CrudResult:
        """
        Upserts row through the 
        `crud <https://github.com/tarantool/crud#upsert>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :param operations: The update operations for the crud module.
        :type operations: :obj:`list`, optional

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, (tuple, list))
        assert isinstance(operations, list)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.upsert", space_name, values, operations, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_upsert_object(self, space_name: str, values: dict, operations: list=[], opts: dict={}) -> CrudResult:
        """
        Upserts object row through the 
        `crud <https://github.com/tarantool/crud#upsert>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values: Tuple to be inserted.
        :type values: :obj:`dict`

        :param operations: The update operations for the crud module.
        :type operations: :obj:`list`, optional

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values, dict)
        assert isinstance(operations, list)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.upsert_object", space_name, values, operations, opts)
        
        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_upsert_many(self, space_name: str, values_operation: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Upserts batch rows through the 
        `crud <https://github.com/tarantool/crud#upsert-many>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values_operation: The data with update operations.
        :type values_operation: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values_operation, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.upsert_many", space_name, values_operation, opts)

        res = None
        if crud_resp[0] is not None:
            res = CrudResult(crud_resp[0])

        if crud_resp[1] is not None:
            errs = list()
            for err in crud_resp[1]:
                errs.append(CrudError(err))
            raise CrudModuleManyError(res, errs)

        return res

    def crud_upsert_object_many(self, space_name: str, values_operation: Union[tuple, list], opts: dict={}) -> CrudResult:
        """
        Upserts batch object rows through the 
        `crud <https://github.com/tarantool/crud#upsert-many>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param values_operation: The data with update operations.
        :type values_operation: :obj:`tuple` or :obj:`list`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(values_operation, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.upsert_object_many", space_name, values_operation, opts)

        res = None
        if crud_resp[0] is not None:
            res = CrudResult(crud_resp[0])

        if crud_resp[1] is not None:
            errs = list()
            for err in crud_resp[1]:
                errs.append(CrudError(err))
            raise CrudModuleManyError(res, errs)

        return res

    def crud_select(self, space_name: str, conditions: list=[], opts: dict={}) -> CrudResult:
        """
        Selects rows through the 
        `crud <https://github.com/tarantool/crud#select>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param conditions: The select conditions for the crud module.
        :type conditions: :obj:`list`, optional

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`select
        """

        assert isinstance(space_name, str)
        assert isinstance(conditions, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.select", space_name, conditions, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_min(self, space_name: str, index_name: str, opts: dict={}) -> CrudResult:
        """
        Gets rows with minimum value in the specified index through 
        the `crud <https://github.com/tarantool/crud#min-and-max>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param index_name: The name of the index.
        :type index_name: :obj:`str`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.min", space_name, index_name, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_max(self, space_name: str, index_name: str, opts: dict={}) -> CrudResult:
        """
        Gets rows with maximum value in the specified index through 
        the `crud <https://github.com/tarantool/crud#min-and-max>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param index_name: The name of the index.
        :type index_name: :obj:`str`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.max", space_name, index_name, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return CrudResult(crud_resp[0])

    def crud_truncate(self, space_name: str, opts: dict={}) -> bool:
        """
        Truncate rows through 
        the `crud <https://github.com/tarantool/crud#truncate>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :obj:`bool`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.truncate", space_name, opts)

        # In absence of an error, crud does not give 
        # variable err as nil (as in most cases).
        if len(crud_resp) != 1:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return crud_resp[0]
    
    def crud_len(self, space_name: str, opts: dict={}) -> int:
        """
        Gets the number of tuples in the space through 
        the `crud <https://github.com/tarantool/crud#len>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :obj:`int`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.len", space_name, opts)

        # In absence of an error, crud does not give 
        # variable err as nil (as in most cases).
        if len(crud_resp) != 1:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return crud_resp[0]

    def crud_storage_info(self, opts: dict={}) -> dict:
        """
        Gets storages status through the 
        `crud <https://github.com/tarantool/crud#storage-info>`__.

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :obj:`dict`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.storage_info", opts)

        # In absence of an error, crud does not give 
        # variable err as nil (as in most cases).
        if len(crud_resp) != 1:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return crud_resp[0]

    def crud_count(self, space_name: str, conditions: list=[], opts: dict={}) -> int:
        """
        Gets rows count through the 
        `crud <https://github.com/tarantool/crud#count>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`

        :param conditions: The conditions for the crud module.
        :type conditions: :obj:`list`, optional

        :param opts: The opts for the crud module.
        :type opts: :obj:`dict`, optional

        :rtype: :obj:`int`

        :raise: :exc:`~tarantool.error.CrudModuleError`,
            :exc:`~tarantool.error.DatabaseError`
        """

        assert isinstance(space_name, str)
        assert isinstance(conditions, (tuple, list))
        assert isinstance(opts, dict)

        crud_resp = call_crud(self, "crud.count", space_name, conditions, opts)

        if crud_resp[1] is not None:
            raise CrudModuleError(None, CrudError(crud_resp[1]))

        return crud_resp[0]

    def crud_stats(self, space_name: str=None) -> CrudResult:
        """
        Gets statistics from the 
        `crud <https://github.com/tarantool/crud#statistics>`__.

        :param space_name: The name of the target space.
        :type space_name: :obj:`str`, optional

        :rtype: :class:`~tarantool.crud.CrudResult`

        :raise: :exc:`~tarantool.error.DatabaseError`
        """

        if space_name is not None:
            assert isinstance(space_name, str)

        crud_resp = call_crud(self, "crud.stats", space_name)

        res = None
        if len(crud_resp.data[0]) > 0:
            res = CrudResult(crud_resp.data[0])

        return res

    def crud_unflatten_rows(self, rows: list, metadata: list) -> list:
        """
        Makes rows unflatten through the 
        `crud <https://github.com/tarantool/crud#api>`__.

        :param rows: The rows to unflatten.
        :type rows: :obj:`list`

        :param metadata: The metadata to unflatten.
        :type metadata: :obj:`list`

        :rtype: :obj:`list`
        """

        assert isinstance(rows, (tuple, list))
        assert isinstance(metadata, (tuple, list))

        res = []
        for row in rows:
            row_res = {}
            for field_idx, field in enumerate(row):
                row_res[metadata[field_idx]['name']] = field
            res.append(row_res)

        return res
