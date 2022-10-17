# pylint: disable=C0301,W0105,W0401,W0614

import sys

from tarantool.connection import Connection
from tarantool.mesh_connection import MeshConnection
from tarantool.const import (
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    DEFAULT_TRANSPORT,
    DEFAULT_SSL_KEY_FILE,
    DEFAULT_SSL_CERT_FILE,
    DEFAULT_SSL_CA_FILE,
    DEFAULT_SSL_CIPHERS
)

from tarantool.error import (
    Error,
    DatabaseError,
    NetworkError,
    NetworkWarning,
)

from tarantool.schema import (
    Schema,
    SchemaError
)

from tarantool.utils import (
    ENCODING_DEFAULT,
)

from tarantool.msgpack_ext.types.datetime import (
    Datetime,
)

from tarantool.msgpack_ext.types.interval import (
    Adjust as IntervalAdjust,
    Interval,
)

from tarantool.connection_pool import ConnectionPool, Mode

from tarantool.types import BoxError

try:
    from tarantool.version import __version__
except ImportError:
    __version__ = '0.0.0-dev'


def connect(host="localhost", port=33013, user=None, password=None,
            encoding=ENCODING_DEFAULT, transport=DEFAULT_TRANSPORT,
            ssl_key_file=DEFAULT_SSL_KEY_FILE,
            ssl_cert_file=DEFAULT_SSL_CERT_FILE,
            ssl_ca_file=DEFAULT_SSL_CA_FILE,
            ssl_ciphers=DEFAULT_SSL_CIPHERS):
    """
    Create a connection to the Tarantool server.

    :param host: Refer to :paramref:`~tarantool.Connection.params.host`.

    :param port: Refer to :paramref:`~tarantool.Connection.params.port`.

    :param user: Refer to :paramref:`~tarantool.Connection.params.user`.

    :param password: Refer to
        :paramref:`~tarantool.Connection.params.password`.

    :param encoding: Refer to
        :paramref:`~tarantool.Connection.params.encoding`.

    :param transport: Refer to
        :paramref:`~tarantool.Connection.params.transport`.

    :param ssl_key_file: Refer to
        :paramref:`~tarantool.Connection.params.ssl_key_file`.

    :param ssl_cert_file: Refer to
        :paramref:`~tarantool.Connection.params.ssl_cert_file`.

    :param ssl_ca_file: Refer to
        :paramref:`~tarantool.Connection.params.ssl_ca_file`.

    :param ssl_ciphers: Refer to
        :paramref:`~tarantool.Connection.params.ssl_ciphers`.

    :rtype: :class:`~tarantool.Connection`

    :raise: :class:`~tarantool.Connection` exceptions
    """

    return Connection(host, port,
                      user=user,
                      password=password,
                      socket_timeout=SOCKET_TIMEOUT,
                      reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                      reconnect_delay=RECONNECT_DELAY,
                      connect_now=True,
                      encoding=encoding,
                      transport=transport,
                      ssl_key_file=ssl_key_file,
                      ssl_cert_file=ssl_cert_file,
                      ssl_ca_file=ssl_ca_file,
                      ssl_ciphers=ssl_ciphers)


def connectmesh(addrs=({'host': 'localhost', 'port': 3301},), user=None,
                password=None, encoding=ENCODING_DEFAULT):
    """
    Create a connection to a cluster of Tarantool servers.

    :param addrs: Refer to
        :paramref:`~tarantool.MeshConnection.params.addrs`.

    :param user: Refer to
        :paramref:`~tarantool.MeshConnection.params.user`.

    :param password: Refer to
        :paramref:`~tarantool.MeshConnection.params.password`.

    :param encoding: Refer to
        :paramref:`~tarantool.MeshConnection.params.encoding`.

    :rtype: :class:`~tarantool.MeshConnection`

    :raise: :class:`~tarantool.MeshConnection` exceptions
    """

    return MeshConnection(addrs=addrs,
                          user=user,
                          password=password,
                          socket_timeout=SOCKET_TIMEOUT,
                          reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                          reconnect_delay=RECONNECT_DELAY,
                          connect_now=True,
                          encoding=encoding)


__all__ = ['connect', 'Connection', 'connectmesh', 'MeshConnection', 'Schema',
           'Error', 'DatabaseError', 'NetworkError', 'NetworkWarning',
           'SchemaError', 'dbapi', 'Datetime', 'Interval', 'IntervalAdjust',
           'ConnectionPool', 'Mode', 'BoxError',]
