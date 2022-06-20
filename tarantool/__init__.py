# -*- coding: utf-8 -*-
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

__version__ = "0.9.0"


def connect(host="localhost", port=33013, user=None, password=None,
            encoding=ENCODING_DEFAULT, transport=DEFAULT_TRANSPORT,
            ssl_key_file=DEFAULT_SSL_KEY_FILE,
            ssl_cert_file=DEFAULT_SSL_CERT_FILE,
            ssl_ca_file=DEFAULT_SSL_CA_FILE,
            ssl_ciphers=DEFAULT_SSL_CIPHERS):
    '''
    Create a connection to the Tarantool server.

    :param str host: Server hostname or IP-address
    :param int port: Server port

    :rtype: :class:`~tarantool.connection.Connection`

    :raise: `NetworkError`
    '''

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
    '''
    Create a connection to the mesh of Tarantool servers.

    :param list addrs: A list of maps: {'host':(HOSTNAME|IP_ADDR), 'port':PORT}.

    :rtype: :class:`~tarantool.mesh_connection.MeshConnection`

    :raise: `NetworkError`
    '''

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
           'SchemaError', 'dbapi']

# ConnectionPool is supported only for Python 3.7 or newer.
if sys.version_info.major >= 3 and sys.version_info.minor >= 7:
    from tarantool.connection_pool import ConnectionPool, Mode
    __all__.extend(['ConnectionPool', 'Mode'])
