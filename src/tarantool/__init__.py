# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

__version__ = "0.3.4"

from tarantool.connection import Connection
from tarantool.const import (
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY
)

from tarantool.schema import (
    Schema,
    RAW,
    STR,
    NUM,
    NUM64
)
from tarantool.error import (
    Error,
    DatabaseError,
    NetworkError,
    NetworkWarning,
    RetryWarning
)


def connect(host="localhost", port=33013, schema=None):
    '''\
    Create a connection to the Tarantool server.

    :param str host: Server hostname or IP-address
    :param int port: Server port
    :param schema: Data schema (see Developer guide
        and :class:`~tarantool.schema.Schema`)
    :type schema: :class:`~tarantool.schema.Schema` or dict

    :rtype: :class:`~tarantool.connection.Connection`
    :raise: `NetworkError`
    '''

    return Connection(host, port,
                      socket_timeout=SOCKET_TIMEOUT,
                      reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                      reconnect_delay=RECONNECT_DELAY,
                      connect_now=True,
                      schema=schema)

__all__ = ['connect', 'Connection', 'Schema', 'Error', 'DatabaseError',
           'NetworkError', 'NetworkWarning', 'RetryWarning', 'RAW', 'STR',
           'NUM', 'NUM64']
