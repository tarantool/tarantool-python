# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

__version__ = "0.3.0"

from tarantool.connection import Connection
from tarantool.const import *
from tarantool.error import *
from tarantool.schema import *

def connect(host="localhost", port=33013, schema=None):
    '''\
    Create a connection to the Tarantool server.
    
    :param str host: Server hostname or IP-address
    :param int port: Server port
    :param schema: Data schema (see Developer guide and :class:`~tarantool.schema.Schema`)
    :type schema: :class:`~tarantool.schema.Schema` or dict

    :rtype: :class:`~tarantool.connection.Connection`
    :raise: `NetworkError`
    '''
    
    return Connection(host, port,
                      socket_timeout=SOCKET_TIMEOUT,
                      reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                      reconnect_delay=RECONNECT_DELAY,
                      connect_now=True,
                      schema = schema)
