# -*- coding: utf-8 -*-
'''
This module provides MeshConnection class with automatic switch
between tarantool instances and basic Round-Robin strategy.
'''

from tarantool.connection import Connection
from tarantool.error import NetworkError
from tarantool.utils import ENCODING_DEFAULT
from tarantool.const import (
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY
)


class RoundRobinStrategy(object):
    def __init__(self, addrs):
        self.addrs = addrs
        self.pos = 0

    def getnext(self):
        tmp = self.pos
        self.pos = (self.pos + 1) % len(self.addrs)
        return self.addrs[tmp]


class MeshConnection(Connection):
    def __init__(self, addrs,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 strategy_class=RoundRobinStrategy):
        self.nattempts = 2 * len(addrs) + 1
        self.strategy = strategy_class(addrs)
        addr = self.strategy.getnext()
        host = addr['host']
        port = addr['port']
        super(MeshConnection, self).__init__(host=host,
                                             port=port,
                                             user=user,
                                             password=password,
                                             socket_timeout=socket_timeout,
                                             reconnect_max_attempts=reconnect_max_attempts,
                                             reconnect_delay=reconnect_delay,
                                             connect_now=connect_now,
                                             encoding=encoding)

    def _opt_reconnect(self):
        nattempts = self.nattempts
        while nattempts > 0:
            try:
                super(MeshConnection, self)._opt_reconnect()
                break
            except NetworkError:
                nattempts -= 1
                addr = self.strategy.getnext()
                self.host = addr['host']
                self.port = addr['port']
        else:
            raise NetworkError
