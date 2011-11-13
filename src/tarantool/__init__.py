# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

from tarantool.connection import Connection


def connect(host="localhost", port=33013):
    return Connection(host, port)
