# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import socket
import time

from tarantool.response import Response
from tarantool.request import (
                    Request,
                    RequestCall,
                    RequestDelete,
                    RequestInsert,
                    RequestSelect,
                    RequestUpdate)
from tarantool.const import *


class Connection(object):

    def __init__(self, host, port, connect=True):
        self._host = host
        self._port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        if connect:
            self.connect()


    def connect(self, host=None, port=None):

        if host:
            self._host = host
        if port:
            self._port = port
        self._socket.connect((self._host, self._port))


    def _send_request(self, request):

        assert isinstance(request, Request)

        self._socket.sendall(bytes(request))
        response = Response(self._socket)
        return response


    def insert(self, space_no, values, return_tuple=False):

        assert isinstance(values, tuple)

        request = RequestInsert(space_no, values, return_tuple)
        return self._send_request(request)


    def delete(self, space_no, key, return_tuple=False):

        assert isinstance(key, (int, basestring))

        request = RequestDelete(space_no, key, return_tuple)
        return self._send_request(request)


    def update(self, space_no, key, op_list, return_tuple=False):

        assert isinstance(key, (int, basestring))

        request = RequestUpdate(space_no, key, op_list, return_tuple)
        return self._send_request(request)


    def ping(self):

        t0 = time.time()
        self._socket.sendall(struct_LLL.pack(0xff00, 0, 0))
        request_type, body_length, request_id = struct_LLL.unpack(self._socket.recv(12)) # pylint: disable=W0612
        t1 = time.time()
        assert request_type == 0xff00
        assert body_length == 0
        return t1 - t0


    def select(self, space_no, index_no, values, offset=0, limit=0xffffffff):

        # 'values' argument must be a list of tuples
        assert isinstance(values, (list, tuple))
        assert len(values) != 0
        assert isinstance(values[0], (list, tuple))

        request = RequestSelect(space_no, index_no, values, offset, limit)
        response = self._send_request(request)
        return response
