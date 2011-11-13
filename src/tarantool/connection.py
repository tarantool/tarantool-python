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


    def _select(self, space_no, index_no, values, offset=0, limit=0xffffffff):

        # 'values' argument must be a list of tuples
        assert isinstance(values, (list, tuple))
        assert len(values) != 0
        assert isinstance(values[0], (list, tuple))

        request = RequestSelect(space_no, index_no, values, offset, limit)
        response = self._send_request(request)
        return response


    def select(self, space_no, index_no, values, offset=0, limit=0xffffffff):
        '''\
        Select one single record (from space=0 and using index=0)
        >>> select(0, 0, 1)

        Select several records using single-valued index
        >>> select(0, 0, [1, 2, 3])
        >>> select(0, 0, [(1,), (2,), (3,)]) # the same as above

        Select serveral records using composite index
        >>> select(0, 1, [(1,'2'), (2,'3'), (3,'4')])

        Select single record using composite index
        >>> select(0, 1, [(1,'2')])
        This is incorrect
        >>> select(0, 1, (1,'2'))
        '''

        # Perform smart type cheching (scalar / list of scalars / list of tuples)
        if isinstance(values, (int, basestring)): # scalar
            # This request is looking for one single record
            values = [(values, )]
        elif isinstance(values, (list, tuple, set, frozenset)):
            assert len(values) > 0
            if isinstance(values[0], (int, basestring)): # list of scalars
                # This request is looking for several records using single-valued index
                # Ex: select(space_no, index_no, [1, 2, 3])
                # Transform a list of scalar values to a list of tuples
                values = [(v, ) for v in values]
            elif isinstance(values[0], (list, tuple)): # list of tuples
                # This request is looking for serveral records using composite index
                pass
            else:
                raise ValueError("Invalid value type, expected one of scalar (int or str) / list of scalars / list of tuples ")

        return self._select(space_no, index_no, values, offset, limit)
