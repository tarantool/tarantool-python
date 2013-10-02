# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Request types definitions
'''

import struct
import msgpack

from tarantool.const import (
    struct_B,
    struct_BB,
    struct_BBB,
    struct_BBBB,
    struct_BBBBB,
    struct_L,
    struct_LL,
    struct_LLL,
    struct_LLLLL,
    struct_LB,
    UPDATE_OPERATION_CODE,
    REQUEST_TYPE_INSERT,
    REQUEST_TYPE_DELETE,
    REQUEST_TYPE_SELECT,
    REQUEST_TYPE_UPDATE,
    REQUEST_TYPE_PING,
    REQUEST_TYPE_CALL
)


class Request(object):

    '''\
    Represents a single request to the server in compliance with the
    Tarantool protocol.
    Responsible for data encapsulation and builds binary packet
    to be sent to the server.

    This is the abstract base class. Specific request types
    are implemented by the inherited classes.
    '''
    request_type = None

    def __init__(self, conn):
        self._bytes = None
        self.conn = conn

    def __bytes__(self):
        return self._bytes
    __str__ = __bytes__

    @classmethod
    def header(cls, body_length):
        return struct_LLL.pack(cls.request_type, body_length, 0)

    def pack_field(self, value):
        return msgpack.dumps(value)

    def pack_tuple(self, values, space_no=None):
        '''\
        Pack tuple of values
        <tuple> ::= <msgpack_array>

        :param value: tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)

        :return: packed tuple
        :rtype: bytes
        '''
        assert isinstance(values, (tuple, list))
        return msgpack.dumps(values)

    def pack_key_tuple(self, values, space_no=None, index_no=None):
        '''\
        Pack key tuple
        <tuple> ::= <msgpack_array>

        :param value: key tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)

        :return: packed tuple
        :rtype: bytes
        '''
        assert isinstance(values, (tuple, list))

        return msgpack.dumps(values)


class RequestInsert(Request):

    '''\
    Represents INSERT request

    <insert_request_body> ::= <space_no><flags><tuple>
    |--------------- header ----------------|--------- body ---------|
     <request_type><body_length><request_id> <space_no><flags><tuple>
                                                               |
                          items to add (multiple values)  -----+
    '''
    request_type = REQUEST_TYPE_INSERT

    # pylint: disable=W0231
    def __init__(self, conn, space_name, values, flags):
        '''\
        '''
        super(RequestInsert, self).__init__(conn)

        space_no = self.conn.schema.space_no(space_name)
        request_body = \
            struct_LL.pack(space_no, flags) + \
            self.pack_tuple(values, space_no)

        self._bytes = self.header(len(request_body)) + request_body


class RequestDelete(Request):

    '''
    Represents DELETE request

    <delete_request_body> ::= <space_no><flags><tuple>
    |--------------- header ----------------|--------- body ---------|
     <request_type><body_length><request_id> <space_no><flags><tuple>
                                                               |
                          key to search in primary index  -----+
                          (tuple with single value)
    '''
    request_type = REQUEST_TYPE_DELETE

    # pylint: disable=W0231
    def __init__(self, conn, space_name, key, return_tuple):
        '''
        '''
        super(RequestDelete, self).__init__(conn)
        flags = 1 if return_tuple else 0

        space_no = self.conn.schema.space_no(space_name)
        request_body = \
            struct_LL.pack(space_no, flags) + \
            self.pack_key_tuple((key,), space_no, 0)

        self._bytes = self.header(len(request_body)) + request_body


class RequestSelect(Request):

    '''\
    Represents SELECT request

    <select_request_body> ::= <space_no><index_no><offset><limit><count><tuple>+

    |--------------- header ----------------|---------------request_body ---------------------...|
     <request_type><body_length><request_id> <space_no><index_no><offset><limit><count><tuple>+
                                                        ^^^^^^^^                 ^^^^^^^^^^^^
                                                            |                          |
                                           Index to use ----+                          |
                                                                                       |
                            List of tuples to search in the index ---------------------+
                            (tuple cardinality can be > 1 when using composite indexes)
    '''
    request_type = REQUEST_TYPE_SELECT

    # pylint: disable=W0231
    def __init__(self, conn, space_name, index_name, tuple_list, offset, limit):
        super(RequestSelect, self).__init__(conn)
        assert isinstance(tuple_list, (list, tuple, set, frozenset))

        space_no = self.conn.schema.space_no(space_name)
        index_no = self.conn.schema.index_no(space_no, index_name)
        request_body = \
            struct_LLLLL.pack(space_no, index_no, offset, limit, len(tuple_list)) + \
            b"".join([self.pack_key_tuple(t, space_no, index_no)
                     for t in tuple_list])

        self._bytes = self.header(len(request_body)) + request_body


class RequestUpdate(Request):

    '''
    <update_request_body> ::= <space_no><flags><tuple><count><operation>+
    <operation> ::= <field_no><op_code><op_arg>

    |--------------- header ----------------|---------------request_body --------------...|
     <request_type><body_length><request_id> <space_no><flags><tuple><count><operation>+
                                                               |      |      |
                           Key to search in primary index -----+      |      +-- list of operations
                           (tuple with cardinality=1)                 +-- number of operations
    '''

    request_type = REQUEST_TYPE_UPDATE

    # pylint: disable=W0231
    def __init__(self, conn, space_name, key, op_list, return_tuple):
        super(RequestUpdate, self).__init__(conn)
        flags = 1 if return_tuple else 0
        assert isinstance(key, (int, long, basestring))

        space_no = self.conn.schema.space_no(space_name)
        request_body = \
            struct_LL.pack(space_no, flags) + \
            self.pack_key_tuple((key,), space_no, 0) + \
            struct_L.pack(len(op_list)) +\
            self.pack_operations(op_list)

        self._bytes = self.header(len(request_body)) + request_body

    def pack_operations(self, op_list):
        result = []
        for op in op_list:
            try:
                field_no, op_symbol, op_arg = op
            except ValueError:
                raise ValueError(
                    "Operation must be a tuple of 3 elements "
                    "(field_id, op, value)")
            try:
                op_code = UPDATE_OPERATION_CODE[op_symbol]
            except KeyError:
                raise ValueError(
                    "Invalid operaction symbol '%s', expected one of %s" % (
                        op_symbol,
                        ', '.join(["'%s'" % c for c in sorted(
                            UPDATE_OPERATION_CODE.keys())])))
            data = b"".join(
                [struct_LB.pack(field_no, op_code), self.pack_field(op_arg)])
            result.append(data)
        return b"".join(result)


class RequestCall(Request):

    '''
    <call_request_body> ::= <flags><proc_name><tuple>
    <proc_name> ::= <field>

    |--------------- header ----------------|-----request_body -------|
     <request_type><body_length><request_id> <flags><proc_name><tuple>
                                                                |
                                    Lua function arguments -----+
    '''
    request_type = REQUEST_TYPE_CALL

    # pylint: disable=W0231
    def __init__(self, conn, proc_name, args, return_tuple):
        super(RequestCall, self).__init__(conn)
        flags = 1 if return_tuple else 0
        assert isinstance(args, (list, tuple))

        request_body = \
            struct_L.pack(flags) + \
            self.pack_field(proc_name) +\
            self.pack_tuple([k for k in args])

        self._bytes = self.header(len(request_body)) + request_body


class RequestPing(Request):

    '''
    Ping body is empty, so body_length == 0 and there's no body
    |--------------- header ----------------|
     <request_type><body_length><request_id>
    '''
    request_typle = REQUEST_TYPE_PING

    def __init__(self, conn):
        super(RequestPing, self).__init__(conn)
        self._bytes = struct_LLL.pack(REQUEST_TYPE_PING, 0, 0)
