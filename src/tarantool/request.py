# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Request types definitions
'''

import struct

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

    # Pre-generated results of pack_int_base128() for small arguments
    # (0..16383)
    _int_base128 = tuple(
        (
            struct_B.pack(val) if val < 128 else struct_BB.pack(
                val >> 7 & 0xff | 0x80, val & 0x7F)
            for val in xrange(0x4000)
        )
    )

    def __init__(self, conn):
        self._bytes = None
        self.conn = conn

    def __bytes__(self):
        return self._bytes
    __str__ = __bytes__

    @classmethod
    def header(cls, body_length):
        return struct_LLL.pack(cls.request_type, body_length, 0)

    @classmethod
    def pack_int_base128(cls, value):
        '''\
        Pack integer value using BER128 encoding
        :param value: integer value to encode
        :type value: int

        :return: encoded value
        :rtype: bytes
        '''

        if value < 1 << 14:
            return cls._int_base128[value]

        if value < 1 << 21:
            return struct_BBB.pack(
                value >> 14 & 0xff | 0x80,
                value >> 7 & 0xff | 0x80,
                value & 0x7F
            )

        if value < 1 << 28:
            return struct_BBBB.pack(
                value >> 21 & 0xff | 0x80,
                value >> 14 & 0xff | 0x80,
                value >> 7 & 0xff | 0x80,
                value & 0x7F
            )

        if value < 1 << 35:
            return struct_BBBBB.pack(
                value >> 28 & 0xff | 0x80,
                value >> 21 & 0xff | 0x80,
                value >> 14 & 0xff | 0x80,
                value >> 7 & 0xff | 0x80,
                value & 0x7F
            )

        raise OverflowError("Number too large to be packed")

    def pack_field(self, value):
        value_len_packed = Request.pack_int_base128(len(value))
        return struct.pack(
            "<%ds%ds" % (len(value_len_packed), len(value)), value_len_packed,
            value)

    def pack_fields(self, packed_values):
        '''\
        Pack tuple of values
        <tuple> ::= <cardinality><field>+

        :param value: tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)

        :return: packed tuple
        :rtype: bytes
        '''
        assert isinstance(packed_values, (tuple, list))
        cardinality = struct_L.pack(len(packed_values))
        packed_items = []
        packed_items.append(cardinality)
        for value in packed_values:
            packed_items.append(self.pack_field(value))
        return b"".join(packed_items)

    def pack_tuple(self, values, space_no=None):
        '''\
        Pack tuple of values
        <tuple> ::= <cardinality><field>+

        :param value: tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)

        :return: packed tuple
        :rtype: bytes
        '''
        return self.pack_fields(self.conn.schema.pack_values(values, space_no))

    def pack_key_tuple(self, values, space_no=None, index_no=None):
        '''\
        Pack key tuple
        <tuple> ::= <cardinality><field>+

        :param value: key tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)

        :return: packed tuple
        :rtype: bytes
        '''
        return self.pack_fields(
            self.conn.schema.pack_key(values, space_no, index_no))


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

        assert isinstance(values, (tuple, list))

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
            op_arg = self.conn.schema.pack_value(op_arg)
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
