# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import struct

from tarantool.const import *


class Request(object):
    request_type = None

    def __init__(self):
        self._bytes = None
        raise NotImplementedError("Abstract method must be overridden")


    def __bytes__(self):
        return self._bytes
    __str__ = __bytes__


    @classmethod
    def header(cls, body_length):
        return struct_LLL.pack(cls.request_type, body_length, 0)


    @staticmethod
    def pack_int(value):

        if __debug__:
            if not isinstance(value, int):
                raise TypeError("Invalid argument type '%s', 'int' expected"%type(value).__name__)
        return struct_BL.pack(4, value)


    @staticmethod
    def pack_int_base128(value):
        """Implement Perl pack's 'w' option, aka base 128 encoding."""
        res = ''
        if value >= 1 << 7:
            if value >= 1 << 14:
                if value >= 1 << 21:
                    if value >= 1 << 28:
                        res += chr(value >> 28 & 0xff | 0x80)
                    res += chr(value >> 21 & 0xff | 0x80)
                res += chr(value >> 14 & 0xff | 0x80)
            res += chr(value >> 7 & 0xff | 0x80)
        res += chr(value & 0x7F)
        return res


    @classmethod
    def pack_str(cls, value):

        if __debug__:
            if not isinstance(value, basestring):
                raise TypeError("Invalid argument type '%s', 'str' expected"%type(value).__name__)
        value_len_packed = cls.pack_int_base128(len(value))
        return struct.pack("<%ds%ds"%(len(value_len_packed), len(value)), value_len_packed,  value)


    @classmethod
    def pack_field(cls, value):

        if isinstance(value, basestring):
            return cls.pack_str(value)
        elif isinstance(value, (int, long)):
            return cls.pack_int(value)
        else:
            raise TypeError("Invalid argument type '%s', 'str' or 'int' expected"%type(value).__name__)


    @classmethod
    def pack_tuple(cls, values):

        assert isinstance(values, (tuple, list))
        cardinality = struct_L.pack(len(values))
        packed_items = [cls.pack_field(v) for v in values]
        packed_items.insert(0, cardinality)
        return b"".join(packed_items)



class RequestInsert(Request):

    request_type = REQUEST_TYPE_INSERT

    def __init__(self, space_no, values, return_tuple):    # pylint: disable=W0231
        '''\
        '''
        assert isinstance(values, (tuple, list))
        flags = 1 if return_tuple else 0

        request_body = \
            struct_LL.pack(space_no, flags) + \
            self.pack_tuple(values)

        self._bytes = self.header(len(request_body)) + request_body



class RequestDelete(Request):

    request_type = REQUEST_TYPE_DELETE

    def __init__(self, space_no, key, return_tuple):    # pylint: disable=W0231
        '''
        '''
        flags = 1 if return_tuple else 0

        request_body = \
            struct_LL.pack(space_no, flags) + \
            self.pack_tuple((key,))

        self._bytes = self.header(len(request_body)) + request_body



class RequestSelect(Request):

    request_type = REQUEST_TYPE_SELECT

    def __init__(self, space_no, index_no, tuple_list, offset, limit):    # pylint: disable=W0231

        assert isinstance(tuple_list, (list, tuple))

        request_body = \
            struct_LLLLL.pack(space_no, index_no, offset, limit, len(tuple_list)) + \
            b"".join([self.pack_tuple(t) for t in tuple_list])

        self._bytes = self.header(len(request_body)) + request_body



class RequestUpdate(Request):

    request_type = REQUEST_TYPE_UPDATE

    def __init__(self, space_no, key, op_list, return_tuple):    # pylint: disable=W0231
        flags = 1 if return_tuple else 0
        assert isinstance(key, (int, basestring))

        request_body = \
            struct_LL.pack(space_no, flags) + \
            self.pack_tuple((key,)) + \
            struct_L.pack(len(op_list)) +\
            self.pack_operations(op_list)

        self._bytes = self.header(len(request_body)) + request_body


    @classmethod
    def pack_operations(cls, op_list):
        result = []
        for op in op_list:
            try:
                field_no, op_symbol, op_arg = op
            except ValueError:
                raise ValueError("Operation must be a tuple of 3 elements (field_id, op, value)")
            try:
                op_code = UPDATE_OPERATION_CODE[op_symbol]
            except KeyError:
                raise ValueError("Invalid operaction symbol '%s'. Expected one of %s"\
                                %(op_symbol, ' '.join(sorted(UPDATE_OPERATION_CODE.keys()))))
            data = b"".join([struct_LB.pack(field_no, op_code), cls.pack_field(op_arg)])
            result.append(data)
        return b"".join(result)



class RequestCall(Request):

    request_type = REQUEST_TYPE_CALL

    def __init__(self, proc_name, args, return_tuple):    # pylint: disable=W0231
        flags = 1 if return_tuple else 0
        assert isinstance(args, (list, tuple))

        request_body = \
            struct_L.pack(flags) + \
            self.pack_field(proc_name) +\
            self.pack_tuple(args)

        self._bytes = self.header(len(request_body)) + request_body