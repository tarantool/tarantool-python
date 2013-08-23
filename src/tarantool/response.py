# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import struct
import sys

from tarantool.const import (
    REQUEST_TYPE_INSERT,
    REQUEST_TYPE_SELECT,
    REQUEST_TYPE_UPDATE,
    REQUEST_TYPE_DELETE,
    struct_L,
    struct_LL,
    struct_LLL,
    struct_Q
)
from tarantool.error import DatabaseError


if sys.version_info < (2, 6):
    bytes = str    # pylint: disable=W0622


class field(bytes):

    '''\
    Represents a single element of the Tarantool's tuple
    '''
    def __new__(cls, value):
        '''\
        Create new instance of Tarantool field (single tuple element)
        '''
        # Since parent class is immutable, we should override __new__, not
        # __init__

        if isinstance(value, unicode):
            return super(field, cls).__new__(
                cls, value.encode("utf-8", "replace"))

        if sys.version_info[0] < 3 and isinstance(value, str):
            return super(field, cls).__new__(cls, value)

        if isinstance(value, (bytearray, bytes)):
            return super(field, cls).__new__(cls, value)

        if isinstance(value, (int, long)):
            if 0 <= value <= 0xFFFFFFFF:
                # 32 bit integer
                return super(field, cls).__new__(cls, struct_L.pack(value))
            elif 0xFFFFFFFF < value <= 0xFFFFFFFFFFFFFFFF:
                # 64 bit integer
                return super(field, cls).__new__(cls, struct_Q.pack(value))
            else:
                raise ValueError("Integer argument out of range")

        # NOTE: It is posible to implement float
        raise TypeError("Unsupported argument type '%s'" %
                        (type(value).__name__))

    def __int__(self):
        '''\
        Cast filed to int
        '''
        if len(self) == 4:
            return struct_L.unpack(self)[0]
        elif len(self) == 8:
            return struct_Q.unpack(self)[0]
        else:
            raise ValueError(
                "Unable to cast field to int: length must be 4 or 8 bytes, "
                "field length is %d" % len(self))

    if sys.version_info[0] > 2:
        def __str__(self):
            '''\
            Cast filed to str
            '''
            return self.decode("utf-8", "replace")
    else:
        def __unicode__(self):
            '''\
            Cast filed to unicode
            '''
            return self.decode("utf-8", "replace")


class Response(list):

    '''\
    Represents a single response from the server in compliance with the
    Tarantool protocol.
    Responsible for data encapsulation (i.e. received list of tuples)
    and parses binary packet received from the server.
    '''

    def __init__(
        self, conn, header, body, space_name=None, field_defs=None,
        default_type=None
    ):
        '''\
        Create an instance of `Response` using data received from the server.

        __init__() itself reads data from the socket, parses response body and
        sets appropriate instance attributes.

        :param header: header of the response
        :type header: array of bytes
        :param body: body of the response
        :type body: array of bytes
        '''

        # This is not necessary, because underlying list data structures are
        # created in the __new__(). But let it be.
        super(Response, self).__init__()

        self._body_length = None
        self._request_id = None
        self._request_type = None
        self._completion_status = None
        self._return_code = None
        self._return_message = None
        self._rowcount = None
        self.conn = conn
        if space_name is not None:
            self.space_no = conn.schema.space_no(space_name)
        else:
            self.space_no = None
        self.field_defs = field_defs
        self.default_type = default_type

        # Unpack header
        request_type, body_length, request_id = struct_LLL.unpack(header)
        self._request_type = request_type
        self._body_length = body_length
        self._request_id = request_id

        if body:
            self._unpack_body(body)

    @staticmethod
    def _unpack_int_base128(varint, offset):
        """Implement Perl unpack's 'w' option, aka base 128 decoding."""
        res = ord(varint[offset])
        if ord(varint[offset]) >= 0x80:
            offset += 1
            res = ((res - 0x80) << 7) + ord(varint[offset])
            if ord(varint[offset]) >= 0x80:
                offset += 1
                res = ((res - 0x80) << 7) + ord(varint[offset])
                if ord(varint[offset]) >= 0x80:
                    offset += 1
                    res = ((res - 0x80) << 7) + ord(varint[offset])
                    if ord(varint[offset]) >= 0x80:
                        offset += 1
                        res = ((res - 0x80) << 7) + ord(varint[offset])
        return res, offset + 1

    def _unpack_tuple(self, buff):
        '''\
        Unpacks the tuple from byte buffer
        <tuple> ::= <cardinality><field>+

        :param buff: byte array of the form <cardinality><field>+
        :type buff: ctypes buffer or bytes

        :return: tuple of unpacked values
        :rtype: tuple
        '''

        cardinality = struct_L.unpack_from(buff)[0]
        _tuple = [''] * cardinality
        # The first 4 bytes in the response body is the <count> we have already
        # read
        offset = 4
        for i in xrange(cardinality):
            field_size, offset = self._unpack_int_base128(buff, offset)
            field_data = struct.unpack_from(
                "<%ds" % field_size, buff, offset)[0]
            _tuple[i] = field(field_data)
            offset += field_size

        return tuple(_tuple)

    def _unpack_body(self, buff):
        '''\
        Parse the response body.
        After body unpacking its data available as python list of tuples

        For each request type the response body has the same format:
        <insert_response_body> ::= <count> | <count><fq_tuple>
        <update_response_body> ::= <count> | <count><fq_tuple>
        <delete_response_body> ::= <count> | <count><fq_tuple>
        <select_response_body> ::= <count><fq_tuple>*
        <call_response_body>   ::= <count><fq_tuple>

        :param buff: buffer containing request body
        :type byff: ctypes buffer
        '''

        # Unpack <return_code> and <count> (how many records affected or
        # selected)
        self._return_code, self._rowcount = struct_LL.unpack_from(
            buff, offset=0)

        # Separate return_code and completion_code
        self._completion_status = self._return_code & 0x00ff
        self._return_code = self._return_code >> 8

        # In case of an error unpack the body as an error message
        if self._return_code != 0:
            self._return_message = unicode(buff[4:-1], "utf8", "replace")
            if self._completion_status == 2:
                raise DatabaseError(self._return_code, self._return_message)

        # If the response don't contains any tuples - there is no tuples to
        # unpack
        if self._body_length == 8:
            return

        # Parse response tuples (<fq_tuple>)
        if self._rowcount > 0:
            # The first 4 bytes in the response body is the <count> we have
            # already read
            offset = 8
            while offset < self._body_length:
                '''
                # In resonse tuples have the form <size><tuple>
                # (<fq_tuple> ::= <size><tuple>).
                # Attribute <size> takes into account only size of tuple's
                # <field> payload,
                # but does not include 4-byte of <cardinality> field.
                # Therefore the actual size of the <tuple> is greater to
                # 4 bytes.
                '''
                tuple_size = struct.unpack_from("<L", buff, offset)[0] + 4
                tuple_data = struct.unpack_from(
                    "<%ds" % (tuple_size), buff, offset + 4)[0]
                tuple_value = self._unpack_tuple(tuple_data)
                tuple_value = self.conn.schema.unpack_values(
                    tuple_value, self.space_no, self.field_defs,
                    self.default_type)
                self.append(tuple_value)

                # This '4' is a size of <size> attribute
                offset = offset + tuple_size + 4

    @property
    def completion_status(self):
        '''\
        :type: int

        Request completion status.

        There are only three completion status codes in use:

            * ``0`` -- "success"; the only possible :attr:`return_code` with
                       this status is ``0``
            * ``1`` -- "try again"; an indicator of an intermittent error.
                       This status is handled automatically by this module.
            * ``2`` -- "error"; in this case :attr:`return_code` holds
                       the actual error.
        '''
        return self._completion_status

    @property
    def rowcount(self):
        '''\
        :type: int

        Number of rows affected or returned by a query.
        '''
        return self._rowcount

    @property
    def return_code(self):
        '''\
        :type: int

        Required field in the server response.
        Value of :attr:`return_code` can be ``0`` if request was sucessfull
        or contains an error code.
        If :attr:`return_code` is non-zero than :attr:`return_message`
        contains an error message.
        '''
        return self._return_code

    @property
    def return_message(self):
        '''\
        :type: str

        The error message returned by the server in case
        of :attr:`return_code` is non-zero.
        '''
        return self._return_message

    def __repr__(self):
        '''\
        Return user friendy string representation of the object.
        Useful for the interactive sessions and debuging.

        :rtype: str or None
        '''
        # If response is not empty then return default list representation
        # If there was an SELECT request - return list representation even it
        # is empty
        if(self._request_type == REQUEST_TYPE_SELECT or len(self)):
            return super(Response, self).__repr__()

        # Return string of form "N records affected"
        affected = str(self.rowcount) + \
            " record" if self.rowcount == 1 else " records"
        if(self._request_type == REQUEST_TYPE_DELETE):
            return affected + " deleted"
        if(self._request_type == REQUEST_TYPE_INSERT):
            return affected + " inserted"
        if(self._request_type == REQUEST_TYPE_UPDATE):
            return affected + " updated"
        return affected + " affected"
