# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import ctypes
import struct
import warnings

from tarantool.const import *


class Response(object):

    def __init__(self, socket):
        self.request_type = None
        self.body_length = None
        self.request_id = None
        self.return_code = None
        self.return_message = None
        self.rowcount = None
        self.rows = []

        # Read response header
        buff = ctypes.create_string_buffer(16)
        socket.recv_into(buff, 16)
        self.request_type, self.body_length, self.request_id, self.return_code = struct_LLLL.unpack(buff)
        if self.body_length != 0:
            self.body_length -= 4 # In the protocol description <body_length> includes 4 bytes of <return_code>
            # Read response body
            buff = ctypes.create_string_buffer(self.body_length)
            socket.recv_into(buff)

            if self.return_code == 0:
                # If no errors, unpack response body
                self.unpack_body(buff)
            else:
                # In case of error unpack body as error message
                self.unpack_message(buff)
                # Check that the low byte is equal to 0x02 ("error").
                # Also It can be 0x01 ("try again").
                # FIXME: Implement support of "try again"
                if (self.return_code & 0x00ff) == 1:
                    raise RuntimeError('Got "try again" indicator')
                raise RuntimeError(self.return_code >> 8, self.return_message)


    def unpack_message(self, buff):

        self.return_message = unicode(buff.value, "utf8", "replace")


    @staticmethod
    def unpack_int_base128(varint, offset):
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


    @classmethod
    def unpack_tuple(cls, buff):

        dummy_int_unpack = True

        cardinality = struct_L.unpack_from(buff)[0]
        _tuple = ['']*cardinality
        offset = 4    # The first 4 bytes in the response body is the <count> we have already read
        for i in xrange(cardinality):
            field_size, offset = cls.unpack_int_base128(buff, offset)
            field_data = struct.unpack_from("<%ds"%field_size, buff, offset)[0]

            if dummy_int_unpack:
                # FIXME: Watch out! 4-byte field is an integer, even if it is a string
                if field_size == 4:
                    warnings.warn("Using dummy int unpack")
                    _tuple[i] = struct_L.unpack(field_data)[0]
                else:
                    _tuple[i] = field_data
            else:
                _tuple[i] = field_data
            offset += field_size

        return tuple(_tuple)


    def unpack_body(self, buff):

        # Unpack <count> (first 4 bytes) - how many records returned
        self.rowcount = struct_L.unpack_from(buff)[0]

        # If the response body contains only <count> - there is no tuples to unpack
        if self.body_length == 4:
            return

        # Parse response tuples (<fq_tuple>)
        if self.rowcount > 0:
            # Access by the index is 1.5-2 times faster than append(), and the list size is already known
            rows = [b'']*self.rowcount
            offset = 4    # The first 4 bytes in the response body is the <count> we have already read
            for i in xrange(self.rowcount):
                '''
                # In resonse tuples have the form <size><tuple> (<fq_tuple> ::= <size><tuple>).
                # Attribute <size> уonly takes into account the size of tuple's <field> payload,
                # but does not include 4-byte of <cardinality> field.
                # Therefore the actual size of the <tuple> is greater to 4 bytes.
                '''
                tuple_size = struct.unpack_from("<L", buff, offset)[0] + 4
                tuple_data = struct.unpack_from("<%ds"%(tuple_size), buff, offset+4)[0]
                rows[i] = self.unpack_tuple(tuple_data)
                offset = offset + tuple_size + 4    # This '4' is a size of <size> attribute
            self.rows = rows


    def __str__(self):
        return str(self.rows)


    def __iter__(self):
        return iter(self.rows)