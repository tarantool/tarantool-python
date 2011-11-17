# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import ctypes
import socket
import struct
import warnings

from tarantool.const import *
from tarantool.error import *


class Response(list):
    '''\
    Represents a single response from the server in compliance with the Tarantool protocol.
    Responsible for data encapsulation (i.e. received list of tuples) and parses binary
    packet received from the server.
    '''

    def __init__(self, _socket, smart_int_unpack=True):
        '''\
        Create an instance of `Response` using data received from the server.

        __init__() itself reads data from the socket, parses response body and
        sets appropriate instance attributes.

        :params _socket: socket connected to the server
        :type _socket: instance of socket.socket class (from stdlib)
        :param smart_int_unpack: (default is True) indicates that the field
                                 of 4 bytes or 8 bytes must be extracted
                                 as string if it contains printable characters
        :type smart_int_unpack: bool
        '''

        super(Response, self).__init__()

        self._body_length = None
        self._request_id = None
        self._request_type = None
        self._return_code = None
        self._return_message = None
        self._rowcount = None
        self._smart_int_unpack = smart_int_unpack

        # Read response header
        buff = ctypes.create_string_buffer(16)
        nbytes = _socket.recv_into(buff, 16, )
        # Immediately raises an exception if the data cannot be read
        if nbytes != 16:
            raise socket.error(socket.errno.ECONNABORTED, "Software caused connection abort")
        # Unpack header (including <return_code> attribute)
        self._request_type, self._body_length, self._request_id, self._return_code = struct_LLLL.unpack(buff)
        if self._body_length != 0:
            self._body_length -= 4 # In the protocol description <body_length> includes 4 bytes of <return_code>
            # Read response body
            buff = ctypes.create_string_buffer(self._body_length)
            nbytes = _socket.recv_into(buff)
            # Immediately raises an exception if the data cannot be read
            if nbytes != self._body_length:
                raise socket.error(socket.errno.ECONNABORTED, "Software caused connection abort")
            if self._return_code == 0:
                # If no errors, unpack response body
                self._unpack_body(buff)
            else:
                # In case of error unpack body as error message
                self._unpack_message(buff)
                # Check that the low byte is equal to 0x02 ("error").
                # Also It can be 0x01 ("try again").
                # FIXME: Implement support of "try again"
                if (self._return_code & 0x00ff) == 1:
                    raise RuntimeError('Got "try again" indicator')
                raise RuntimeError(self._return_code >> 8, self._return_message)


    def _unpack_message(self, buff):
        '''\
        Extract error message from response body
        Called when return_code! = 0.

        :param buff: buffer containing request body
        :type byff: ctypes buffer
        :return: error message
        :rtype:  str
        '''

        self._return_message = unicode(buff.value, "utf8", "replace")


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


    @staticmethod
    def is_printable(s):
        '''\
        Check if argument is printable (i.e. looks like a string and not a number)
        '''
        assert isinstance(s, bytes)

        if 32 <= ord(s[0]) < 127:
            # ascii
            return True
        elif (ord(s[0]) >> 5 == 0b110) and (ord(s[1]) >> 6 == 0b10) \
             and (ord(s[2]) >> 5 == 0b110) and (ord(s[3]) >> 6 == 0b10):
            # utf8 (110xxxxx 10xxxxxx)
            return True
        else:
            return False


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
        _tuple = ['']*cardinality
        offset = 4    # The first 4 bytes in the response body is the <count> we have already read
        for i in xrange(cardinality):
            field_size, offset = self._unpack_int_base128(buff, offset)
            field_data = struct.unpack_from("<%ds"%field_size, buff, offset)[0]

            if self._smart_int_unpack:
                # FIXME: Watch out! 4-byte field is an integer, even if it is a string
                if field_size == 4 and not self.is_printable(field_data):
                    warnings.warn("Using dummy int unpack")
                    _tuple[i] = struct_L.unpack(field_data)[0]
                elif field_size == 8 and not self.is_printable(field_data):
                    warnings.warn("Using dummy int unpack")
                    _tuple[i] = struct_LL.unpack(field_data)[0]
                else:
                    _tuple[i] = field_data
            else:
                _tuple[i] = field_data
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

        # Unpack <count> (first 4 bytes) - how many records returned
        self._rowcount = struct_L.unpack_from(buff)[0]

        # If the response body contains only <count> - there is no tuples to unpack
        if self._body_length == 4:
            return

        # Parse response tuples (<fq_tuple>)
        if self._rowcount > 0:
            offset = 4    # The first 4 bytes in the response body is the <count> we have already read
            for i in xrange(self._rowcount):
                '''
                # In resonse tuples have the form <size><tuple> (<fq_tuple> ::= <size><tuple>).
                # Attribute <size> takes into account only size of tuple's <field> payload,
                # but does not include 4-byte of <cardinality> field.
                # Therefore the actual size of the <tuple> is greater to 4 bytes.
                '''
                tuple_size = struct.unpack_from("<L", buff, offset)[0] + 4
                tuple_data = struct.unpack_from("<%ds"%(tuple_size), buff, offset+4)[0]
                self.append(self._unpack_tuple(tuple_data))
                offset = offset + tuple_size + 4    # This '4' is a size of <size> attribute

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def return_code(self):
        return self._return_code

    @property
    def return_message(self):
        return self._return_message
