# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import struct
import sys
import msgpack

from tarantool.const import (
    REQUEST_TYPE_INSERT,
    REQUEST_TYPE_SELECT,
    REQUEST_TYPE_UPDATE,
    REQUEST_TYPE_DELETE,
    IPROTO_CODE,
    IPROTO_ERROR,
    IPROTO_DATA,
)
from tarantool.error import DatabaseError


if sys.version_info < (2, 6):
    bytes = str    # pylint: disable=W0622

class Response(list):

    '''\
    Represents a single response from the server in compliance with the
    Tarantool protocol.
    Responsible for data encapsulation (i.e. received list of tuples)
    and parses binary packet received from the server.
    '''

    def __init__(
        self, conn, response, space_name=None, field_defs=None,
        default_type=None
    ):
        '''\
        Create an instance of `Response` using data received from the server.

        __init__() itself reads data from the socket, parses response body and
        sets appropriate instance attributes.

        :param body: body of the response
        :type body: array of bytes
        '''

        # This is not necessary, because underlying list data structures are
        # created in the __new__(). But let it be.
        super(Response, self).__init__()

        unpacker = msgpack.Unpacker(use_list = False)
        unpacker.feed(response)

        header = unpacker.unpack()

        # Separate return_code and completion_code
        self._completion_status = header[IPROTO_CODE] & 0x00ff
        self._return_code = header[IPROTO_CODE] >> 8
        self._return_message = None
        self.conn = conn
        if space_name is not None:
            self.space_no = conn.schema.space_no(space_name)
        else:
            self.space_no = None
        self.field_defs = field_defs
        self.default_type = default_type

        body = None
        try:
            body = unpacker.unpack()
        except msgpack.OutOfData:
            return

        if self._return_code != 0:
            self._return_message = body[IPROTO_ERROR]
            if self._completion_status == 2 and self.conn.error:
                raise DatabaseError(self._return_code, self._return_message)
        else:
            self.extend(body[IPROTO_DATA])

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
        return len(self)

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
        return affected + " affected"
