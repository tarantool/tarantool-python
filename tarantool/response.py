# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import sys
import msgpack
import yaml

from tarantool.const import (
    IPROTO_CODE,
    IPROTO_DATA,
    IPROTO_ERROR,
    IPROTO_SYNC,
    REQUEST_TYPE_OK,
    REQUEST_TYPE_ERROR
)
from tarantool.error import DatabaseError, tnt_strerror

if sys.version_info < (2, 6):
    bytes = str    # pylint: disable=W0622

class Response(list):

    '''\
    Represents a single response from the server in compliance with the
    Tarantool protocol.
    Responsible for data encapsulation (i.e. received list of tuples)
    and parses binary packet received from the server.
    '''

    def __init__(self, conn, response):
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

        unpacker = msgpack.Unpacker(use_list = True)
        unpacker.feed(response)
        header = unpacker.unpack()

        self._sync = header.get(IPROTO_SYNC, 0)
        self.conn = conn
        self._code = header[IPROTO_CODE]
        self._body = {}
        try:
            self._body = unpacker.unpack()
        except msgpack.OutOfData:
            pass

        if self._code < REQUEST_TYPE_ERROR:
            self._return_code = 0;
            self._completion_status = 0
            self._data = self._body.get(IPROTO_DATA, None)
            # Backward-compatibility
            if isinstance(self._data, (list, tuple)):
                self.extend(self._data)
            else:
                self.append(self._data)
        else:
            # Separate return_code and completion_code
            self._return_message = self._body.get(IPROTO_ERROR, "")
            self._return_code = self._code & (REQUEST_TYPE_ERROR - 1)
            self._completion_status = 2
            self._data = None
            if self.conn.error:
                raise DatabaseError(self._return_code, self._return_message)

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
    def body(self):
        '''\
        :type: dict

        Required field in the server response.
        Contains raw response body.
        '''
        return self._body

    @property
    def code(self):
        '''\
        :type: int

        Required field in the server response.
        Contains response type id.
        '''
        return self._code

    @property
    def sync(self):
        '''\
        :type: int

        Required field in the server response.
        Contains response header IPROTO_SYNC.
        '''
        return self._sync

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
    def data(self):
        '''\
        :type: object

        Required field in the server response.
        Contains list of tuples of SELECT, REPLACE and DELETE requests
        and arbitrary data for CALL.
        '''
        return self._data

    @property
    def strerror(self):
        '''\
        :type: str

        It may be ER_OK if request was successful, or contain error code string.
        '''
        return tnt_strerror(self._return_code)

    @property
    def return_message(self):
        '''\
        :type: str

        The error message returned by the server in case
        of :attr:`return_code` is non-zero.
        '''
        return self._return_message

    def __str__(self):
        '''\
        Return user friendy string representation of the object.
        Useful for the interactive sessions and debuging.

        :rtype: str or None
        '''
        if self.completion_status:
            return yaml.dump({ 'error': {
                'code': self.strerror[0],
                'reason': self.return_message
            }})
        return yaml.dump(self._data)

    __repr__ = __str__
