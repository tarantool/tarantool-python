# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

try:
    # Python 3.3+
    from collections.abc import Sequence
except ImportError:
    # Python 2
    from collections import Sequence

import json
import msgpack

from tarantool.const import (
    IPROTO_CODE,
    IPROTO_DATA,
    IPROTO_ERROR,
    IPROTO_SYNC,
    IPROTO_SCHEMA_ID,
    REQUEST_TYPE_ERROR
)
from tarantool.error import (
    DatabaseError,
    InterfaceError,
    SchemaReloadException,
    tnt_strerror
)


class Response(Sequence):
    '''
    Represents a single response from the server in compliance with the
    Tarantool protocol.
    Responsible for data encapsulation (i.e. received list of tuples)
    and parses binary packet received from the server.
    '''

    def __init__(self, conn, response):
        '''
        Create an instance of `Response` using data received from the server.

        __init__() itself reads data from the socket, parses response body and
        sets appropriate instance attributes.

        :param body: body of the response
        :type body: array of bytes
        '''

        # This is not necessary, because underlying list data structures are
        # created in the __new__().
        # super(Response, self).__init__()

        if msgpack.version >= (0, 5, 2) and conn.encoding == 'utf-8':
            # Get rid of the following warning.
            # > PendingDeprecationWarning: encoding is deprecated,
            # > Use raw=False instead.
            unpacker = msgpack.Unpacker(use_list=True, raw=False)
        elif conn.encoding is not None:
            unpacker = msgpack.Unpacker(use_list=True, encoding=conn.encoding)
        else:
            unpacker = msgpack.Unpacker(use_list=True)

        unpacker.feed(response)
        header = unpacker.unpack()

        self.conn = conn
        self._sync = header.get(IPROTO_SYNC, 0)
        self._code = header[IPROTO_CODE]
        self._body = {}
        self._schema_version = header.get(IPROTO_SCHEMA_ID, None)
        try:
            self._body = unpacker.unpack()
        except msgpack.OutOfData:
            pass

        if self._code < REQUEST_TYPE_ERROR:
            self._return_code = 0
            self._schema_version = header.get(IPROTO_SCHEMA_ID, None)
            self._data = self._body.get(IPROTO_DATA, None)
            if (not isinstance(self._data, (list, tuple)) and
                    self._data is not None):
                self._data = [self._data]
            # # Backward-compatibility
            # if isinstance(self._data, (list, tuple)):
            #     self.extend(self._data)
            # else:
            #     self.append(self._data)
        else:
            # Separate return_code and completion_code
            self._return_message = self._body.get(IPROTO_ERROR, "")
            self._return_code = self._code & (REQUEST_TYPE_ERROR - 1)
            self._data = []
            if self._return_code == 109:
                raise SchemaReloadException(self._return_message,
                                            self._schema_version)
            if self.conn.error:
                raise DatabaseError(self._return_code, self._return_message)

    def __getitem__(self, idx):
        if self._data is None:
            raise InterfaceError("Trying to access data, when there's no data")
        return self._data.__getitem__(idx)

    def __len__(self):
        if self._data is None:
            raise InterfaceError("Trying to access data, when there's no data")
        return len(self._data)

    def __contains__(self, item):
        if self._data is None:
            raise InterfaceError("Trying to access data, when there's no data")
        return item in self._data

    def __iter__(self):
        if self._data is None:
            raise InterfaceError("Trying to access data, when there's no data")
        return iter(self._data)

    def __reversed__(self):
        if self._data is None:
            raise InterfaceError("Trying to access data, when there's no data")
        return reversed(self._data)

    def index(self, *args):
        if self._data is None:
            raise InterfaceError("Trying to access data, when there's no data")
        return self._data.index(*args)

    def count(self, item):
        if self._data is None:
            raise InterfaceError("Trying to access data, when there's no data")
        return self._data.count(item)

    @property
    def rowcount(self):
        '''
        :type: int

        Number of rows affected or returned by a query.
        '''
        return len(self)

    @property
    def body(self):
        '''
        :type: dict

        Required field in the server response.
        Contains raw response body.
        '''
        return self._body

    @property
    def code(self):
        '''
        :type: int

        Required field in the server response.
        Contains response type id.
        '''
        return self._code

    @property
    def sync(self):
        '''
        :type: int

        Required field in the server response.
        Contains response header IPROTO_SYNC.
        '''
        return self._sync

    @property
    def return_code(self):
        '''
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
        '''
        :type: object

        Required field in the server response.
        Contains list of tuples of SELECT, REPLACE and DELETE requests
        and arbitrary data for CALL.
        '''
        return self._data

    @property
    def strerror(self):
        '''
        :type: str

        It may be ER_OK if request was successful,
        or contain error code string.
        '''
        return tnt_strerror(self._return_code)

    @property
    def return_message(self):
        '''
        :type: str

        The error message returned by the server in case
        of :attr:`return_code` is non-zero.
        '''
        return self._return_message

    @property
    def schema_version(self):
        '''
        :type: int

        Current schema version of request.
        '''
        return self._schema_version

    def __str__(self):
        '''
        Return user friendy string representation of the object.
        Useful for the interactive sessions and debuging.

        :rtype: str or None
        '''
        if self.return_code:
            return json.dumps({
                'error': {
                    'code': self.strerror[0],
                    'reason': self.return_message
                }
            }, sort_keys = True, indent = 4)
        output = []
        for tpl in self._data or ():
            output.extend(("- ", repr(tpl), "\n"))
        if len(output) > 0:
            output.pop()
        return ''.join(output)

    __repr__ = __str__
