# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Request types definitions
'''

import struct
import msgpack

from tarantool.const import (
    IPROTO_CODE,
    IPROTO_SYNC,
    IPROTO_SPACE_ID,
    IPROTO_INDEX_ID,
    IPROTO_LIMIT,
    IPROTO_OFFSET,
    IPROTO_ITERATOR,
    IPROTO_KEY,
    IPROTO_TUPLE,
    IPROTO_FUNCTION_NAME,
    REQUEST_TYPE_PING,
    REQUEST_TYPE_SELECT,
    REQUEST_TYPE_INSERT,
    REQUEST_TYPE_REPLACE,
    REQUEST_TYPE_DELETE,
    REQUEST_TYPE_UPDATE,
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
    def header(cls, length):
        header = msgpack.dumps({ IPROTO_CODE : cls.request_type,
                                IPROTO_SYNC : 0})
        return msgpack.dumps(length + len(header)) + header

class RequestInsert(Request):

    '''\
    Represents INSERT request
    '''
    request_type = REQUEST_TYPE_INSERT

    # pylint: disable=W0231
    def __init__(self, conn, space_name, values):
        '''\
        '''
        super(RequestInsert, self).__init__(conn)
        assert isinstance(values, (tuple, list))

        space_no = self.conn.schema.space_no(space_name)
        request_body = msgpack.dumps({ IPROTO_SPACE_ID: space_no, \
                                       IPROTO_TUPLE: values })

        self._bytes = self.header(len(request_body)) + request_body

class RequestReplace(Request):

    '''\
    Represents REPLACE request
    '''
    request_type = REQUEST_TYPE_REPLACE

    # pylint: disable=W0231
    def __init__(self, conn, space_name, values):
        '''\
        '''
        super(RequestReplace, self).__init__(conn)
        assert isinstance(values, (tuple, list))

        space_no = self.conn.schema.space_no(space_name)
        request_body = msgpack.dumps({ IPROTO_SPACE_ID: space_no, \
                                       IPROTO_TUPLE: values })

        self._bytes = self.header(len(request_body)) + request_body


class RequestDelete(Request):

    '''
    Represents DELETE request
    '''
    request_type = REQUEST_TYPE_DELETE

    # pylint: disable=W0231
    def __init__(self, conn, space_name, key):
        '''
        '''
        super(RequestDelete, self).__init__(conn)

        space_no = self.conn.schema.space_no(space_name)
        request_body = msgpack.dumps({ IPROTO_SPACE_ID: space_no, \
                                       IPROTO_KEY: (key,) })

        self._bytes = self.header(len(request_body)) + request_body


class RequestSelect(Request):

    '''\
    Represents SELECT request
    '''
    request_type = REQUEST_TYPE_SELECT

    # pylint: disable=W0231
    def __init__(self, conn, space_name, index_name, key, offset, limit):
        super(RequestSelect, self).__init__(conn)

        space_no = self.conn.schema.space_no(space_name)
        index_no = self.conn.schema.index_no(space_no, index_name)
        request_body = msgpack.dumps({ IPROTO_SPACE_ID: space_no, \
                                       IPROTO_INDEX_ID: index_no, \
                                       IPROTO_OFFSET: offset, \
                                       IPROTO_LIMIT: limit, \
                                       IPROTO_KEY: (key,) })

        self._bytes = self.header(len(request_body)) + request_body


class RequestUpdate(Request):

    '''\
        Represents UPDATE request
    '''

    request_type = REQUEST_TYPE_UPDATE

    # pylint: disable=W0231
    def __init__(self, conn, space_name, key, op_list):
        super(RequestUpdate, self).__init__(conn)
        assert isinstance(key, (int, long, basestring))

        space_no = self.conn.schema.space_no(space_name)
        request_body = msgpack.dumps({ IPROTO_SPACE_ID: space_no, \
                                       IPROTO_KEY: (key,), \
                                       IPROTO_TUPLE: op_list })

        self._bytes = self.header(len(request_body)) + request_body

class RequestCall(Request):

    '''
        Represents CALL request
    '''
    request_type = REQUEST_TYPE_CALL

    # pylint: disable=W0231
    def __init__(self, conn, name, args):
        super(RequestCall, self).__init__(conn)
        assert isinstance(args, (list, tuple))

        request_body = msgpack.dumps({ IPROTO_FUNCTION_NAME: name, \
                                       IPROTO_TUPLE: args })

        self._bytes = self.header(len(request_body)) + request_body


class RequestPing(Request):

    '''
    Ping body is empty, so body_length == 0 and there's no body
    '''
    request_type = REQUEST_TYPE_PING

    def __init__(self, conn):
        super(RequestPing, self).__init__(conn)
        self._bytes = self.header(0)
