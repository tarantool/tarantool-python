# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Request types definitions
'''

import msgpack
import hashlib


from tarantool.const import (
    IPROTO_CODE,
    IPROTO_SYNC,
    IPROTO_SPACE_ID,
    IPROTO_INDEX_ID,
    IPROTO_LIMIT,
    IPROTO_OFFSET,
    IPROTO_KEY,
    IPROTO_USER_NAME,
    IPROTO_TUPLE,
    IPROTO_FUNCTION_NAME,
    IPROTO_ITERATOR,
    IPROTO_SERVER_UUID,
    IPROTO_CLUSTER_UUID,
    IPROTO_VCLOCK,
    IPROTO_EXPR,
    IPROTO_OPS,
    # IPROTO_INDEX_BASE,
    IPROTO_SCHEMA_ID,
    REQUEST_TYPE_OK,
    REQUEST_TYPE_PING,
    REQUEST_TYPE_SELECT,
    REQUEST_TYPE_INSERT,
    REQUEST_TYPE_REPLACE,
    REQUEST_TYPE_DELETE,
    REQUEST_TYPE_UPDATE,
    REQUEST_TYPE_UPSERT,
    REQUEST_TYPE_CALL16,
    REQUEST_TYPE_CALL,
    REQUEST_TYPE_EVAL,
    REQUEST_TYPE_AUTHENTICATE,
    REQUEST_TYPE_JOIN,
    REQUEST_TYPE_SUBSCRIBE
)
from tarantool.utils import (
    strxor,
    binary_types
)

class Request(object):
    '''
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
        self._sync = None
        self._body = ''

    def __bytes__(self):
        return self.header(len(self._body)) + self._body

    __str__ = __bytes__

    @property
    def sync(self):
        '''
        :type: int

        Required field in the server request.
        Contains request header IPROTO_SYNC.
        '''
        return self._sync

    def header(self, length):
        self._sync = self.conn.generate_sync()
        header = msgpack.dumps({IPROTO_CODE: self.request_type,
                                IPROTO_SYNC: self._sync,
                                IPROTO_SCHEMA_ID: self.conn.schema_version})

        return msgpack.dumps(length + len(header)) + header


class RequestInsert(Request):
    '''
    Represents INSERT request
    '''
    request_type = REQUEST_TYPE_INSERT

    # pylint: disable=W0231
    def __init__(self, conn, space_no, values):
        '''
        '''
        super(RequestInsert, self).__init__(conn)
        assert isinstance(values, (tuple, list))

        request_body = msgpack.dumps({IPROTO_SPACE_ID: space_no,
                                      IPROTO_TUPLE: values})

        self._body = request_body


class RequestAuthenticate(Request):
    '''
    Represents AUTHENTICATE request
    '''
    request_type = REQUEST_TYPE_AUTHENTICATE

    def __init__(self, conn, salt, user, password):
        super(RequestAuthenticate, self).__init__(conn)

        def sha1(values):
            sha = hashlib.sha1()
            for i in values:
                if i is not None:
                    if isinstance(i, binary_types):
                        sha.update(i)
                    else:
                        sha.update(i.encode())
            return sha.digest()

        hash1 = sha1((password,))
        hash2 = sha1((hash1,))
        scramble = sha1((salt, hash2))
        scramble = strxor(hash1, scramble)
        request_body = msgpack.dumps({IPROTO_USER_NAME: user,
                                      IPROTO_TUPLE: ("chap-sha1", scramble)})
        self._body = request_body

    def header(self, length):
        self._sync = self.conn.generate_sync()
        # Set IPROTO_SCHEMA_ID: 0 to avoid SchemaReloadException
        # It is ok to use 0 in auth every time.
        header = msgpack.dumps({IPROTO_CODE: self.request_type,
                                IPROTO_SYNC: self._sync,
                                IPROTO_SCHEMA_ID: 0})

        return msgpack.dumps(length + len(header)) + header


class RequestReplace(Request):
    '''
    Represents REPLACE request
    '''
    request_type = REQUEST_TYPE_REPLACE

    # pylint: disable=W0231
    def __init__(self, conn, space_no, values):
        '''
        '''
        super(RequestReplace, self).__init__(conn)
        assert isinstance(values, (tuple, list))

        request_body = msgpack.dumps({IPROTO_SPACE_ID: space_no,
                                      IPROTO_TUPLE: values})

        self._body = request_body


class RequestDelete(Request):
    '''
    Represents DELETE request
    '''
    request_type = REQUEST_TYPE_DELETE

    # pylint: disable=W0231
    def __init__(self, conn, space_no, index_no, key):
        '''
        '''
        super(RequestDelete, self).__init__(conn)

        request_body = msgpack.dumps({IPROTO_SPACE_ID: space_no,
                                      IPROTO_INDEX_ID: index_no,
                                      IPROTO_KEY: key})

        self._body = request_body


class RequestSelect(Request):
    '''
    Represents SELECT request
    '''
    request_type = REQUEST_TYPE_SELECT

    # pylint: disable=W0231
    def __init__(self, conn, space_no, index_no, key, offset, limit, iterator):
        super(RequestSelect, self).__init__(conn)
        request_body = msgpack.dumps({IPROTO_SPACE_ID: space_no,
                                      IPROTO_INDEX_ID: index_no,
                                      IPROTO_OFFSET: offset,
                                      IPROTO_LIMIT: limit,
                                      IPROTO_ITERATOR: iterator,
                                      IPROTO_KEY: key})

        self._body = request_body


class RequestUpdate(Request):
    '''
    Represents UPDATE request
    '''

    request_type = REQUEST_TYPE_UPDATE

    # pylint: disable=W0231
    def __init__(self, conn, space_no, index_no, key, op_list):
        super(RequestUpdate, self).__init__(conn)

        request_body = msgpack.dumps({IPROTO_SPACE_ID: space_no,
                                      IPROTO_INDEX_ID: index_no,
                                      IPROTO_KEY: key,
                                      IPROTO_TUPLE: op_list})

        self._body = request_body


class RequestCall(Request):
    '''
    Represents CALL request
    '''
    request_type = REQUEST_TYPE_CALL

    # pylint: disable=W0231
    def __init__(self, conn, name, args, call_16):
        if call_16:
            self.request_type = REQUEST_TYPE_CALL16
        super(RequestCall, self).__init__(conn)
        assert isinstance(args, (list, tuple))

        request_body = msgpack.dumps({IPROTO_FUNCTION_NAME: name,
                                      IPROTO_TUPLE: args})

        self._body = request_body


class RequestEval(Request):
    '''
    Represents EVAL request
    '''
    request_type = REQUEST_TYPE_EVAL

    # pylint: disable=W0231
    def __init__(self, conn, name, args):
        super(RequestEval, self).__init__(conn)
        assert isinstance(args, (list, tuple))

        request_body = msgpack.dumps({IPROTO_EXPR: name,
                                      IPROTO_TUPLE: args})

        self._body = request_body


class RequestPing(Request):
    '''
    Ping body is empty, so body_length == 0 and there's no body
    '''
    request_type = REQUEST_TYPE_PING

    def __init__(self, conn):
        super(RequestPing, self).__init__(conn)
        self._body = b''


class RequestUpsert(Request):
    '''
    Represents UPSERT request
    '''

    request_type = REQUEST_TYPE_UPSERT

    # pylint: disable=W0231
    def __init__(self, conn, space_no, index_no, tuple_value, op_list):
        super(RequestUpsert, self).__init__(conn)

        request_body = msgpack.dumps({IPROTO_SPACE_ID: space_no,
                                      IPROTO_INDEX_ID: index_no,
                                      IPROTO_TUPLE: tuple_value,
                                      IPROTO_OPS: op_list})

        self._body = request_body


class RequestJoin(Request):
    '''
    Represents JOIN request
    '''
    request_type = REQUEST_TYPE_JOIN

    # pylint: disable=W0231
    def __init__(self, conn, server_uuid):
        super(RequestJoin, self).__init__(conn)
        request_body = msgpack.dumps({IPROTO_SERVER_UUID: server_uuid})
        self._body = request_body


class RequestSubscribe(Request):
    '''
    Represents SUBSCRIBE request
    '''
    request_type = REQUEST_TYPE_SUBSCRIBE

    # pylint: disable=W0231
    def __init__(self, conn, cluster_uuid, server_uuid, vclock):
        super(RequestSubscribe, self).__init__(conn)
        assert isinstance(vclock, dict)

        request_body = msgpack.dumps({
            IPROTO_CLUSTER_UUID: cluster_uuid,
            IPROTO_SERVER_UUID: server_uuid,
            IPROTO_VCLOCK: vclock
        })
        self._body = request_body


class RequestOK(Request):
    '''
    Represents OK acknowledgement
    '''
    request_type = REQUEST_TYPE_OK

    # pylint: disable=W0231
    def __init__(self, conn, sync):
        super(RequestOK, self).__init__(conn)
        request_body = msgpack.dumps({IPROTO_CODE: self.request_type,
                                      IPROTO_SYNC: sync})
        self._body = request_body
