# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

IPROTO_CODE = 0x00
IPROTO_SYNC = 0x01
# replication keys (header)
IPROTO_SERVER_ID = 0x02
IPROTO_LSN = 0x03
IPROTO_TIMESTAMP = 0x04
IPROTO_SCHEMA_ID = 0X05
#
IPROTO_SPACE_ID = 0x10
IPROTO_INDEX_ID = 0x11
IPROTO_LIMIT = 0x12
IPROTO_OFFSET = 0x13
IPROTO_ITERATOR = 0x14
IPROTO_INDEX_BASE = 0x15
#
IPROTO_KEY = 0x20
IPROTO_TUPLE = 0x21
IPROTO_FUNCTION_NAME = 0x22
IPROTO_USER_NAME = 0x23
#
IPROTO_SERVER_UUID = 0x24
IPROTO_CLUSTER_UUID = 0x25
IPROTO_VCLOCK = 0x26
IPROTO_EXPR = 0x27
IPROTO_OPS = 0x28
#
IPROTO_DATA = 0x30
IPROTO_ERROR = 0x31

IPROTO_GREETING_SIZE = 128
IPROTO_BODY_MAX_LEN = 2147483648

REQUEST_TYPE_OK = 0
REQUEST_TYPE_SELECT = 1
REQUEST_TYPE_INSERT = 2
REQUEST_TYPE_REPLACE = 3
REQUEST_TYPE_UPDATE = 4
REQUEST_TYPE_DELETE = 5
REQUEST_TYPE_CALL16 = 6
REQUEST_TYPE_AUTHENTICATE = 7
REQUEST_TYPE_EVAL = 8
REQUEST_TYPE_UPSERT = 9
REQUEST_TYPE_CALL = 10
REQUEST_TYPE_PING = 64
REQUEST_TYPE_JOIN = 65
REQUEST_TYPE_SUBSCRIBE = 66
REQUEST_TYPE_ERROR = 1 << 15

SPACE_SCHEMA = 272
SPACE_SPACE = 280
SPACE_INDEX = 288
SPACE_FUNC = 296
SPACE_VSPACE = 281
SPACE_VINDEX = 289
SPACE_VFUNC = 297
SPACE_USER = 304
SPACE_PRIV = 312
SPACE_CLUSTER = 320

INDEX_SPACE_PRIMARY = 0
INDEX_SPACE_NAME = 2
INDEX_INDEX_PRIMARY = 0
INDEX_INDEX_NAME = 2

ITERATOR_EQ = 0
ITERATOR_REQ = 1
ITERATOR_ALL = 2
ITERATOR_LT = 3
ITERATOR_LE = 4
ITERATOR_GE = 5
ITERATOR_GT = 6
ITERATOR_BITSET_ALL_SET = 7
ITERATOR_BITSET_ANY_SET = 8
ITERATOR_BITSET_ALL_NOT_SET = 9
ITERATOR_OVERLAPS = 10
ITERATOR_NEIGHBOR = 11

# Default value for connection timeout (seconds)
CONNECTION_TIMEOUT = None
# Default value for socket timeout (seconds)
SOCKET_TIMEOUT = None
# Default maximum number of attempts to reconnect
RECONNECT_MAX_ATTEMPTS = 10
# Default delay between attempts to reconnect (seconds)
RECONNECT_DELAY = 0.1
# Default cluster nodes list refresh interval (seconds)
CLUSTER_DISCOVERY_DELAY = 60
