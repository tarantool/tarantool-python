# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

IPROTO_CODE = 0x00
IPROTO_SYNC = 0x01
IPROTO_SPACE_ID = 0x10
IPROTO_INDEX_ID = 0x11
IPROTO_LIMIT = 0x12
IPROTO_OFFSET = 0x13
IPROTO_ITERATOR = 0x14
IPROTO_KEY = 0x20
IPROTO_TUPLE = 0x21
IPROTO_FUNCTION_NAME = 0x22
IPROTO_DATA = 0x30
IPROTO_ERROR = 0x31

REQUEST_TYPE_PING = 0 
REQUEST_TYPE_SELECT = 1
REQUEST_TYPE_INSERT = 2
REQUEST_TYPE_REPLACE = 3
REQUEST_TYPE_UPDATE = 4 
REQUEST_TYPE_DELETE = 5 
REQUEST_TYPE_CALL = 6 

# Default value for socket timeout (seconds)
SOCKET_TIMEOUT = None
# Default maximum number of attempts to reconnect
RECONNECT_MAX_ATTEMPTS = 10
# Default delay between attempts to reconnect (seconds)
RECONNECT_DELAY = 0.1
# Number of reattempts in case of server
# return completion_status == 1 (try again)
RETRY_MAX_ATTEMPTS = 10
