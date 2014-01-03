# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614

import struct


# pylint: disable=C0103
struct_B = struct.Struct('<B')
struct_BB = struct.Struct('<BB')
struct_BBB = struct.Struct('<BBB')
struct_BBBB = struct.Struct('<BBBB')
struct_BBBBB = struct.Struct('<BBBBB')
struct_BL = struct.Struct("<BL")
struct_LB = struct.Struct("<LB")
struct_L = struct.Struct("<L")
struct_LL = struct.Struct("<LL")
struct_LLL = struct.Struct("<LLL")
struct_LLLL = struct.Struct("<LLLL")
struct_LLLLL = struct.Struct("<LLLLL")
struct_Q = struct.Struct("<Q")
struct_BQ = struct.Struct("<BQ")


REQUEST_TYPE_PING = 0 
REQUEST_TYPE_SELECT = 1
REQUEST_TYPE_INSERT = 2
REQUEST_TYPE_REPLACE = 3
REQUEST_TYPE_UPDATE = 4 
REQUEST_TYPE_DELETE = 5 
REQUEST_TYPE_CALL = 6 

BOX_RETURN_TUPLE = 1
BOX_ADD = 2
BOX_REPLACE = 4


UPDATE_OPERATION_CODE = {'=': 0, '+': 1, '&': 2, '^': 3, '|': 4, 'splice': 5}

# Default value for socket timeout (seconds)
SOCKET_TIMEOUT = None
# Default maximum number of attempts to reconnect
RECONNECT_MAX_ATTEMPTS = 10
# Default delay between attempts to reconnect (seconds)
RECONNECT_DELAY = 0.1
# Number of reattempts in case of server
# return completion_status == 1 (try again)
RETRY_MAX_ATTEMPTS = 10
