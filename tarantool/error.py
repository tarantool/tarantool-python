# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Python DB API compatible exceptions
http://www.python.org/dev/peps/pep-0249/

The PEP-249 says that database related exceptions must be inherited as follows:

    StandardError
    |__Warning
    |__Error
       |__InterfaceError
       |__DatabaseError
          |__DataError
          |__OperationalError
          |__IntegrityError
          |__InternalError
          |__ProgrammingError
          |__NotSupportedError
'''

import os
import socket
import sys
import warnings


class Error(StandardError):

    '''Base class for error exceptions'''


class DatabaseError(Error):

    '''Error related to the database engine'''


class InterfaceError(Error):

    '''\
    Error related to the database interface rather than the database itself
    '''


# Monkey patch os.strerror for win32
if sys.platform == "win32":
    # Windows Sockets Error Codes (not all, but related on network errors)
    # http://msdn.microsoft.com/en-us/library/windows/desktop/ms740668(v=vs.85).aspx
    _code2str = {
        10004: "Interrupted system call",
        10009: "Bad file descriptor",
        10013: "Permission denied",
        10014: "Bad address",
        10022: "Invalid argument",
        10024: "Too many open files",
        10035: "Resource temporarily unavailable",
        10036: "Operation now in progress",
        10037: "Operation already in progress",
        10038: "Socket operation on nonsocket",
        10039: "Destination address required",
        10040: "Message too long",
        10041: "Protocol wrong type for socket",
        10042: "Bad protocol option",
        10043: "Protocol not supported",
        10044: "Socket type not supported",
        10045: "Operation not supported",
        10046: "Protocol family not supported",
        10047: "Address family not supported by protocol family",
        10048: "Address already in use",
        10049: "Cannot assign requested address",
        10050: "Network is down",
        10051: "Network is unreachable",
        10052: "Network dropped connection on reset",
        10053: "Software caused connection abort",
        10054: "Connection reset by peer",
        10055: "No buffer space available",
        10056: "Socket is already connected",
        10057: "Socket is not connected",
        10058: "Cannot send after transport endpoint shutdown",
        10060: "Connection timed out",
        10061: "Connection refused",
        10062: "Cannot translate name",
        10063: "File name too long",
        10064: "Host is down",
        10065: "No route to host",
        11001: "Host not found",
        11004: "Name or service not known"
    }

    os_strerror_orig = os.strerror

    def os_strerror_patched(code):
        '''\
        Return cross-platform message about socket-related errors

        This function exists because under Windows os.strerror returns
        'Unknown error' on all socket-related errors.
        And socket-related exception contain broken non-ascii encoded messages.
        '''
        message = os_strerror_orig(code)
        if not message.startswith("Unknown"):
            return message
        else:
            return _code2str.get(code, "Unknown error %s" % code)

    os.strerror = os_strerror_patched
    del os_strerror_patched


class SchemaError(DatabaseError):
    def __init__(self, value):
        super(SchemaError, self).__init__(0, value)
        self.value = value

    def __str__(self):
        return str(self.value)


class NetworkError(DatabaseError):

    '''Error related to network'''

    def __init__(self, orig_exception=None, *args):
        self.errno = 0
        if hasattr(orig_exception, 'errno'):
            self.errno = orig_exception.errno
        if orig_exception:
            if isinstance(orig_exception, socket.timeout):
                self.message = "Socket timeout"
                super(NetworkError, self).__init__(0, self.message)
            elif isinstance(orig_exception, socket.error):
                self.message = os.strerror(orig_exception.errno)
                super(NetworkError, self).__init__(
                    orig_exception.errno, self.message)
            else:
                super(NetworkError, self).__init__(orig_exception, *args)


class NetworkWarning(UserWarning):

    '''Warning related to network'''
    pass


class RetryWarning(UserWarning):

    '''\
    Warning is emited in case of server return completion_status == 1
    (try again)
    '''
    pass


# always print this warnings
warnings.filterwarnings("always", category=NetworkWarning)
warnings.filterwarnings("always", category=RetryWarning)


def warn(message, warning_class):
    '''\
    Emit warinig message.
    Just like standard warnings.warn() but don't output full filename.
    '''
    frame = sys._getframe(2)  # pylint: disable=W0212
    module_name = frame.f_globals.get("__name__")
    line_no = frame.f_lineno
    warnings.warn_explicit(message, warning_class, module_name, line_no)

_strerror = {
     0: ("ER_OK", "OK"),
     1: ("ER_ILLEGAL_PARAMS", "Illegal parameters, %s"),
     2: ("ER_MEMORY_ISSUE", "Failed to allocate %u bytes in %s for %s"),
     3: ("ER_TUPLE_FOUND", "Duplicate key exists in unique index %u"),
     4: ("ER_TUPLE_NOT_FOUND", "Tuple doesn't exist in index %u"),
     5: ("ER_UNSUPPORTED", "%s does not support %s"),
     6: ("ER_NONMASTER", "Can't modify data on a replication slave. My \
            master is: %s"),
     7: ("ER_SECONDARY", "Can't modify data upon a request on the \
            secondary port."),
     8: ("ER_INJECTION", "Error injection '%s'"),
     9: ("ER_CREATE_SPACE", "Failed to create space %u: %s"),
    10: ("ER_SPACE_EXISTS", "Space %u already exists"),
    11: ("ER_DROP_SPACE", "Can't drop space %u: %s"),
    12: ("ER_ALTER_SPACE", "Can't modify space %u: %s"),
    13: ("ER_INDEX_TYPE", "Unsupported index type supplied for index %u \
            in space %u"),
    14: ("ER_MODIFY_INDEX", "Can't create or modify index %u in space \
            %u: %s"),
    15: ("ER_LAST_DROP", "Can't drop the primary key in a system space, \
            space id %u"),
    16: ("ER_TUPLE_FORMAT_LIMIT", "Tuple format limit reached: %u"),
    17: ("ER_DROP_PRIMARY_KEY", "Can't drop primary key in space %u \
            while secondary keys exist"),
    18: ("ER_KEY_FIELD_TYPE", "Supplied key type of part %u does not \
            match index part type: expected %s"),
    19: ("ER_EXACT_MATCH", "Invalid key part count in an exact match \
            (expected %u, got %u)"),
    20: ("ER_INVALID_MSGPACK", "Invalid MsgPack - %s"),
    21: ("ER_PROC_RET", "msgpack.encode: can not encode Lua type '%s'"),
    22: ("ER_TUPLE_NOT_ARRAY", "Tuple/Key must be MsgPack array"),
    23: ("ER_FIELD_TYPE", "Tuple field %u type does not match one \
            required by operation: expected %s"),
    24: ("ER_FIELD_TYPE_MISMATCH", "Ambiguous field type in index %u, \
            key part %u. Requested type is %s but the field has \
            previously been defined as %s"),
    25: ("ER_SPLICE", "Field SPLICE error: %s"),
    26: ("ER_ARG_TYPE", "Argument type in operation on field %u does \
            not match field type: expected a %s"),
    27: ("ER_TUPLE_IS_TOO_LONG", "Tuple is too long %u"),
    28: ("ER_UNKNOWN_UPDATE_OP", "Unknown UPDATE operation"),
    29: ("ER_UPDATE_FIELD", "Field %u UPDATE error: %s"),
    30: ("ER_FIBER_STACK", "Can not create a new fiber: recursion \
            limit reached"),
    31: ("ER_KEY_PART_COUNT", "Invalid key part count (expected \
            [0..%u], got %u)"),
    32: ("ER_PROC_LUA", "%s"),
    33: ("ER_NO_SUCH_PROC", "Procedure '%.*s' is not defined"),
    34: ("ER_NO_SUCH_TRIGGER", "Trigger is not found"),
    35: ("ER_NO_SUCH_INDEX", "No index #%u is defined in space %u"),
    36: ("ER_NO_SUCH_SPACE", "Space %u does not exist"),
    37: ("ER_NO_SUCH_FIELD", "Field %u was not found in the tuple"),
    38: ("ER_SPACE_ARITY", "Tuple field count %u does not match space \
            %u arity %u"),
    39: ("ER_INDEX_ARITY", "Tuple field count %u is less than required \
            by a defined index (expected %u)"),
    40: ("ER_WAL_IO", "Failed to write to disk"),
    41: ("ER_MORE_THAN_ONE_TUPLE", "More than one tuple found"),
}

def tnt_strerror(num):
    if num in _strerror:
        return _strerror[num]
    return "UNDEFINED"

