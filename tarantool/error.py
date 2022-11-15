# pylint: disable=C0301,W0105,W0401,W0614
"""
Python DB API compatible exceptions, see `PEP-249`_.

.. _PEP-249: http://www.python.org/dev/peps/pep-0249/
"""

import os
import socket
try:
    import ssl
    is_ssl_supported = True
except ImportError:
    is_ssl_supported = False
import sys
import warnings



class Warning(Exception):
    """
    Exception raised for important warnings
    like data truncations while inserting, etc.
    """

class Error(Exception):
    """
    Base class for error exceptions.
    """


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database
    interface rather than the database itself.
    """


class DatabaseError(Error):
    """
    Exception raised for errors that are related to the database.
    """

    def __init__(self, *args, extra_info=None):
        """
        :param args: ``(code, message)`` or ``(message,)``.
        :type args: :obj:`tuple`

        :param extra_info: Additional `box.error`_ information
            with backtrace.
        :type extra_info: :class:`~tarantool.types.BoxError` or
            :obj:`None`, optional

        .. _box.error: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_error/error/
        """

        super().__init__(*args)

        if (len(args) == 2) and isinstance(args[0], int) and isinstance(args[1], (str, bytes)):
            self.code = args[0]
            self.message = args[1]
        elif (len(args) == 1) and isinstance(args[0], (str, bytes)):
            self.code = 0
            self.message = args[0]
        else:
            self.code = 0
            self.message = ''

        self.extra_info = extra_info


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc.
    """


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc.
    """


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is
    affected, e.g. a foreign key check fails.
    """


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of
    sync, etc.
    """


class ProgrammingError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of
    sync, etc.
    """


class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which is
    not supported by the database, e.g. requesting a .rollback() on a
    connection that does not support transactions or has transactions
    turned off.
    """


class ConfigurationError(Error):
    """
    Error of initialization with a user-provided configuration.
    """

class MsgpackError(Error):
    """
    Error with encoding or decoding of `MP_EXT`_ types.

    .. _MP_EXT: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/
    """

class MsgpackWarning(UserWarning):
    """
    Warning with encoding or decoding of `MP_EXT`_ types.
    """

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
        """
        Return cross-platform message about socket-related errors

        This function exists because under Windows os.strerror returns
        'Unknown error' on all socket-related errors.
        And socket-related exception contain broken non-ascii encoded messages.
        """
        message = os_strerror_orig(code)
        if not message.startswith("Unknown"):
            return message
        else:
            return _code2str.get(code, "Unknown error %s" % code)

    os.strerror = os_strerror_patched
    del os_strerror_patched


class SchemaError(DatabaseError):
    """
    Error related to extracting space and index schema.
    """

    def __init__(self, value):
        """
        :param value: Error value.
        """

        super(SchemaError, self).__init__(0, value)
        self.value = value

    def __str__(self):
        return str(self.value)


class SchemaReloadException(DatabaseError):
    """
    Error related to outdated space and index schema.
    """

    def __init__(self, message, schema_version):
        """
        :param message: Error message.
        :type message: :obj:`str`

        :param schema_version: Response schema version.
        :type schema_version: :obj:`int`
        """

        super(SchemaReloadException, self).__init__(109, message)
        self.schema_version = schema_version

    def __str__(self):
        return str(self.message)


class NetworkError(DatabaseError):
    """
    Error related to network.
    """

    def __init__(self, orig_exception=None, *args, **kwargs):
        """
        :param orig_exception: Exception to wrap.
        :type orig_exception: optional

        :param args: Wrapped exception arguments.
        :type args: :obj:`tuple`
        """

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
                super(NetworkError, self).__init__(orig_exception, *args, **kwargs)


class NetworkWarning(UserWarning):
    """
    Warning related to network.
    """
    pass

class SslError(DatabaseError):
    """
    Error related to SSL.
    """

    def __init__(self, orig_exception=None, *args):
        """
        :param orig_exception: Exception to wrap.
        :type orig_exception: optional

        :param args: Wrapped exception arguments.
        :type args: :obj:`tuple`
        """

        self.errno = 0
        if hasattr(orig_exception, 'errno'):
            self.errno = orig_exception.errno
        if orig_exception:
            if is_ssl_supported and isinstance(orig_exception, ssl.SSLError):
                super(SslError, self).__init__(orig_exception, *args)
            else:
                super(SslError, self).__init__(orig_exception, *args)


class ClusterDiscoveryWarning(UserWarning):
    """
    Warning related to cluster discovery.
    """
    pass


class ClusterConnectWarning(UserWarning):
    """
    Warning related to cluster pool connection.
    """
    pass


class PoolTolopogyWarning(UserWarning):
    """
    Warning related to unsatisfying `box.info.ro`_ state of
    pool instances.
    """
    pass


class PoolTolopogyError(DatabaseError):
    """
    Exception raised due to unsatisfying `box.info.ro`_ state of
    pool instances.

    .. _box.info.ro: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_info/
    """
    pass


class CrudModuleError(DatabaseError):
    """
    Exception raised for errors that are related to
    the operation result of `crud`_ module.

    .. _crud: https://github.com/tarantool/crud
    """

    def __init__(self, _, error):
        """
        Sets fields with result and errors.
        
        :param args: The tuple from the crud module with result and errors.
        :type args: :obj:`tuple`
        """

        super(CrudModuleError, self).__init__(0, error.err)
        # Sets tarantool.crud.CrudError object.
        self.extra_info_error = error


class CrudModuleManyError(DatabaseError):
    """
    Exception raised for errors that are related to
    the batching operation result of `crud`_ module.

    .. _crud: https://github.com/tarantool/crud
    """

    def __init__(self, success, error):
        """
        Sets fields with result and errors.
        
        :param args: The tuple from the crud module with result and errors.
        :type args: :obj:`tuple`
        """

        exc_msg = "Got multiple errors, see errors_list and success_list"
        super(CrudModuleManyError, self).__init__(0, exc_msg)
        # Sets list of tarantool.crud.CrudResult objects.
        self.success_list = success
        # Sets list of tarantool.crud.CrudError objects.
        self.errors_list = error


# always print this warnings
warnings.filterwarnings("always", category=NetworkWarning)


def warn(message, warning_class):
    """
    Emit a warning message.
    Just like standard warnings.warn() but don't output full filename.

    :param message: Warning message.
    :type message: :obj:`str`

    :param warning_class: Warning class.
    :type warning_class: :class:`~tarantool.error.Warning`
    """

    frame = sys._getframe(2)  # pylint: disable=W0212
    module_name = frame.f_globals.get("__name__")
    line_no = frame.f_lineno
    warnings.warn_explicit(message, warning_class, module_name, line_no)


_strerror = {
    0: ("ER_UNKNOWN", "Unknown error"),
    1: ("ER_ILLEGAL_PARAMS", "Illegal parameters, %s"),
    2: ("ER_MEMORY_ISSUE", "Failed to allocate %u bytes in %s for %s"),
    3: ("ER_TUPLE_FOUND",
        "Duplicate key exists in unique index '%s' in space '%s'"),
    4: ("ER_TUPLE_NOT_FOUND",
        "Tuple doesn't exist in index '%s' in space '%s'"),
    5: ("ER_UNSUPPORTED", "%s does not support %s"),
    6: ("ER_NONMASTER",
        "Can't modify data on a replication slave. My master is: %s"),
    7: ("ER_READONLY",
        "Can't modify data because this server is in read-only mode."),
    8: ("ER_INJECTION", "Error injection '%s'"),
    9: ("ER_CREATE_SPACE", "Failed to create space '%s': %s"),
    10: ("ER_SPACE_EXISTS", "Space '%s' already exists"),
    11: ("ER_DROP_SPACE", "Can't drop space '%s': %s"),
    12: ("ER_ALTER_SPACE", "Can't modify space '%s': %s"),
    13: ("ER_INDEX_TYPE",
         "Unsupported index type supplied for index '%s' in space '%s'"),
    14: ("ER_MODIFY_INDEX",
         "Can't create or modify index '%s' in space '%s': %s"),
    15: ("ER_LAST_DROP",
         "Can't drop the primary key in a system space, space '%s'"),
    16: ("ER_TUPLE_FORMAT_LIMIT", "Tuple format limit reached: %u"),
    17: ("ER_DROP_PRIMARY_KEY",
         "Can't drop primary key in space '%s' while secondary keys exist"),
    18: ("ER_KEY_PART_TYPE", (
         "Supplied key type of part %u does not match index part type:"
         " expected %s")),
    19: ("ER_EXACT_MATCH",
         "Invalid key part count in an exact match (expected %u, got %u)"),
    20: ("ER_INVALID_MSGPACK", "Invalid MsgPack - %s"),
    21: ("ER_PROC_RET", "msgpack.encode: can not encode Lua type '%s'"),
    22: ("ER_TUPLE_NOT_ARRAY", "Tuple/Key must be MsgPack array"),
    23: ("ER_FIELD_TYPE", (
         "Tuple field %u type does not match one required by operation:"
         " expected %s")),
    24: ("ER_FIELD_TYPE_MISMATCH", (
         "Ambiguous field type in index '%s', key part %u. Requested type"
         " is %s but the field has previously been defined as %s")),
    25: ("ER_SPLICE", "SPLICE error on field %u: %s"),
    26: ("ER_ARG_TYPE", (
         "Argument type in operation '%c' on field %u does not match"
         " field type: expected a %s")),
    27: ("ER_TUPLE_IS_TOO_LONG", "Tuple is too long %u"),
    28: ("ER_UNKNOWN_UPDATE_OP", "Unknown UPDATE operation"),
    29: ("ER_UPDATE_FIELD", "Field %u UPDATE error: %s"),
    30: ("ER_FIBER_STACK",
         "Can not create a new fiber: recursion limit reached"),
    31: ("ER_KEY_PART_COUNT",
         "Invalid key part count (expected [0..%u], got %u)"),
    32: ("ER_PROC_LUA", "%s"),
    33: ("ER_NO_SUCH_PROC", "Procedure '%.*s' is not defined"),
    34: ("ER_NO_SUCH_TRIGGER", "Trigger is not found"),
    35: ("ER_NO_SUCH_INDEX", "No index #%u is defined in space '%s'"),
    36: ("ER_NO_SUCH_SPACE", "Space '%s' does not exist"),
    37: ("ER_NO_SUCH_FIELD", "Field %d was not found in the tuple"),
    38: ("ER_SPACE_FIELD_COUNT",
         "Tuple field count %u does not match space '%s' field count %u"),
    39: ("ER_INDEX_FIELD_COUNT", (
         "Tuple field count %u is less than required by a defined index"
         " (expected %u)")),
    40: ("ER_WAL_IO", "Failed to write to disk"),
    41: ("ER_MORE_THAN_ONE_TUPLE", "More than one tuple found by get()"),
    42: ("ER_ACCESS_DENIED", "%s access on %s is denied for user '%s'"),
    43: ("ER_CREATE_USER", "Failed to create user '%s': %s"),
    44: ("ER_DROP_USER", "Failed to drop user or role '%s': %s"),
    45: ("ER_NO_SUCH_USER", "User '%s' is not found"),
    46: ("ER_USER_EXISTS", "User '%s' already exists"),
    47: ("ER_PASSWORD_MISMATCH", "Incorrect password supplied for user '%s'"),
    48: ("ER_UNKNOWN_REQUEST_TYPE", "Unknown request type %u"),
    49: ("ER_UNKNOWN_SCHEMA_OBJECT", "Unknown object type '%s'"),
    50: ("ER_CREATE_FUNCTION", "Failed to create function '%s': %s"),
    51: ("ER_NO_SUCH_FUNCTION", "Function '%s' does not exist"),
    52: ("ER_FUNCTION_EXISTS", "Function '%s' already exists"),
    53: ("ER_FUNCTION_ACCESS_DENIED",
         "%s access is denied for user '%s' to function '%s'"),
    54: ("ER_FUNCTION_MAX",
         "A limit on the total number of functions has been reached: %u"),
    55: ("ER_SPACE_ACCESS_DENIED",
         "%s access is denied for user '%s' to space '%s'"),
    56: ("ER_USER_MAX",
         "A limit on the total number of users has been reached: %u"),
    57: ("ER_NO_SUCH_ENGINE", "Space engine '%s' does not exist"),
    58: ("ER_RELOAD_CFG", "Can't set option '%s' dynamically"),
    59: ("ER_CFG", "Incorrect value for option '%s': %s"),
    60: ("ER_SOPHIA", "%s"),
    61: ("ER_LOCAL_SERVER_IS_NOT_ACTIVE", "Local server is not active"),
    62: ("ER_UNKNOWN_SERVER", "Server %s is not registered with the cluster"),
    63: ("ER_CLUSTER_ID_MISMATCH", (
         "Cluster id of the replica %s doesn't match cluster id"
         " of the master %s")),
    64: ("ER_INVALID_UUID", "Invalid UUID: %s"),
    65: ("ER_CLUSTER_ID_IS_RO",
         "Can't reset cluster id: it is already assigned"),
    66: ("ER_RESERVED66", "Reserved66"),
    67: ("ER_SERVER_ID_IS_RESERVED",
         "Can't initialize server id with a reserved value %u"),
    68: ("ER_INVALID_ORDER", (
         "Invalid LSN order for server %u: previous LSN = %llu,"
         " new lsn = %llu")),
    69: ("ER_MISSING_REQUEST_FIELD",
         "Missing mandatory field '%s' in request"),
    70: ("ER_IDENTIFIER", (
         "Invalid identifier '%s' (expected letters, digits"
         " or an underscore)")),
    71: ("ER_DROP_FUNCTION", "Can't drop function %u: %s"),
    72: ("ER_ITERATOR_TYPE", "Unknown iterator type '%s'"),
    73: ("ER_REPLICA_MAX", "Replica count limit reached: %u"),
    74: ("ER_INVALID_XLOG", "Failed to read xlog: %lld"),
    75: ("ER_INVALID_XLOG_NAME", "Invalid xlog name: expected %lld got %lld"),
    76: ("ER_INVALID_XLOG_ORDER", "Invalid xlog order: %lld and %lld"),
    77: ("ER_NO_CONNECTION", "Connection is not established"),
    78: ("ER_TIMEOUT", "Timeout exceeded"),
    79: ("ER_ACTIVE_TRANSACTION",
         "Operation is not permitted when there is an active transaction "),
    80: ("ER_NO_ACTIVE_TRANSACTION",
         "Operation is not permitted when there is no active transaction "),
    81: ("ER_CROSS_ENGINE_TRANSACTION",
         "A multi-statement transaction can not use multiple storage engines"),
    82: ("ER_NO_SUCH_ROLE", "Role '%s' is not found"),
    83: ("ER_ROLE_EXISTS", "Role '%s' already exists"),
    84: ("ER_CREATE_ROLE", "Failed to create role '%s': %s"),
    85: ("ER_INDEX_EXISTS", "Index '%s' already exists"),
    86: ("ER_TUPLE_REF_OVERFLOW", "Tuple reference counter overflow"),
    87: ("ER_ROLE_LOOP",
         "Granting role '%s' to role '%s' would create a loop"),
    88: ("ER_GRANT", "Incorrect grant arguments: %s"),
    89: ("ER_PRIV_GRANTED", "User '%s' already has %s access on %s '%s'"),
    90: ("ER_ROLE_GRANTED", "User '%s' already has role '%s'"),
    91: ("ER_PRIV_NOT_GRANTED",
         "User '%s' does not have %s access on %s '%s'"),
    92: ("ER_ROLE_NOT_GRANTED", "User '%s' does not have role '%s'"),
    93: ("ER_MISSING_SNAPSHOT", "Can't find snapshot"),
    94: ("ER_CANT_UPDATE_PRIMARY_KEY", (
         "Attempt to modify a tuple field which is part of index '%s'"
         " in space '%s'")),
    95: ("ER_UPDATE_INTEGER_OVERFLOW",
         "Integer overflow when performing '%c' operation on field %u"),
    96: ("ER_GUEST_USER_PASSWORD",
         "Setting password for guest user has no effect"),
    97: ("ER_TRANSACTION_CONFLICT",
         "Transaction has been aborted by conflict"),
    98: ("ER_UNSUPPORTED_ROLE_PRIV", "Unsupported role privilege '%s'"),
    99: ("ER_LOAD_FUNCTION", "Failed to dynamically load function '%s': %s"),
    100: ("ER_FUNCTION_LANGUAGE",
          "Unsupported language '%s' specified for function '%s'"),
    101: ("ER_RTREE_RECT", (
          "RTree: %s must be an array with %u (point)"
          " or %u (rectangle/box) numeric coordinates")),
    102: ("ER_PROC_C", "%s"),
    103: ("ER_UNKNOWN_RTREE_INDEX_DISTANCE_TYPE",
          "Unknown RTREE index distance type %s"),
    104: ("ER_PROTOCOL", "%s"),
    105: ("ER_UPSERT_UNIQUE_SECONDARY_KEY",
          "Space %s has a unique secondary index and does not support UPSERT"),
    106: ("ER_WRONG_INDEX_RECORD",
          "Wrong record in _index space: got {%s}, expected {%s}"),
    107: ("ER_WRONG_INDEX_PARTS", (
          "Wrong index parts (field %u): %s; expected field1 id (number),"
          " field1 type (string), ...")),
    108: ("ER_WRONG_INDEX_OPTIONS", "Wrong index options (field %u): %s"),
    109: ("ER_WRONG_SCHEMA_VERSION",
          "Wrong schema version, current: %d, in request: %u"),
    110: ("ER_SLAB_ALLOC_MAX", (
          "Failed to allocate %u bytes for tuple in the slab allocator:"
          " tuple is too large. Check 'slab_alloc_maximal'"
          " configuration option.")),
    111: ("ER_WRONG_SPACE_OPTIONS", "Wrong space options (field %u): %s"),
    112: ("ER_UNSUPPORTED_INDEX_FEATURE",
          "Index '%s' (%s) of space '%s' (%s) does not support %s"),
    113: ("ER_VIEW_IS_RO", "View '%s' is read-only"),
}


# Response error code for case "Requested procedure is not defined".
ER_NO_SUCH_PROC = 33
# Response error code for case "Access is denied for user".
ER_ACCESS_DENIED = 42


def tnt_strerror(num):
    """
    Parse Tarantool error to string data.

    :param num: Tarantool error code.
    :type num: :obj:`int`

    :return: Tuple ``(ER_NAME, message)`` or ``"UNDEFINED"``.
    :rtype: :obj:`tuple` or :obj:`str`
    """

    if num in _strerror:
        return _strerror[num]
    return "UNDEFINED"
