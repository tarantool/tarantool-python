"""
Request types definitions. For internal use only, there is no API to
send pre-build request objects.
"""

import hashlib
from collections.abc import Sequence, Mapping

import msgpack

from tarantool.const import (
    IPROTO_REQUEST_TYPE,
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
    IPROTO_EXPR,
    IPROTO_OPS,
    IPROTO_SCHEMA_ID,
    IPROTO_SQL_TEXT,
    IPROTO_SQL_BIND,
    IPROTO_VERSION,
    IPROTO_FEATURES,
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
    REQUEST_TYPE_EXECUTE,
    REQUEST_TYPE_EVAL,
    REQUEST_TYPE_AUTHENTICATE,
    REQUEST_TYPE_ID,
    AUTH_TYPE_CHAP_SHA1,
    AUTH_TYPE_PAP_SHA256,
)
from tarantool.response import (
    Response,
    ResponseExecute,
    ResponseProtocolVersion,
)
from tarantool.utils import (
    strxor,
)

from tarantool.msgpack_ext.packer import default as packer_default


def packer_factory(conn):
    """
    Build packer to pack request.

    :param conn: Request sender.
    :type conn: :class:`~tarantool.Connection`

    :rtype: :class:`msgpack.Packer`
    """

    packer_kwargs = {}

    # use_bin_type=True is default since msgpack-1.0.0.
    #
    # The option controls whether to pack binary (non-unicode)
    # string values as mp_bin or as mp_str.
    #
    # The default behaviour of the Python 3 connector (since
    # default encoding is "utf-8") is to pack bytes as mp_bin
    # and Unicode strings as mp_str. encoding=None mode must
    # be used to work with non-utf strings.
    #
    # encoding = 'utf-8'
    #
    # Python 3 -> Tarantool          -> Python 3
    # str      -> mp_str (string)    -> str
    # bytes    -> mp_bin (varbinary) -> bytes
    #
    # encoding = None
    #
    # Python 3 -> Tarantool          -> Python 3
    # bytes    -> mp_str (string)    -> bytes
    # str      -> mp_str (string)    -> bytes
    #             mp_bin (varbinary) -> bytes
    #
    # msgpack-0.5.0 (and only this version) warns when the
    # option is unset:
    #
    #  | FutureWarning: use_bin_type option is not specified.
    #  | Default value of the option will be changed in future
    #  | version.
    #
    # The option is supported since msgpack-0.4.0, so we can
    # just always set it for all msgpack versions to get rid
    # of the warning on msgpack-0.5.0 and to keep our
    # behaviour on msgpack-1.0.0.
    if conn.encoding is None:
        packer_kwargs['use_bin_type'] = False
    else:
        packer_kwargs['use_bin_type'] = True

    # We need configured packer to work with error extension
    # type payload, but module do not provide access to self
    # inside extension type packers.
    def default(obj):
        packer_no_ext = msgpack.Packer(**packer_kwargs)
        return packer_default(obj, packer_no_ext)
    packer_kwargs['default'] = default

    return msgpack.Packer(**packer_kwargs)


class Request():
    """
    Represents a single request to the server in compliance with the
    Tarantool protocol. Responsible for data encapsulation and building
    the binary packet to be sent to the server.

    This is the abstract base class. Specific request types are
    implemented in the inherited classes.
    """

    request_type = None

    def __init__(self, conn):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`
        """

        self._bytes = None
        self.conn = conn
        self._sync = None
        self._body = ''
        self.response_class = Response

        self.packer = conn._packer_factory()

    def _dumps(self, src):
        """
        Encode MsgPack data.
        """

        return self.packer.pack(src)

    def __bytes__(self):
        return self.header(len(self._body)) + self._body

    __str__ = __bytes__

    @property
    def sync(self):
        """
        :type: :obj:`int`

        Contains request header IPROTO_SYNC.
        """

        return self._sync

    def header(self, length):
        """
        Pack total (header + payload) length info together with header
        itself.

        :param length: Payload length.
        :type: :obj:`int`

        :return: MsgPack data with encoded total (header + payload)
            length info and header.
        :rtype: :obj:`bytes`
        """

        self._sync = self.conn.generate_sync()
        header_fields = {
            IPROTO_REQUEST_TYPE: self.request_type,
            IPROTO_SYNC: self._sync,
        }
        if self.conn.schema is not None:
            header_fields[IPROTO_SCHEMA_ID] = self.conn.schema_version
        header = self._dumps(header_fields)

        return self._dumps(length + len(header)) + header


class RequestInsert(Request):
    """
    Represents INSERT request.
    """

    request_type = REQUEST_TYPE_INSERT

    def __init__(self, conn, space_no, values):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param space_no: Space id.
        :type space_no: :obj:`int`

        :param values: Record to be inserted.
        :type values: :obj:`tuple` or :obj:`list`

        :raise: :exc:`~AssertionError`
        """

        super().__init__(conn)
        assert isinstance(values, (tuple, list))

        request_body = self._dumps({IPROTO_SPACE_ID: space_no,
                                    IPROTO_TUPLE: values})

        self._body = request_body


def sha1(values):
    """
    Compute hash.

    :param values: Values to hash.
    :type values: :obj:`tuple`

    :rtype: :obj:`str`

    :meta private:
    """

    sha = hashlib.sha1()
    for i in values:
        if i is not None:
            if isinstance(i, bytes):
                sha.update(i)
            else:
                sha.update(i.encode())
    return sha.digest()


class RequestAuthenticate(Request):
    """
    Represents AUTHENTICATE request.
    """

    request_type = REQUEST_TYPE_AUTHENTICATE

    def __init__(self, conn, salt, user, password, auth_type=AUTH_TYPE_CHAP_SHA1):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param salt: base64-encoded session salt.
        :type salt: :obj:`str`

        :param user: User name for authentication on the Tarantool
            server.
        :type user: :obj:`str`

        :param password: User password for authentication on the
            Tarantool server.
        :type password: :obj:`str`

        :param auth_type: Refer to :paramref:`~tarantool.Connection.auth_type`.
        :type auth_type: :obj:`str`, optional
        """
        # pylint: disable=too-many-arguments

        super().__init__(conn)

        if auth_type == AUTH_TYPE_CHAP_SHA1:
            hash1 = sha1((password,))
            hash2 = sha1((hash1,))
            scramble = sha1((salt, hash2))
            scramble = strxor(hash1, scramble)
        elif auth_type == AUTH_TYPE_PAP_SHA256:
            scramble = password
        else:
            raise ValueError(f'Unexpected auth_type {auth_type}')

        self._body = self._dumps({IPROTO_USER_NAME: user,
                                  IPROTO_TUPLE: (auth_type, scramble)})

    def header(self, length):
        """
        Pack total (header + payload) length info together with header
        itself.

        :param length: Payload length.
        :type: :obj:`int`

        :return: MsgPack data with encoded total (header + payload)
            length info and header.
        :rtype: :obj:`bytes`
        """

        self._sync = self.conn.generate_sync()
        # Set IPROTO_SCHEMA_ID: 0 to avoid SchemaReloadException
        # It is ok to use 0 in auth every time.
        header = self._dumps({IPROTO_REQUEST_TYPE: self.request_type,
                              IPROTO_SYNC: self._sync,
                              IPROTO_SCHEMA_ID: 0})

        return self._dumps(length + len(header)) + header


class RequestReplace(Request):
    """
    Represents REPLACE request.
    """

    request_type = REQUEST_TYPE_REPLACE

    def __init__(self, conn, space_no, values):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param space_no: Space id.
        :type space_no: :obj:`int`

        :param values: Record to be replaced.
        :type values: :obj:`tuple` or :obj:`list`

        :raise: :exc:`~AssertionError`
        """

        super().__init__(conn)
        assert isinstance(values, (tuple, list))

        request_body = self._dumps({IPROTO_SPACE_ID: space_no,
                                    IPROTO_TUPLE: values})

        self._body = request_body


class RequestDelete(Request):
    """
    Represents DELETE request.
    """

    request_type = REQUEST_TYPE_DELETE

    def __init__(self, conn, space_no, index_no, key):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param space_no: Space id.
        :type space_no: :obj:`int`

        :param index_no: Index id.
        :type index_no: :obj:`int`

        :param key: Key of a tuple to be deleted.
        :type key: :obj:`list`

        :raise: :exc:`~AssertionError`
        """

        super().__init__(conn)

        request_body = self._dumps({IPROTO_SPACE_ID: space_no,
                                    IPROTO_INDEX_ID: index_no,
                                    IPROTO_KEY: key})

        self._body = request_body


class RequestSelect(Request):
    """
    Represents SELECT request.
    """

    request_type = REQUEST_TYPE_SELECT

    def __init__(self, conn, space_no, index_no, key, offset, limit, iterator):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param space_no: Space id.
        :type space_no: :obj:`int`

        :param index_no: Index id.
        :type index_no: :obj:`int`

        :param key: Key of a tuple to be selected.
        :type key: :obj:`list`

        :param offset: Number of tuples to skip.
        :type offset: :obj:`int`

        :param limit: Maximum number of tuples to select.
        :type limit: :obj:`int`

        :param iterator: Index iterator type, see
            :paramref:`~tarantool.Connection.select.params.iterator`.
        :type iterator: :obj:`str`

        :raise: :exc:`~AssertionError`
        """
        # pylint: disable=too-many-arguments

        super().__init__(conn)
        request_body = self._dumps({IPROTO_SPACE_ID: space_no,
                                    IPROTO_INDEX_ID: index_no,
                                    IPROTO_OFFSET: offset,
                                    IPROTO_LIMIT: limit,
                                    IPROTO_ITERATOR: iterator,
                                    IPROTO_KEY: key})

        self._body = request_body


class RequestUpdate(Request):
    """
    Represents UPDATE request.
    """

    request_type = REQUEST_TYPE_UPDATE

    def __init__(self, conn, space_no, index_no, key, op_list):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param space_no: Space id.
        :type space_no: :obj:`int`

        :param index_no: Index id.
        :type index_no: :obj:`int`

        :param key: Key of a tuple to be updated.
        :type key: :obj:`list`

        :param op_list: The list of operations to update individual
            fields, refer to
            :paramref:`~tarantool.Connection.update.params.op_list`.
        :type op_list: :obj:`tuple` or :obj:`list`

        :raise: :exc:`~AssertionError`
        """
        # pylint: disable=too-many-arguments

        super().__init__(conn)

        request_body = self._dumps({IPROTO_SPACE_ID: space_no,
                                    IPROTO_INDEX_ID: index_no,
                                    IPROTO_KEY: key,
                                    IPROTO_TUPLE: op_list})

        self._body = request_body


class RequestCall(Request):
    """
    Represents CALL request.
    """

    request_type = REQUEST_TYPE_CALL

    def __init__(self, conn, name, args, call_16):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param name: Stored Lua function name.
        :type func_name: :obj:`str`

        :param args: Stored Lua function arguments.
        :type args: :obj:`tuple`

        :param call_16: If ``True``, use compatibility mode with
            Tarantool 1.6 or older.
        :type call_16: :obj:`bool`

        :raise: :exc:`~AssertionError`
        """

        if call_16:
            self.request_type = REQUEST_TYPE_CALL16
        super().__init__(conn)
        assert isinstance(args, (list, tuple))

        request_body = self._dumps({IPROTO_FUNCTION_NAME: name,
                                    IPROTO_TUPLE: args})

        self._body = request_body


class RequestEval(Request):
    """
    Represents EVAL request.
    """

    request_type = REQUEST_TYPE_EVAL

    def __init__(self, conn, name, args):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param name: Lua expression.
        :type func_name: :obj:`str`

        :param args: Lua expression arguments.
        :type args: :obj:`tuple`

        :raise: :exc:`~AssertionError`
        """

        super().__init__(conn)
        assert isinstance(args, (list, tuple))

        request_body = self._dumps({IPROTO_EXPR: name,
                                    IPROTO_TUPLE: args})

        self._body = request_body


class RequestPing(Request):
    """
    Represents a ping request with the empty body.
    """

    request_type = REQUEST_TYPE_PING

    def __init__(self, conn):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`
        """

        super().__init__(conn)
        self._body = b''


class RequestUpsert(Request):
    """
    Represents UPSERT request.
    """

    request_type = REQUEST_TYPE_UPSERT

    def __init__(self, conn, space_no, index_no, tuple_value, op_list):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param space_no: Space id.
        :type space_no: :obj:`int`

        :param index_no: Index id.
        :type index_no: :obj:`int`

        :param tuple_value: Tuple to be upserted.
        :type tuple_value: :obj:`tuple` or :obj:`list`

        :param op_list: The list of operations to update individual
            fields, refer to
            :paramref:`~tarantool.Connection.update.params.op_list`.
        :type op_list: :obj:`tuple` or :obj:`list`

        :raise: :exc:`~AssertionError`
        """
        # pylint: disable=too-many-arguments

        super().__init__(conn)

        request_body = self._dumps({IPROTO_SPACE_ID: space_no,
                                    IPROTO_INDEX_ID: index_no,
                                    IPROTO_TUPLE: tuple_value,
                                    IPROTO_OPS: op_list})

        self._body = request_body


class RequestOK(Request):
    """
    Represents OK acknowledgement.
    """

    request_type = REQUEST_TYPE_OK

    def __init__(self, conn, sync):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param sync: Previous request sync id.
        :type sync: :obj:`int`
        """

        super().__init__(conn)
        request_body = self._dumps({IPROTO_REQUEST_TYPE: self.request_type,
                                    IPROTO_SYNC: sync})
        self._body = request_body


class RequestExecute(Request):
    """
    Represents EXECUTE SQL request.
    """

    request_type = REQUEST_TYPE_EXECUTE

    def __init__(self, conn, sql, args):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param sql: SQL query.
        :type sql: :obj:`str`

        :param args: SQL query bind values.
        :type args: :obj:`dict` or :obj:`list`

        :raise: :exc:`~TypeError`
        """

        super().__init__(conn)
        if isinstance(args, Mapping):
            args = [{f":{name}": value} for name, value in args.items()]
        elif not isinstance(args, Sequence):
            raise TypeError(f"Parameter type '{type(args)}' is not supported. "
                            "Must be a mapping or sequence")

        request_body = self._dumps({IPROTO_SQL_TEXT: sql,
                                    IPROTO_SQL_BIND: args})

        self._body = request_body
        self.response_class = ResponseExecute


class RequestProtocolVersion(Request):
    """
    Represents ID request: inform the server about the protocol
    version and features connector support.
    """

    request_type = REQUEST_TYPE_ID

    def __init__(self, conn, protocol_version, features):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param protocol_version: Connector protocol version.
        :type protocol_version: :obj:`int`

        :param features: List of supported features.
        :type features: :obj:`list`
        """

        super().__init__(conn)

        request_body = self._dumps({IPROTO_VERSION: protocol_version,
                                    IPROTO_FEATURES: features})

        self._body = request_body
        self.response_class = ResponseProtocolVersion
