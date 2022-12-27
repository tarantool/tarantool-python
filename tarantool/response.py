# pylint: disable=C0301,W0105,W0401,W0614
"""
Request response types definitions.
"""

from collections.abc import Sequence

import json
import msgpack

from tarantool.const import (
    IPROTO_REQUEST_TYPE,
    IPROTO_DATA,
    IPROTO_ERROR_24,
    IPROTO_ERROR,
    IPROTO_SYNC,
    IPROTO_SCHEMA_ID,
    REQUEST_TYPE_ERROR,
    IPROTO_SQL_INFO,
    IPROTO_SQL_INFO_ROW_COUNT,
    IPROTO_SQL_INFO_AUTOINCREMENT_IDS,
    IPROTO_VERSION,
    IPROTO_FEATURES,
    IPROTO_AUTH_TYPE,
)
from tarantool.types import decode_box_error
from tarantool.error import (
    DatabaseError,
    InterfaceError,
    SchemaReloadException,
    tnt_strerror
)
from tarantool.schema import to_unicode

from tarantool.msgpack_ext.unpacker import ext_hook as unpacker_ext_hook

def unpacker_factory(conn):
    """
    Build unpacker to unpack request response.

    :param conn: Request sender.
    :type conn: :class:`~tarantool.Connection`

    :rtype: :class:`msgpack.Unpacker`
    """

    unpacker_kwargs = dict()

    # Decode MsgPack arrays into Python lists by default (not tuples).
    # Can be configured in the Connection init
    unpacker_kwargs['use_list'] = conn.use_list

    # Use raw=False instead of encoding='utf-8'.
    if msgpack.version >= (0, 5, 2) and conn.encoding == 'utf-8':
        # Get rid of the following warning.
        # > PendingDeprecationWarning: encoding is deprecated,
        # > Use raw=False instead.
        unpacker_kwargs['raw'] = False
    elif conn.encoding is not None:
        unpacker_kwargs['encoding'] = conn.encoding

    # raw=False is default since msgpack-1.0.0.
    #
    # The option decodes mp_str to bytes, not a Unicode
    # string (when True).
    if msgpack.version >= (1, 0, 0) and conn.encoding is None:
        unpacker_kwargs['raw'] = True

    # encoding option is not supported since msgpack-1.0.0,
    # but it is handled in the Connection constructor.
    assert(msgpack.version < (1, 0, 0) or conn.encoding in (None, 'utf-8'))

    # strict_map_key=True is default since msgpack-1.0.0.
    #
    # The option forbids non-string keys in a map (when True).
    if msgpack.version >= (1, 0, 0):
        unpacker_kwargs['strict_map_key'] = False

    # We need configured unpacker to work with error extention
    # type payload, but module do not provide access to self
    # inside extension type unpackers.
    unpacker_no_ext = msgpack.Unpacker(**unpacker_kwargs)
    ext_hook = lambda code, data: unpacker_ext_hook(code, data, unpacker_no_ext)
    unpacker_kwargs['ext_hook'] = ext_hook

    return msgpack.Unpacker(**unpacker_kwargs)


class Response(Sequence):
    """
    Represents a single response from the server in compliance with the
    Tarantool protocol. Responsible for data encapsulation (i.e.
    received list of tuples) and parsing of binary packets received from
    the server.
    """

    def __init__(self, conn, response):
        """
        :param conn: Request sender.
        :type conn: :class:`~tarantool.Connection`

        :param response: Response binary data.
        :type response: :obj:`bytes`

        :raise: :exc:`~tarantool.error.DatabaseError`,
            :exc:`~tarantool.error.SchemaReloadException`
        """

        # This is not necessary, because underlying list data structures are
        # created in the __new__().
        # super(Response, self).__init__()

        unpacker = conn._unpacker_factory()

        unpacker.feed(response)
        header = unpacker.unpack()

        self.conn = conn
        self._sync = header.get(IPROTO_SYNC, 0)
        self._code = header[IPROTO_REQUEST_TYPE]
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
            self._return_message = self._body.get(IPROTO_ERROR_24, "")
            self._return_code = self._code & (REQUEST_TYPE_ERROR - 1)

            self._return_error = None
            return_error_map = self._body.get(IPROTO_ERROR)
            if return_error_map is not None:
                self._return_error = decode_box_error(return_error_map)

            self._data = []
            if self._return_code == 109:
                raise SchemaReloadException(self._return_message,
                                            self._schema_version)
            if self.conn.error:
                raise DatabaseError(self._return_code,
                                    self._return_message,
                                    extra_info=self._return_error)

    def __getitem__(self, idx):
        if self._data is None:
            raise InterfaceError("Trying to access data when there's no data")
        return self._data.__getitem__(idx)

    def __len__(self):
        if self._data is None:
            raise InterfaceError("Trying to access data when there's no data")
        return len(self._data)

    def __contains__(self, item):
        if self._data is None:
            raise InterfaceError("Trying to access data when there's no data")
        return item in self._data

    def __iter__(self):
        if self._data is None:
            raise InterfaceError("Trying to access data when there's no data")
        return iter(self._data)

    def __reversed__(self):
        if self._data is None:
            raise InterfaceError("Trying to access data when there's no data")
        return reversed(self._data)

    def index(self, *args):
        """
        Refer to :class:`collections.abc.Sequence`.

        :raises: :exc:`~tarantool.error.InterfaceError.`
        """

        if self._data is None:
            raise InterfaceError("Trying to access data when there's no data")
        return self._data.index(*args)

    def count(self, item):
        """
        Refer to :class:`collections.abc.Sequence`.

        :raises: :exc:`~tarantool.error.InterfaceError`
        """

        if self._data is None:
            raise InterfaceError("Trying to access data when there's no data")
        return self._data.count(item)

    @property
    def rowcount(self):
        """
        :type: :obj:`int`

        Number of rows affected or returned by a query.
        """

        return len(self)

    @property
    def body(self):
        """
        :type: :obj:`dict`

        Raw response body.
        """

        return self._body

    @property
    def code(self):
        """
        :type: :obj:`int`

        Response type id.
        """

        return self._code

    @property
    def sync(self):
        """
        :type: :obj:`int`

        Response header IPROTO_SYNC.
        """

        return self._sync

    @property
    def return_code(self):
        """
        :type: :obj:`int`

        If the request was successful, the value of is ``0``.
        Otherwise, it contains an error code. If the value is non-zero,
        :attr:`return_message` contains an error message.
        """

        return self._return_code

    @property
    def data(self):
        """
        :type: :obj:`object`

        Contains the list of tuples for SELECT, REPLACE and DELETE
        requests and arbitrary data for CALL.
        """

        return self._data

    @property
    def strerror(self):
        """
        Refer to :func:`~tarantool.error.tnt_strerror`.
        """

        return tnt_strerror(self._return_code)

    @property
    def return_message(self):
        """
        :type: :obj:`str`

        The error message returned by the server in case of non-zero
        :attr:`return_code` (empty string otherwise).
        """

        return self._return_message

    @property
    def schema_version(self):
        """
        :type: :obj:`int`

        Request current schema version.
        """

        return self._schema_version

    def __str__(self):
        if self.return_code:
            return json.dumps({
                'error': {
                    'code': self.strerror[0],
                    'reason': self.return_message
                }
            }, sort_keys = True, indent = 4, separators=(', ', ': '))
        output = []
        for tpl in self._data or ():
            output.extend(("- ", repr(tpl), "\n"))
        if len(output) > 0:
            output.pop()
        return ''.join(output)

    __repr__ = __str__


class ResponseExecute(Response):
    """
    Represents an SQL EXECUTE request response.
    """

    @property
    def autoincrement_ids(self):
        """
        A list with the new primary-key value (or values) for an
        INSERT in a table defined with PRIMARY KEY AUTOINCREMENT (NOT
        result set size).

        :rtype: :obj:`list` or :obj:`None`
        """

        if self._return_code != 0:
            return None
        info = self._body.get(IPROTO_SQL_INFO)

        if info is None:
            return None

        autoincrement_ids = info.get(IPROTO_SQL_INFO_AUTOINCREMENT_IDS)

        return autoincrement_ids

    @property
    def affected_row_count(self):
        """
        The number of changed rows for responses to DML requests and
        ``None`` for DQL requests.

        :rtype: :obj:`int` or :obj:`None`
        """

        if self._return_code != 0:
            return None
        info = self._body.get(IPROTO_SQL_INFO)

        if info is None:
            return None

        return info.get(IPROTO_SQL_INFO_ROW_COUNT)


class ResponseProtocolVersion(Response):
    """
    Represents an ID request response: information about server protocol
    version and features it supports.
    """

    @property
    def protocol_version(self):
        """
        Server protocol version.

        :rtype: :obj:`int` or :obj:`None`
        """

        if self._return_code != 0:
            return None
        return self._body.get(IPROTO_VERSION)

    @property
    def features(self):
        """
        Server supported features.

        :rtype: :obj:`list`
        """

        if self._return_code != 0:
            return []
        return self._body.get(IPROTO_FEATURES)

    @property
    def auth_type(self):
        """
        Server expected authentication method.

        :rtype: :obj:`str` or :obj:`None`
        """

        if self._return_code != 0:
            return None
        return to_unicode(self._body.get(IPROTO_AUTH_TYPE))
