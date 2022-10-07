# pylint: disable=R0903
"""
Schema types definitions. For internal use only, there is no API to use
pre-build schema objects.
"""

from tarantool.error import (
    Error,
    SchemaError,
    DatabaseError
)
import tarantool.const as const


class RecursionError(Error):
    """
    Report the situation when max recursion depth is reached.

    This is an internal error of
    :func:`~tarantool.schema.to_unicode_recursive` caller and it should
    be re-raised properly by the caller.
    """


def to_unicode(s):
    """
    Decode :obj:`bytes` to unicode :obj:`str`.

    :param s: Value to convert.

    :return: Decoded unicode :obj:`str`, if value is :obj:`bytes`.
        Otherwise, it returns the original value.

    :meta private:
    """

    if isinstance(s, bytes):
        return s.decode(encoding='utf-8')
    return s


def to_unicode_recursive(x, max_depth):
    """
    Recursively decode :obj:`bytes` to unicode :obj:`str` over
    :obj:`dict`, :obj:`list` and :obj:`tuple`.

    :param x: Value to convert.

    :param max_depth: Maximum depth recursion.
    :type max_depth: :obj:`int`

    :return: The same structure where all :obj:`bytes` are replaced
        with unicode :obj:`str`.

    :raise: :exc:`~tarantool.schema.RecursionError`

    :meta private:
    """

    if max_depth <= 0:
        raise RecursionError('Max recursion depth is reached')

    if isinstance(x, dict):
        res = dict()
        for key, val in x.items():
            key = to_unicode_recursive(key, max_depth - 1)
            val = to_unicode_recursive(val, max_depth - 1)
            res[key] = val
        return res

    if isinstance(x, list) or isinstance(x, tuple):
        res = []
        for val in x:
            val = to_unicode_recursive(val, max_depth - 1)
            res.append(val)
        if isinstance(x, tuple):
            return tuple(res)
        return res

    return to_unicode(x)


class SchemaIndex(object):
    """
    Contains schema for a space index.
    """

    def __init__(self, index_row, space):
        """
        :param index_row: Index format data received from Tarantool.
        :type index_row: :obj:`list` or :obj:`tuple`

        :param space: Related space schema.
        :type space: :class:`~tarantool.schema.SchemaSpace`

        :raise: :exc:`~tarantool.error.SchemaError`
        """

        self.iid = index_row[1]
        self.name = index_row[2]
        self.name = to_unicode(index_row[2])
        self.index = index_row[3]
        self.unique = index_row[4]
        self.parts = []
        try:
            parts_raw = to_unicode_recursive(index_row[5], 3)
        except RecursionError as e:
            errmsg = 'Unexpected index parts structure: ' + str(e)
            raise SchemaError(errmsg)
        if isinstance(parts_raw, (list, tuple)):
            for val in parts_raw:
                if isinstance(val, dict):
                    self.parts.append((val['field'], val['type']))
                else:
                    self.parts.append((val[0], val[1]))
        else:
            for i in range(parts_raw):
                self.parts.append((
                    to_unicode(index_row[5 + 1 + i * 2]),
                    to_unicode(index_row[5 + 2 + i * 2])
                ))
        self.space = space
        self.space.indexes[self.iid] = self
        if self.name:
            self.space.indexes[self.name] = self

    def flush(self):
        """
        Clean existing index data.
        """

        del self.space.indexes[self.iid]
        if self.name:
            del self.space.indexes[self.name]


class SchemaSpace(object):
    """
    Contains schema for a space.
    """

    def __init__(self, space_row, schema):
        """
        :param space_row: Space format data received from Tarantool.
        :type space_row: :obj:`list` or :obj:`tuple`

        :param schema: Related server schema.
        :type schema: :class:`~tarantool.schema.Schema`

        :raise: :exc:`~tarantool.error.SchemaError`
        """

        self.sid = space_row[0]
        self.arity = space_row[1]
        self.name = to_unicode(space_row[2])
        self.indexes = {}
        self.schema = schema
        self.schema[self.sid] = self
        if self.name:
            self.schema[self.name] = self
        self.format = dict()
        try:
            format_raw = to_unicode_recursive(space_row[6], 3)
        except RecursionError as e:
            errmsg = 'Unexpected space format structure: ' + str(e)
            raise SchemaError(errmsg)
        for part_id, part in enumerate(format_raw):
            part['id'] = part_id
            self.format[part['name']] = part
            self.format[part_id     ] = part

    def flush(self):
        """
        Clean existing space data.
        """

        del self.schema[self.sid]
        if self.name:
            del self.schema[self.name]


class Schema(object):
    """
    Contains Tarantool server spaces schema.
    """

    def __init__(self, con):
        """
        :param con: Related Tarantool server connection.
        :type con: :class:`~tarantool.Connection`
        """

        self.schema = {}
        self.con = con

    def get_space(self, space):
        """
        Get space schema. If it exists in the local schema, return local
        data, otherwise fetch data from the Tarantool server.

        :param space: Space name or space id.
        :type space: :obj:`str` or :obj:`int`

        :rtype: :class:`~tarantool.schema.SchemaSpace`

        :raises: :meth:`~tarantool.schema.Schema.fetch_space` exceptions
        """

        space = to_unicode(space)

        try:
            return self.schema[space]
        except KeyError:
            pass

        return self.fetch_space(space)

    def fetch_space(self, space):
        """
        Fetch a single space schema from the Tarantool server and build
        a schema object.

        :param space: Space name or space id to fetch.
        :type space: :obj:`str` or :obj:`int`

        :rtype: :class:`~tarantool.schema.SchemaSpace`

        :raises: :exc:`~tarantool.error.SchemaError`,
            :meth:`~tarantool.schema.Schema.fetch_space_from` exceptions
        """

        space_row = self.fetch_space_from(space)

        if len(space_row) > 1:
            # We have selected more than one space, it's strange
            raise SchemaError(
                'Some strange output from server: \n' + str(space_row)
            )
        elif len(space_row) == 0 or not len(space_row[0]):
            # We can't find space with this name or id
            temp_name = 'name' if isinstance(space, str) else 'id'
            errmsg = "There's no space with {1} '{0}'".format(space, temp_name)
            raise SchemaError(errmsg)

        space_row = space_row[0]

        return SchemaSpace(space_row, self.schema)

    def fetch_space_from(self, space):
        """
        Fetch space schema from the Tarantool server.

        :param space: Space name or space id to fetch. If ``None``,
            fetch all spaces.
        :type space: :obj:`str` or :obj:`int` or :obj:`None`

        :return: Space format data received from Tarantool.
        :rtype: :obj:`list` or :obj:`tuple`

        :raises: :meth:`~tarantool.Connection.select` exceptions
        """

        _index = None
        if isinstance(space, str):
            _index = const.INDEX_SPACE_NAME
        else:
            _index = const.INDEX_SPACE_PRIMARY

        if space is None:
            space = ()

        space_row = None
        try:
            # Try to fetch from '_vspace'
            space_row = self.con.select(const.SPACE_VSPACE, space,
                                        index=_index)
        except DatabaseError as e:
            # if space can't be found, then user is using old version of
            # tarantool, try again with '_space'
            if e.args[0] != 36:
                raise
        if space_row is None:
            # Try to fetch from '_space'
            space_row = self.con.select(const.SPACE_SPACE, space, index=_index)

        return space_row

    def fetch_space_all(self):
        """
        Fetch all spaces schema from the Tarantool server and build
        corresponding schema objects.

        :raises: :meth:`~tarantool.schema.Schema.fetch_space_from`
            exceptions
        """

        space_rows = self.fetch_space_from(None)
        for row in space_rows:
            SchemaSpace(row, self.schema)

    def get_index(self, space, index):
        """
        Get space index schema. If it exists in the local schema, return
        local data, otherwise fetch data from the Tarantool server.

        :param space: Space id or space name.
        :type space: :obj:`str` or :obj:`int`

        :param index: Index id or index name.
        :type index: :obj:`str` or :obj:`int`

        :rtype: :class:`~tarantool.schema.SchemaIndex`

        :raises: :meth:`~tarantool.schema.Schema.fetch_index` exceptions
        """
        space = to_unicode(space)
        index = to_unicode(index)

        _space = self.get_space(space)
        try:
            return _space.indexes[index]
        except KeyError:
            pass

        return self.fetch_index(_space, index)

    def fetch_index(self, space_object, index):
        """
        Fetch a single index space schema from the Tarantool server and
        build a schema object.

        :param space: Space schema.
        :type space: :class:`~tarantool.schema.SchemaSpace`

        :param index: Index name or id.
        :type index: :obj:`str` or :obj:`int`

        :rtype: :class:`~tarantool.schema.SchemaIndex`

        :raises: :exc:`~tarantool.error.SchemaError`,
            :meth:`~tarantool.schema.Schema.fetch_index_from` exceptions
        """

        index_row = self.fetch_index_from(space_object.sid, index)

        if len(index_row) > 1:
            # We have selected more than one index, it's strange
            raise SchemaError(
                'Some strange output from server: \n' + str(index_row)
            )
        elif len(index_row) == 0 or not len(index_row[0]):
            # We can't find index with this name or id
            temp_name = 'name' if isinstance(index, str) else 'id'
            errmsg = ("There's no index with {2} '{0}'"
                      " in space '{1}'").format(index, space_object.name,
                                                temp_name)
            raise SchemaError(errmsg)

        index_row = index_row[0]

        return SchemaIndex(index_row, space_object)

    def fetch_index_all(self):
        """
        Fetch all spaces indexes schema from the Tarantool server and
        build corresponding schema objects.

        :raises: :meth:`~tarantool.schema.Schema.fetch_index_from`
            exceptions
        """
        index_rows = self.fetch_index_from(None, None)
        for row in index_rows:
            SchemaIndex(row, self.schema[row[0]])

    def fetch_index_from(self, space, index):
        """
        Fetch space index schema from the Tarantool server.

        :param space: Space id. If ``None``, fetch all spaces
            index schema.
        :type space: :obj:`int` or :obj:`None`

        :param index: Index name or id. If ``None``, fetch all space
            indexes schema.
        :type index: :obj:`str` or :obj:`int` or :obj:`None`

        :return: Space index format data received from Tarantool.
        :rtype: :obj:`list` or :obj:`tuple`

        :raises: :meth:`~tarantool.Connection.select` exceptions
        """

        _index = None
        if isinstance(index, str):
            _index = const.INDEX_INDEX_NAME
        else:
            _index = const.INDEX_INDEX_PRIMARY

        _key_tuple = None
        if space is None and index is None:
            _key_tuple = ()
        elif space is not None and index is None:
            _key_tuple = (space)
        elif space is not None and index is not None:
            _key_tuple = (space, index)
        else:
            raise SchemaError("Bad arguments for schema resolving")

        index_row = None
        try:
            # Try to fetch from '_vindex'
            index_row = self.con.select(const.SPACE_VINDEX, _key_tuple,
                                        index=_index)
        except DatabaseError as e:
            # if space can't be found, then user is using old version of
            # tarantool, try again with '_index'
            if e.args[0] != 36:
                raise
        if index_row is None:
            # Try to fetch from '_index'
            index_row = self.con.select(const.SPACE_INDEX, _key_tuple,
                                        index=_index)

        return index_row

    def get_field(self, space, field):
        """
        Get space field format info.

        :param space: Space name or space id.
        :type space: :obj:`str` or :obj:`int`

        :param field: Field name or field id.
        :type field: :obj:`str` or :obj:`int`

        :return: Field format info.
        :rtype: :obj:`dict`

        :raises: :exc:`~tarantool.error.SchemaError`,
            :meth:`~tarantool.schema.Schema.fetch_space` exceptions
        """
        space = to_unicode(space)
        field = to_unicode(field)

        _space = self.get_space(space)
        try:
            return _space.format[field]
        except:
            tp = 'name' if isinstance(field, str) else 'id'
            errmsg = "There's no field with {2} '{0}' in space '{1}'".format(
                    field, _space.name, tp
            )
            raise SchemaError(errmsg)

        return field

    def flush(self):
        """
        Clean existing schema data.
        """

        self.schema.clear()
