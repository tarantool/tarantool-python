# -*- coding: utf-8 -*-
# pylint: disable=R0903
'''
This module provides :class:`~tarantool.schema.Schema` class.
It is a Tarantool schema description.
'''

from tarantool.utils import (
    string_types,
    integer_types,
)
from tarantool.error import (
    Error,
    SchemaError,
    DatabaseError
)
import tarantool.const as const


class RecursionError(Error):
    """Report the situation when max recursion depth is reached.

       This is internal error for <to_unicode_recursive> caller
       and it should be re-raised properly be the caller.
    """


def to_unicode(s):
    if isinstance(s, bytes):
        return s.decode(encoding='utf-8')
    return s


def to_unicode_recursive(x, max_depth):
    """Same as to_unicode(), but traverses over dictionaries,
       lists and tuples recursivery.

       x: value to convert

       max_depth: 1 accepts a scalar, 2 accepts a list of scalars,
       etc.
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
    def __init__(self, index_row, space):
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
        del self.space.indexes[self.iid]
        if self.name:
            del self.space.indexes[self.name]


class SchemaSpace(object):
    def __init__(self, space_row, schema):
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
        del self.schema[self.sid]
        if self.name:
            del self.schema[self.name]


class Schema(object):
    def __init__(self, con):
        self.schema = {}
        self.con = con

    def get_space(self, space):
        space = to_unicode(space)

        try:
            return self.schema[space]
        except KeyError:
            pass

        return self.fetch_space(space)

    def fetch_space(self, space):
        space_row = self.fetch_space_from(space)

        if len(space_row) > 1:
            # We have selected more than one space, it's strange
            raise SchemaError(
                'Some strange output from server: \n' + str(space_row)
            )
        elif len(space_row) == 0 or not len(space_row[0]):
            # We can't find space with this name or id
            temp_name = 'name' if isinstance(space, string_types) else 'id'
            errmsg = "There's no space with {1} '{0}'".format(space, temp_name)
            raise SchemaError(errmsg)

        space_row = space_row[0]

        return SchemaSpace(space_row, self.schema)

    def fetch_space_from(self, space):
        _index = None
        if isinstance(space, string_types):
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
        space_rows = self.fetch_space_from(None)
        for row in space_rows:
            SchemaSpace(row, self.schema)

    def get_index(self, space, index):
        space = to_unicode(space)
        index = to_unicode(index)

        _space = self.get_space(space)
        try:
            return _space.indexes[index]
        except KeyError:
            pass

        return self.fetch_index(_space, index)

    def fetch_index(self, space_object, index):
        index_row = self.fetch_index_from(space_object.sid, index)

        if len(index_row) > 1:
            # We have selected more than one index, it's strange
            raise SchemaError(
                'Some strange output from server: \n' + str(index_row)
            )
        elif len(index_row) == 0 or not len(index_row[0]):
            # We can't find index with this name or id
            temp_name = 'name' if isinstance(index, string_types) else 'id'
            errmsg = ("There's no index with {2} '{0}'"
                      " in space '{1}'").format(index, space_object.name,
                                                temp_name)
            raise SchemaError(errmsg)

        index_row = index_row[0]

        return SchemaIndex(index_row, space_object)

    def fetch_index_all(self):
        index_rows = self.fetch_index_from(None, None)
        for row in index_rows:
            SchemaIndex(row, self.schema[row[0]])

    def fetch_index_from(self, space, index):
        _index = None
        if isinstance(index, string_types):
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
        space = to_unicode(space)
        field = to_unicode(field)

        _space = self.get_space(space)
        try:
            return _space.format[field]
        except:
            tp = 'name' if isinstance(field, string_types) else 'id'
            errmsg = "There's no field with {2} '{0}' in space '{1}'".format(
                    field, _space.name, tp
            )
            raise SchemaError(errmsg)

        return field

    def flush(self):
        self.schema.clear()
