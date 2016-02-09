# -*- coding: utf-8 -*-
# pylint: disable=R0903
'''
This module provides :class:`~tarantool.schema.Schema` class.
It is a Tarantool schema description.
'''

import six

from tarantool.error import SchemaError, DatabaseError
import tarantool.const as const


class SchemaIndex(object):
    def __init__(self, index_row, space):
        self.iid = index_row[1]
        self.name = index_row[2]
        if isinstance(self.name, bytes):
            self.name = self.name.decode()
        self.index = index_row[3]
        self.unique = index_row[4]
        self.parts = []
        if isinstance(index_row[5], (list, tuple)):
            for k, v in index_row[5]:
                self.parts.append((k, v))
        else:
            for i in range(index_row[5]):
                self.parts.append((index_row[5 + 1 + i * 2], index_row[5 + 2 + i * 2]))
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
        self.name = space_row[2]
        if isinstance(self.name, bytes):
            self.name = self.name.decode()
        self.indexes = {}
        self.schema = schema
        self.schema[self.sid] = self
        if self.name:
            self.schema[self.name] = self

    def flush(self):
        del self.schema[self.sid]
        if self.name:
            del self.schema[self.name]


class Schema(object):
    def __init__(self, con):
        self.schema = {}
        self.con = con

    def get_space(self, space):
        try:
            return self.schema[space]
        except KeyError:
            pass
        _index = (const.INDEX_SPACE_NAME
                  if isinstance(space, six.string_types)
                  else const.INDEX_SPACE_PRIMARY)

        space_row = None
        try:
            space_row = self.con.select(const.SPACE_VSPACE, space, index=_index)
        except DatabaseError as e:
            if e.args[0] != 36:
                raise
        if space_row is None:
            space_row = self.con.select(const.SPACE_SPACE, space, index=_index)
        if len(space_row) > 1:
            raise SchemaError('Some strange output from server: \n' + space_row)
        elif len(space_row) == 0 or not len(space_row[0]):
            temp_name = ('name' if isinstance(space, six.string_types) else 'id')
            raise SchemaError(
                "There's no space with {1} '{0}'".format(space, temp_name))
        space_row = space_row[0]
        return SchemaSpace(space_row, self.schema)

    def get_index(self, space, index):
        _space = self.get_space(space)
        try:
            return _space.indexes[index]
        except KeyError:
            pass
        _index = (const.INDEX_INDEX_NAME
                  if isinstance(index, six.string_types)
                  else const.INDEX_INDEX_PRIMARY)

        index_row = None
        try:
            index_row = self.con.select(const.SPACE_VINDEX, [_space.sid, index],
                                index=_index)
        except DatabaseError as e:
            if e.args[0] != 36:
                raise
        if index_row is None:
            index_row = self.con.select(const.SPACE_INDEX, [_space.sid, index],
                                index=_index)

        if len(index_row) > 1:
            raise SchemaError('Some strange output from server: \n' + index_row)
        elif len(index_row) == 0 or not len(index_row[0]):
            temp_name = ('name' if isinstance(index, six.string_types) else 'id')
            raise SchemaError(
                "There's no index with {2} '{0}' in space '{1}'".format(
                    index, _space.name, temp_name))
        index_row = index_row[0]
        return SchemaIndex(index_row, _space)

    def flush(self):
        self.schema.clear()
