# -*- coding: utf-8 -*-
# pylint: disable=R0903
'''
This module provides :class:`~tarantool.schema.Schema` class.
It is a Tarantool schema description.
'''

from tarantool.error import SchemaError
import tarantool.const as const

class SchemaIndex(object):
    def __init__(self, array, space):
        self.iid = array[1]
        self.name = array[2]
        self.index = array[3]
        self.unique = array[4]
        self.parts = []
        for i in xrange(array[5]):
            self.parts.append((array[5+1+i*2], array[5+2+i*2]))
        self.space = space
        self.space.indexes[self.iid] = self

    def flush(self):
        del self.space.indexes[self.iid]

class SchemaSpace(object):
    def __init__(self, array, schema):
        self.sid = array[0]
        self.arity = array[1]
        self.name = array[2]
        self.indexes = {}
        self.schema = schema
        self.schema[self.sid] = self

    def flush(self):
        del self.schema[self.sid]

class Schema(object):
    def __init__(self, con):
        self.schema = {}
        self.con = con

    def find_local_space(self, space):
        if isinstance(space, basestring):
            for _, val in self.schema.iteritems():
                if val.name == space:
                    return val
            return None
        try:
            return self.schema[space]
        except KeyError:
            return None

    def find_local_index(self, space, index):
        space = self.find_local_space(space)
        if space is None:
            return None
        if isinstance(index, basestring):
            for _, val in space.indexes.iteritems():
                if val.name == index:
                    return val
            return None
        try:
            return space.indexes[index]
        except KeyError:
            return None

    def get_space(self, space):
        _space = self.find_local_space(space)
        if _space is not None:
            return _space
        _index = (const.INDEX_SPACE_NAME if isinstance(space, basestring) else const.INDEX_SPACE_PRIMARY)

        array = self.con.select(const.SPACE_SPACE, space, index=_index)
        if len(array) > 1:
            raise SchemaError('Some strange output from server: \n'+array)
        elif len(array) == 0 or not len(array[0]):
            temp_name = ('name' if isinstance(space, basestring) else 'id')
            raise SchemaError('There\'s no space with {1} \'{0}\''.format(space,
                temp_name))
        array = array[0]
        return SchemaSpace(array, self.schema)

    def get_index(self, space, index):
        _index = self.find_local_index(space, index)
        if _index is not None:
            return _index
        space = self.get_space(space)
        _index = (const.INDEX_INDEX_NAME if isinstance(index, basestring) else const.INDEX_INDEX_PRIMARY)

        array = self.con.select(const.SPACE_INDEX, [space.sid, index], index=_index)
        if len(array) > 1:
            raise SchemaError('Some strange output from server: \n'+array)
        elif len(array) == 0 or not len(array[0]):
            temp_name = ('name' if isinstance(space, basestring) else 'id')
            raise SchemaError('There\'s no index with {2} \'{0}\' '
                    'in space \'{1}\''.format(index, space.name, temp_name))
        array = array[0]
        return SchemaIndex(array, space)

    def flush(self):
        self.schema.clear()
