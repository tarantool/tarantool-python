# -*- coding: utf-8 -*-
### pylint: disable=C0301,W0105,W0401,W0614
'''
This module provides object-oriented wrapper for accessing a particular Tarantool space
'''


class Space(object):
    '''\
    Object-oriented wrapper for accessing a particular space.
    Encapsulates the identifier of the space and provides more convenient syntax
    for database operations.
    '''
    def __init__(self, connection, space_no, field_types=None):
        if __debug__:
            if not all([(type(t) is type) or (t is None) or (t is any) for t in field_types]):
                # FIXME: [py3] explicit unicode usage
                raise TypeError, "Argument field_types can contain only any, bytes, int, unicode or None"
        self.connection = connection
        self.space_no = space_no
        self.field_types = field_types

    def insert(self, values, return_tuple=False):
        return self.connection.insert(self.space_no, values, return_tuple, self.field_types)

    def delete(self, key, return_tuple=False):
        return self.connection.delete(self.space_no, key, return_tuple, self.field_types)

    def update(self, key, op_list, return_tuple=False):
        return self.connection.update(self.space_no, key, op_list, return_tuple, self.field_types)

    def select(self, index_no, values, offset=0, limit=0xffffffff):
        return self.connection.select(self.space_no, index_no, values, offset, limit, self.field_types)
