# -*- coding: utf-8 -*-
### pylint: disable=C0301,W0105,W0401,W0614
'''
This module provides object-oriented wrapper for accessing a particular Tarantool space
'''
import sys



class Space(object):
    '''\
    Object-oriented wrapper for accessing a particular space.
    Encapsulates the identifier of the space and provides more convenient syntax
    for database operations.
    '''
    def __init__(self, connection, space_no, field_types=None):
        if __debug__:
            if field_types and not all([(t is bytes) or (t is int) or (t is unicode) for t in field_types]):
                raise TypeError("Argument field_types can contain only bytes, int or %s"\
                                %('str' if sys.version_info.major > 2 else 'unicode'))

        self.connection = connection
        self.space_no = space_no
        self.field_types = field_types

    def insert(self, values, return_tuple=False):
        return self.connection.insert(self.space_no, values, return_tuple, self.field_types)

    def delete(self, key, return_tuple=False):
        return self.connection.delete(self.space_no, key, return_tuple, self.field_types)

    def update(self, key, op_list, return_tuple=False):
        return self.connection.update(self.space_no, key, op_list, return_tuple, self.field_types)

    def select(self, values, **kwargs):

        # Initialize arguments and its defaults from **kwargs
        # I use the explicit argument initialization from the kwargs
        # to make it impossible to pass positional arguments
        index = kwargs.get("index", 0)
        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", 0xffffffff)
        field_types = kwargs.get("field_types", self.field_types)

        return self.connection.select(self.space_no, values, index=index, offset=offset, limit=limit, field_types=field_types)

    def call(self, func_name, *args, **kwargs):
        return self.connection.call(func_name, *args, **kwargs)
