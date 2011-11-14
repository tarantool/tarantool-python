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
    def __init__(self, connection, space_no):
        self.connection = connection
        self.space_no = space_no

    def insert(self, values, return_tuple=False):
        return self.connection.insert(self.space_no, values, return_tuple)

    def delete(self, key, return_tuple=False):
        return self.connection.delete(self.space_no, key, return_tuple)

    def update(self, key, op_list, return_tuple=False):
        return self.connection.update(self.space_no, key, op_list, return_tuple)

    def select(self, index_no, values, offset=0, limit=0xffffffff):
        return self.connection.select(self.space_no, index_no, values, offset, limit)
