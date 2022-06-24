# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
This module provides the :class:`~tarantool.space.Space` class.
It is an object-oriented wrapper for requests to a Tarantool space.
'''


class Space(object):
    '''
    Object-oriented wrapper for accessing a particular space.
    Encapsulates the identifier of the space and provides a more convenient
    syntax for database operations.
    '''

    def __init__(self, connection, space_name):
        '''
        Create Space instance.

        :param connection: object representing connection to the server
        :type connection: :class:`~tarantool.connection.Connection` instance
        :param int space_name: space no or name to insert a record
        :type space_name: int or str
        '''

        self.connection = connection
        self.space_no = self.connection.schema.get_space(space_name).sid

    def insert(self, *args, **kwargs):
        '''
        Execute an INSERT request.

        See `~tarantool.connection.insert` for more information.
        '''
        return self.connection.insert(self.space_no, *args, **kwargs)

    def replace(self, *args, **kwargs):
        '''
        Execute a REPLACE request.

        See `~tarantool.connection.replace` for more information.
        '''
        return self.connection.replace(self.space_no, *args, **kwargs)

    def delete(self, *args, **kwargs):
        '''
        Execute a DELETE request.

        See `~tarantool.connection.delete` for more information.
        '''
        return self.connection.delete(self.space_no, *args, **kwargs)

    def update(self, *args, **kwargs):
        '''
        Execute an UPDATE request.

        See `~tarantool.connection.update` for more information.
        '''
        return self.connection.update(self.space_no, *args, **kwargs)

    def upsert(self, *args, **kwargs):
        '''
        Execute an UPDATE request.

        See `~tarantool.connection.upsert` for more information.
        '''
        return self.connection.upsert(self.space_no, *args, **kwargs)

    def select(self, *args, **kwargs):
        '''
        Execute a SELECT request.

        See `~tarantool.connection.select` for more information.
        '''
        return self.connection.select(self.space_no, *args, **kwargs)

    def call(self, func_name, *args, **kwargs):
        '''
        Execute a CALL request. Call a stored Lua function.

        Deprecated, use `~tarantool.connection.call` instead.
        '''
        return self.connection.call(func_name, *args, **kwargs)
