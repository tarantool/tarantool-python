# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
This module provides :class:`~tarantool.space.Space` class.
It is an object-oriented wrapper for request over Tarantool space.
'''


class Space(object):

    '''\
    Object-oriented wrapper for accessing a particular space.
    Encapsulates the identifier of the space and provides more convenient
    syntax for database operations.
    '''

    def __init__(self, connection, space_name):
        '''\
        Create Space instance.

        :param connection: Object representing connection to the server
        :type connection: :class:`~tarantool.connection.Connection` instance
        :param int space_no: space no or name to insert a record
        :type space_name: int or str
        '''

        self.connection = connection
        self.space_no = connection.schema.space_no(space_name)

    def insert(self, values, return_tuple=False):
        '''\
        Insert single record into the space.

        :param values: record to be inserted. The tuple must contain only
            scalar (integer or strings) values
        :type values: tuple
        :param return_tuple: True indicates that it is required to return
            the inserted tuple back
        :type return_tuple: boo

        :rtype: :class:`~tarantool.response.Response` instance
        '''
        return self.connection.insert(self.space_no, values, return_tuple)

    def delete(self, key, return_tuple=False):
        '''
        Delete records by its primary key.

        :param key: key of records to be deleted
        :type values: tuple or str or int or long
        :param return_tuple: True indicates that it is required to return
            the inserted tuple back
        :type return_tuple: bool

        :rtype: :class:`~tarantool.response.Response` instance
        '''
        return self.connection.delete(self.space_no, key, return_tuple)

    def update(self, key, op_list, return_tuple=False):
        '''
        Update records by it's primary key with operations defined in op_list

        :param key: key of records to be updated
        :type values: tuple or str or int or long
        :param return_tuple: True indicates that it is required to return
            the inserted tuple back
        :type return_tuple: bool

        :rtype: :class:`~tarantool.response.Response` instance
        '''
        return self.connection.update(
            self.space_no, key, op_list, return_tuple)

    def select(self, values, **kwargs):
        '''\
        Execute SELECT request.
        Select and retrieve data from the database.

        :param values: list of values to search over the index
        :type values: list of tuples
        :param index: specifies which index to use (default is **0** which
            means that the **primary index** will be used)
        :type index: int
        :param offset: offset in the resulting tuple set
        :type offset: int
        :param limit: limits the total number of returned tuples
        :type limit: int

        :rtype: `Response` instance
        '''
        # Initialize arguments and its defaults from **kwargs
        # I use the explicit argument initialization from the kwargs
        # to make it impossible to pass positional arguments
        index = kwargs.get("index", 0)
        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", 0xffffffff)

        return self.connection.select(
            self.space_no, values, index=index, offset=offset, limit=limit)

    def call(self, func_name, *args, **kwargs):
        '''\
        Execute CALL request. Call stored Lua function.

        :param func_name: stored Lua function name
        :type func_name: str
        :param args: list of function arguments
        :type args: list or tuple
        :param return_tuple: True indicates that it is required to return
            the inserted tuple back
        :type return_tuple: bool
        :param field_defs: field definitions used for types conversion,
               e.g. [('field0', tarantool.NUM), ('field1', tarantool.STR)]
        :type field_defs: None or  [(name, type) or None]
        :param default_type: None a default type used for result conversion,
            as defined in ``schema[space_no]['default_type']``
        :type default_type: None or int
        :param space_name: space number or name. A schema for the space
            will be used for type conversion.
        :type space_name: None or int or str

        :rtype: `Response` instance
        '''
        return self.connection.call(func_name, *args, **kwargs)
