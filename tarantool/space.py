# pylint: disable=C0301,W0105,W0401,W0614
"""
Space type definition. It is an object-oriented wrapper for requests to
a Tarantool server space.
"""


class Space(object):
    """
    Object-oriented wrapper for accessing a particular space.
    Encapsulates the identifier of the space and provides a more
    convenient syntax for database operations.
    """

    def __init__(self, connection, space_name):
        """
        :param connection: Connection to the server.
        :type connection: :class:`~tarantool.Connection`

        :param space_name: Space name or space id to bind.
        :type space_name: :obj:`str` or :obj:`int`

        :raises: :meth:`~tarantool.schema.Schema.get_space` exceptions
        """

        self.connection = connection
        self.space_no = self.connection.schema.get_space(space_name).sid

    def insert(self, *args, **kwargs):
        """
        Refer to :meth:`~tarantool.Connection.insert`.
        """

        return self.connection.insert(self.space_no, *args, **kwargs)

    def replace(self, *args, **kwargs):
        """
        Refer to :meth:`~tarantool.Connection.replace`.
        """

        return self.connection.replace(self.space_no, *args, **kwargs)

    def delete(self, *args, **kwargs):
        """
        Refer to :meth:`~tarantool.Connection.delete`.
        """

        return self.connection.delete(self.space_no, *args, **kwargs)

    def update(self, *args, **kwargs):
        """
        Refer to :meth:`~tarantool.Connection.update`.
        """

        return self.connection.update(self.space_no, *args, **kwargs)

    def upsert(self, *args, **kwargs):
        """
        Refer to :meth:`~tarantool.Connection.upsert`.
        """

        return self.connection.upsert(self.space_no, *args, **kwargs)

    def select(self, *args, **kwargs):
        """
        Refer to :meth:`~tarantool.Connection.select`.
        """

        return self.connection.select(self.space_no, *args, **kwargs)

    def call(self, func_name, *args, **kwargs):
        """
        **Deprecated**, use :meth:`~tarantool.Connection.call` instead.
        """

        return self.connection.call(func_name, *args, **kwargs)
