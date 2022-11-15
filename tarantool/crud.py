"""
This module provides API for interaction with the `crud`_ module.

.. _crud: https://github.com/tarantool/crud/
"""

from tarantool.error import DatabaseError, ER_NO_SUCH_PROC, ER_ACCESS_DENIED


class CrudResponse(object):
    """
    Contains response fields from the `crud`_ module that correspond 
    to the Lua implementation.

    .. _crud: https://github.com/tarantool/crud/
    """

    def __init__(self, response):
        """
        Sets response fields as in Lua implementation.

        :param response: The response object of the crud module call.
        :type response: :class:`~tarantool.response.Response`
        """

        if isinstance(response, dict):
            for response_field_name in response.keys():
                if isinstance(response_field_name, bytes):
                    setattr(self, response_field_name.decode(), response[response_field_name])
                else:
                    setattr(self, response_field_name, response[response_field_name])
        else:
            raise RuntimeError('Unable to decode response to object due to unknown type')


class CrudResult(CrudResponse):
    """
    Contains result's fields from result variable 
    of crud module operation.
    """


class CrudError(CrudResponse):
    """
    Contains error's fields from error variable 
    of crud module operation.
    """


def call_crud(conn, *args):
    """
    Calls the crud via connection.call with try/except block.

    :param conn: The connection object for the crud module call.
    :type conn: :class:`~tarantool.connection.Connection`

    :param args: The method name and args for the crud method.
    :type args: :obj:`tuple`

    :raise: :exc:`~tarantool.error.DatabaseError`

    :meta private:
    """

    try:
        crud_resp = conn.call(*args)
    except DatabaseError as e:
        if e.code == ER_NO_SUCH_PROC or e.code == ER_ACCESS_DENIED:
            exc_msg = ". Ensure that you're calling crud.router and user has sufficient grants"
            raise DatabaseError(e.code, e.message + exc_msg, extra_info=e.extra_info) from e

    return crud_resp
