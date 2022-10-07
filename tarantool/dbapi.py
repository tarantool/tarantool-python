"""
Python DB API implementation, refer to `PEP-249`_.

.. _PEP-249: http://www.python.org/dev/peps/pep-0249/
"""

from tarantool.connection import Connection as BaseConnection
from tarantool.error import *


paramstyle = 'named'
apilevel = "2.0"
threadsafety = 1


class Cursor:
    """
    Represent a database `cursor`_, which is used to manage the context
    of a fetch operation.

    .. _cursor: https://peps.python.org/pep-0249/#cursor-objects
    """

    def __init__(self, conn):
        """
        :param conn: Connection to a Tarantool server.
        :type conn: :class:`~tarantool.Connection`
        """

        self._c = conn
        self._lastrowid = None
        self._rowcount = None
        self.arraysize = 1
        self._rows = None

    def callproc(self, procname, *params):
        """
        **Not supported**

        Call a stored database procedure with the given name. The
        sequence of parameters must contain one entry for each argument
        that the procedure expects. The result of the call is returned
        as a modified copy of the input sequence. The input parameters
        are left untouched, the output and input/output parameters
        replaced with possibly new values.

        :raises: :exc:`~tarantool.error.NotSupportedError`
        """
        raise NotSupportedError("callproc() method is not supported")

    @property
    def rows(self):
        return self._rows

    @property
    def description(self):
        """
        **Not implemented**

        Call a stored database procedure with the given name. The
        sequence of parameters must contain one entry for each argument
        that the procedure expects. The result of the call is returned
        as a modified copy of the input sequence. The input parameters
        are left untouched, the output and input/output parameters
        replaced with possibly new values.

        :raises: :exc:`~NotImplementedError`
        """

        # FIXME Implement this method please
        raise NotImplementedError("description() property is not implemented")

    def close(self):
        """
        Close the cursor now (rather than whenever __del__ is called).
        The cursor will be unusable from this point forward;
        :exc:`~tarantool.error.InterfaceError` exception will be
        raised if any operation is attempted with the cursor.
        """

        self._c = None
        self._rows = None
        self._lastrowid = None
        self._rowcount = None

    def _check_not_closed(self, error=None):
        """
        Check that cursor is not closed. Raise
        :exc:`~tarantool.error.InterfaceError` otherwise.

        :param error: Custom error to be raised if cursor is closed.
        :type error: optional

        :raises: :exc:`~tarantool.error.InterfaceError`
        """
        if self._c is None:
            raise InterfaceError(error or "Can not operate on a closed cursor")
        if self._c.is_closed():
            raise InterfaceError("The cursor can not be used "
                                 "with a closed connection")

    def execute(self, query, params=None):
        """
        Execute an SQL request. Refer to
        :meth:`~tarantool.Connection.execute`.

        :param query: Refer to
            :paramref:`~tarantool.Connection.execute.params.query`

        :param params: Refer to
            :paramref:`~tarantool.Connection.execute.params.params`

        :raises: :exc:`~tarantool.error.InterfaceError`,
            :meth:`~tarantool.Connection.execute` exceptions
        """

        self._check_not_closed("Can not execute on closed cursor.")

        response = self._c.execute(query, params)

        self._rows = response.data
        self._rowcount = response.affected_row_count or -1
        if response.autoincrement_ids:
            self._lastrowid = response.autoincrement_ids[-1]
        else:
            self._lastrowid = None

    def executemany(self, query, param_sets):
        """
        Execute several SQL requests with same query and different
        parameters. Refer to :meth:`~tarantool.dbapi.Cursor.execute`.

        :param query: Refer to
            :paramref:`~tarantool.dbapi.Cursor.execute.params.query`.

        :param param_sets: Set of parameters for execution. Refer to
            :paramref:`~tarantool.dbapi.Cursor.execute.params.params`
            for item description.
        :type param sets: :obj:`list` or :obj:`tuple`

        :raises: :exc:`~tarantool.error.InterfaceError`,
            :meth:`~tarantool.dbapi.Cursor.execute` exceptions
        """

        self._check_not_closed("Can not execute on closed cursor.")
        rowcount = 0
        for params in param_sets:
            self.execute(query, params)
            if self.rowcount == -1:
                rowcount = -1
            if rowcount != -1:
                rowcount += self.rowcount
        self._rowcount = rowcount

    @property
    def lastrowid(self):
        """
        This read-only attribute provides the rowid of the last modified
        row (most databases return a rowid only when a single INSERT
        operation is performed).

        :type: :obj:`int`
        """

        return self._lastrowid

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the
        last ``.execute*()`` produced (for DQL statements like SELECT)
        or affected (for DML statements like UPDATE or INSERT).

        :type: :obj:`int`
        """

        return self._rowcount

    def _check_result_set(self, error=None):
        """
        Non-public method for raising an error when Cursor object does
        not have any row to fetch. Useful for checking access after DQL
        requests.

        :param error: Error to raise in case of fail.
        :type error: optional

        :raise: :exc:`~tarantool.error.InterfaceError`

        :meta private:
        """
        if self._rows is None:
            raise InterfaceError(error or "No result set to fetch from")

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        :raise: :exc:`~tarantool.error.InterfaceError`
        """

        self._check_result_set()
        return self.fetchmany(1)[0] if self._rows else None

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty sequence
        is returned when no more rows are available.

        :param size: Count of rows to fetch. If ``None``, fetch all.
        :type size: :obj:`int` or :obj:`None`, optional

        :raise: :exc:`~tarantool.error.InterfaceError`
        """

        self._check_result_set()

        size = size or self.arraysize

        if len(self._rows) < size:
            items = self._rows
            self._rows = []
        else:
            items, self._rows = self._rows[:size], self._rows[size:]

        return items

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as
        a sequence of sequences (e.g. a list of tuples). Note that
        the cursor's arraysize attribute can affect the performance of
        this operation.

        :raise: :exc:`~tarantool.error.InterfaceError`
        """

        self._check_result_set()

        items = self._rows
        self._rows = []
        return items

    def setinputsizes(self, sizes):
        """
        **Not implemented** (optional, refer to `PEP-249`_)

        Do nothing.
        """

    def setoutputsize(self, size, column=None):
        """
        **Not implemented** (optional, refer to `PEP-249`_)

        Do nothing.
        """


class Connection(BaseConnection):
    """
    `PEP-249`_ compatible :class:`~tarantool.Connection` class wrapper.
    """

    def __init__(self, *args, **kwargs):
        """
        :param args: :class:`~tarantool.Connection` args.
        :type args: :obj:`tuple`

        :param kwargs: :class:`~tarantool.Connection` kwargs.
        :type kwargs: :obj:`dict`

        :param autocommit: Enable or disable autocommit. Defaults to
            ``True``.
        :type autocommit: :obj:`bool`, optional

        :raise: :class:`~tarantool.Connection` exceptions
        """

        super(Connection, self).__init__(*args, **kwargs)
        self._set_autocommit(kwargs.get('autocommit', True))

    def _set_autocommit(self, autocommit):
        """
        Autocommit setter. ``False`` is not supported.

        :raise: :exc:`~tarantool.error.InterfaceError`,
            :exc:`~tarantool.error.NotSupportedError`
        """

        if not isinstance(autocommit, bool):
            raise InterfaceError("autocommit parameter must be boolean, "
                                 "not %s" % autocommit.__class__.__name__)
        if autocommit is False:
            raise NotSupportedError("The connector supports "
                                    "only autocommit mode")
        self._autocommit = autocommit

    @property
    def autocommit(self):
        """
        Autocommit state.
        """

        return self._autocommit

    @autocommit.setter
    def autocommit(self, autocommit):
        """
        Set autocommit state. ``False`` is not supported.

        :raise: :exc:`~tarantool.error.InterfaceError`,
            :exc:`~tarantool.error.NotSupportedError`
        """

        self._set_autocommit(autocommit)

    def _check_not_closed(self, error=None):
        """
        Checks if the connection is not closed and raises an error if it
        is.

        :param error: Error to raise in case of fail.
        :type error: optional

        :raise: :exc:`~tarantool.error.InterfaceError`
        """
        if self.is_closed():
            raise InterfaceError(error or "The connector is closed")

    def close(self):
        """
        Close the connection.

        :raise: :exc:`~tarantool.error.InterfaceError`
        """

        self._check_not_closed("The closed connector can not be closed again.")
        super(Connection, self).close()

    def commit(self):
        """
        Commit any pending transaction to the database.

        :raise: :exc:`~tarantool.error.InterfaceError`
        """

        self._check_not_closed("Can not commit on the closed connection")

    def rollback(self):
        """
        **Not supported**

        Roll back pending transaction.

        :raise: :exc:`~tarantool.error.InterfaceError`,
            :exc:`~tarantool.error.NotSupportedError`
        """

        self._check_not_closed("Can not roll back on a closed connection")
        raise NotSupportedError("Transactions are not supported in this"
                                "version of connector")

    def cursor(self):
        """
        Return a new Cursor object using the connection.

        :rtype: :class:`~tarantool.dbapi.Cursor`

        :raise: :exc:`~tarantool.error.InterfaceError`,
            :class:`~tarantool.dbapi.Cursor` exceptions
        """

        self._check_not_closed("Cursor creation is not allowed on a closed "
                               "connection")
        return Cursor(self)


def connect(dsn=None, host=None, port=None,
            user=None, password=None, **kwargs):
    """
    Constructor for creating a connection to the database.

    :param dsn: **Not implemented**. Tarantool server URI:
        ``[[[username[:password]@]host:]port``.
    :type dsn: :obj:`str`

    :param host: Refer to :paramref:`~tarantool.Connection.params.host`.

    :param port: Refer to :paramref:`~tarantool.Connection.params.port`.

    :param user: Refer to :paramref:`~tarantool.Connection.params.user`.

    :param password: Refer to
        :paramref:`~tarantool.Connection.params.password`.

    :rtype: :class:`~tarantool.Connection`

    :raise:  :exc:`~NotImplementedError`,
        :class:`~tarantool.Connection` exceptions
    """

    if dsn:
        raise NotImplementedError("dsn param is not implemented in"
                                  "this version of dbapi module")
    params = {}
    if host:
        params["host"] = host
    if port:
        params["port"] = port
    if user:
        params["user"] = user
    if password:
        params["password"] = password

    kwargs.update(params)

    return Connection(**kwargs)
