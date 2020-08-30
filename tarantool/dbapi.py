# -*- coding: utf-8 -*-
from tarantool.connection import Connection as BaseConnection
from tarantool.error import *


paramstyle = 'named'
apilevel = "2.0"
threadsafety = 1


class Cursor:

    def __init__(self, conn):
        self._c = conn
        self._lastrowid = None
        self._rowcount = None
        self.arraysize = 1
        self._rows = None

    def callproc(self, procname, *params):
        """
        Call a stored database procedure with the given name. The sequence of
        parameters must contain one entry for each argument that the
        procedure expects. The result of the call is returned as modified
        copy of the input sequence. Input parameters are left untouched,
        output and input/output parameters replaced with possibly new values.
        """
        raise NotSupportedError("callproc() method is not supported")

    @property
    def rows(self):
        return self._rows

    @property
    def description(self):
        # FIXME Implement this method please
        raise NotImplementedError("description() property is not implemented")

    def close(self):
        """
        Close the cursor now (rather than whenever __del__ is called).
        The cursor will be unusable from this point forward; DatabaseError
        exception will be raised if any operation is attempted with
        the cursor.
        """
        self._c = None
        self._rows = None
        self._lastrowid = None
        self._rowcount = None

    def _check_not_closed(self, error=None):
        if self._c is None:
            raise InterfaceError(error or "Can not operate on a closed cursor")
        if self._c.is_closed():
            raise InterfaceError("The cursor can not be used "
                                 "with a closed connection")

    def execute(self, query, params=None):
        """
        Prepare and execute a database operation (query or command).
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
        This read-only attribute provides the rowid of the last modified row
        (most databases return a rowid only when a single INSERT operation is
        performed).
        """
        return self._lastrowid

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the last
        .execute*() produced (for DQL statements like SELECT) or affected (
        for DML statements like UPDATE or INSERT).
        """
        return self._rowcount

    def _check_result_set(self, error=None):
        """
        Non-public method for raising an error when Cursor object does not have
        any row to fetch. Useful for checking access after DQL requests.
        """
        if self._rows is None:
            raise InterfaceError(error or "No result set to fetch from")

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.
        """
        self._check_result_set()
        return self.fetchmany(1)[0] if self._rows else None

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
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
        """Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        self._check_result_set()

        items = self._rows
        self._rows = []
        return items

    def setinputsizes(self, sizes):
        """PEP-249 allows to not implement this method and do nothing."""

    def setoutputsize(self, size, column=None):
        """PEP-249 allows to not implement this method and do nothing."""


class Connection(BaseConnection):

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self._set_autocommit(kwargs.get('autocommit', True))

    def _set_autocommit(self, autocommit):
        """Autocommit is True by default and the default will be changed
        to False. Set the autocommit property explicitly to True or verify
        it when lean on autocommit behaviour."""
        if not isinstance(autocommit, bool):
            raise InterfaceError("autocommit parameter must be boolean, "
                                 "not %s" % autocommit.__class__.__name__)
        if autocommit is False:
            raise NotSupportedError("The connector supports "
                                    "only autocommit mode")
        self._autocommit = autocommit

    @property
    def autocommit(self):
        """Autocommit state"""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, autocommit):
        """Set autocommit state"""
        self._set_autocommit(autocommit)

    def _check_not_closed(self, error=None):
        """
        Checks if the connection is not closed and rises an error if it is.
        """
        if self.is_closed():
            raise InterfaceError(error or "The connector is closed")

    def close(self):
        """
        Closes the connection
        """
        self._check_not_closed("The closed connector can not be closed again.")
        super(Connection, self).close()

    def commit(self):
        """
        Commit any pending transaction to the database.
        """
        self._check_not_closed("Can not commit on the closed connection")

    def rollback(self):
        """
        Roll back pending transaction
        """
        self._check_not_closed("Can not roll back on a closed connection")
        raise NotSupportedError("Transactions are not supported in this"
                                "version of connector")

    def cursor(self):
        """
        Return a new Cursor Object using the connection.
        """
        self._check_not_closed("Cursor creation is not allowed on a closed "
                               "connection")
        return Cursor(self)


def connect(dsn=None, host=None, port=None,
            user=None, password=None, **kwargs):
    """
    Constructor for creating a connection to the database.

    :param str dsn: Data source name (Tarantool URI)
                    ([[[username[:password]@]host:]port)
    :param str host: Server hostname or IP-address
    :param int port: Server port
    :param str user: Tarantool user
    :param str password: User password
    :rtype: Connection
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
