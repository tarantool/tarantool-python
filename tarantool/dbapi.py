"""
https://www.python.org/dev/peps/pep-0249/
"""
import re

from tarantool.error import InterfaceError
from .connection import Connection as BaseConnection

update_insert_pattern = re.compile(r'^UPDATE|INSERT')


class Cursor:
    _lastrowid = 0
    _rowcount = 0
    description = None
    position = 0
    arraysize = 200
    autocommit = True

    def __init__(self, connection):
        self._c = connection
        self.rows = []

    def callproc(self, procname, *params):  # TODO
        """
        Call a stored database procedure with the given name. The sequence of
        parameters must contain one entry for each argument that the
        procedure expects. The result of the call is returned as modified
        copy of the input sequence. Input parameters are left untouched,
        output and input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then
        be made available through the standard .fetch*() methods.
        """

    def close(self):  # TODO
        """
        Close the cursor now (rather than whenever __del__ is called).

        The cursor will be unusable from this point forward; an Error (or
        subclass) exception will be raised if any operation is attempted with
        the cursor.
        """

    @staticmethod
    def _convert_param(p):
        if p is None:
            return "NULL"
        if isinstance(p, bool):
            return str(p)
        return "'%s'" % p

    @staticmethod
    def _extract_last_row_id(body):  # TODO: Need to be checked
        try:
            val = tuple(tuple(body.items())[0][-1].items())[-1][-1][0]
        except TypeError:
            val = -1
        return val

    def execute(self, query, params=None):
        """
        Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound
        to variables in the operation. Variables are specified in a
        database-specific notation (see the module's paramstyle attribute for
        details).

        A reference to the operation will be retained by the cursor. If the
        same operation object is passed in again, then the cursor can
        optimize its behavior. This is most effective for algorithms where
        the same operation is used, but different parameters are bound to it
        (many times).

        For maximum efficiency when reusing an operation, it is best to use
        the .setinputsizes() method to specify the parameter types and sizes
        ahead of time. It is legal for a parameter to not match the
        predefined information; the implementation should compensate,
        possibly with a loss of efficiency.

        The parameters may also be specified as list of tuples to e.g. insert
        multiple rows in a single operation, but this kind of usage is
        deprecated: .executemany() should be used instead.

        Return values are not defined.
        """
        if params:
            query = query % tuple(
                self._convert_param(param) for param in params)

        response = self._c.execute(query)

        self.rows = tuple(response.body.values())[1] if len(
            response.body) > 1 else []

        if update_insert_pattern.match(query.upper()):
            try:
                self._rowcount = response.rowcount
            except InterfaceError:
                self._rowcount = 1
        else:
            self._rowcount = 1

        if query.upper().startswith('INSERT'):
            self._lastrowid = self._extract_last_row_id(response.body)
        return response

    def executemany(self, query, params):
        return self.execute(query, params)

    @property
    def lastrowid(self):
        """
        This read-only attribute provides the rowid of the last modified row
        (most databases return a rowid only when a single INSERT operation is
        performed). If the operation does not set a rowid or if the database
        does not support rowids, this attribute should be set to None.

        The semantics of .lastrowid are undefined in case the last executed
        statement modified more than one row, e.g. when using INSERT with
        .executemany().

        Warning Message: "DB-API extension cursor.lastrowid used"
        """
        return self._lastrowid

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the last
        .execute*() produced (for DQL statements like SELECT) or affected (
        for DML statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute*() has been performed on the
        cursor or the rowcount of the last operation is cannot be determined
        by the interface.

        Note:
        Future versions of the DB API specification could redefine the latter
        case to have the object return None instead of -1.
        """
        return self._rowcount

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        return self.fetchmany(1)[0] if len(self.rows) else None

    def fetchmany(self, size):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.

        The number of rows to fetch per call is specified by the parameter.
        If it is not given, the cursor's arraysize determines the number of
        rows to be fetched. The method should try to fetch as many rows as
        indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be
        returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        .arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the next.
        """
        if len(self.rows) < size:
            items = self.rows
            self.rows = []
        else:
            items, self.rows = self.rows[:size], self.rows[size:]

        return items if len(items) else []

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """
        items = self.rows[:]
        self.rows = []
        return items

    def setinputsizes(self, sizes):
        """This can be used before a call to .execute*() to predefine memory
        areas for the operation's parameters.

        sizes is specified as a sequence — one item for each input parameter.
        The item should be a Type Object that corresponds to the input that
        will be used, or it should be an integer specifying the maximum
        length of a string parameter. If the item is None, then no predefined
        memory area will be reserved for that column (this is useful to avoid
        predefined areas for large inputs).

        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are
        free to not use it."""

    def setoutputsize(self, size, column=None):
        pass


class Connection(BaseConnection):
    _cursor = None

    server_version = 2

    def commit(self):  # TODO
        """
        Commit any pending transaction to the database.

        Note that if the database supports an auto-commit feature, this must
        be initially off. An interface method may be provided to turn it back
        on.

        Database modules that do not support transactions should implement
        this method with void functionality.
        """

    def rollback(self):
        """
        In case a database does provide transactions this method causes the
        database to roll back to the start of any pending transaction.
        Closing a connection without committing the changes first will cause
        an implicit rollback to be performed.
        """

    def close(self):
        """
        Close the connection now (rather than whenever .__del__() is called).

        The connection will be unusable from this point forward; an Error (or
        subclass) exception will be raised if any operation is attempted with
        the connection. The same applies to all cursor objects trying to use
        the connection. Note that closing a connection without committing the
        changes first will cause an implicit rollback to be performed.
        """
        if self._socket:
            self._socket.close()
            self._socket = None

    def _set_cursor(self):
        self._cursor = Cursor(self)
        return self._cursor

    def cursor(self, params=None):
        """
        Return a new Cursor Object using the connection.

        If the database does not provide a direct cursor concept, the module
        will have to emulate cursors using other means to the extent needed
        by this specification.
        """
        return self._cursor or self._set_cursor()
