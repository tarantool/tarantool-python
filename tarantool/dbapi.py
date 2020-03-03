"""
Supports python 3.6 and above
"""
import re
from copy import deepcopy

from tarantool.error import InterfaceError

from .connection import Connection as BaseConnection


class Cursor:
    _lastrowid = 0
    _rowcount = 0
    description = None
    position = 0
    arraysize = 200
    autocommit = True
    ui_pattern = re.compile(r'^(UPDATE|INSERT)')
    u_pattern = re.compile(r'^INSERT')

    def __init__(self, connection):
        self._c = connection
        self.rows = []

    def callproc(self, procname, *params):
        pass

    def close(self):  # TODO: Find out how to implement closing connection correctly
        pass

    @staticmethod
    def _convert_param(p):
        if p is None:
            return "NULL"
        if isinstance(p, bool):
            return str(p)
        return "'%s'" % p

    @staticmethod
    def _extract_last_row_id(body):  # Need to be checked
        try:
            val = tuple(tuple(body.items())[0][-1].items())[-1][-1][0]
        except TypeError:
            val = -1
        return val

    def execute(self, query, params=None):
        if params:
            query = query % tuple(self._convert_param(param) for param in params)

        response = self._c.execute(query)

        self.rows = tuple(response.body.values())[1] if len(response.body) > 1 else []

        if self.ui_pattern.match(query):
            try:
                self._rowcount = response.rowcount
            except InterfaceError:
                self._rowcount = 1
        else:
            self._rowcount = 1

        if self.u_pattern.match(query):
            self._lastrowid = self._extract_last_row_id(response.body)
        return response

    def executemany(self, query, params):
        return self.execute(query, params)

    @property
    def lastrowid(self):
        return self._lastrowid

    @property
    def rowcount(self):
        return self._rowcount

    def fetchone(self):
        return self.rows[0] if len(self.rows) else None

    def fetchmany(self, size):
        if len(self.rows) < size:
            items = self.rows
            self.rows = []
        else:
            items, self.rows = self.rows[:size], self.rows[size:]

        return items if len(items) else []

    def fetchall(self):
        items = deepcopy(self.rows)
        self.rows = []
        return items

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


class Connection(BaseConnection):
    _cursor = None

    server_version = 2

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        if self._socket:
            self._socket.close()
            self._socket = None

    def _set_cursor(self):
        self._cursor = Cursor(self)
        return self._cursor

    def cursor(self, params=None):
        return self._cursor or self._set_cursor()
