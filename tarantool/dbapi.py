"""
Supports python 3.6 and above
"""
import re
from copy import deepcopy

from tarantool.error import InterfaceError

from .connection import Connection as BaseConnection


class Cursor:
    _lastrowid = 0
    description = None
    arraysize = None
    rows = []
    position = 0
    autocommit = True
    _rowcount = 0

    def __init__(self, connection):
        self._c = connection

    def callproc(self, procname, *params):
        pass

    def close(self):
        self._c.close()

    def _convert_param(self, p):
        if isinstance(p, bool):
            return str(p)
        elif p is None:
            return "NULL"
        return "'%s'" % p

    def execute(self, query, params=None):
        if params:
            query = query % tuple(self._convert_param(param) for param in params)

        # print(query)
        response = self._c.execute(query)

        self.rows = tuple(response.body.values())[1] if len(response.body) > 1 else []

        rc_pattern = re.compile(r'^(UPDATE|INSERT)')
        if rc_pattern.match(query):
            try:
                self._rowcount = response.rowcount
            except InterfaceError:
                pass
        else:
            self._rowcount = 1

        def extract_last_row_id(body):  # Need to be checked
            try:
                val = tuple(tuple(body.items())[0][-1].items())[-1][-1][0]
            except TypeError:
                val = 1
            return val

        u_pattern = re.compile(r'^INSERT')
        if u_pattern.match(query):
            self._lastrowid = extract_last_row_id(response.body)
        return response

    @property
    def lastrowid(self):
        return self._lastrowid

    @property
    def rowcount(self):
        return self._rowcount

    def executemany(self, query, params):
        return self.execute(query, params)

    def fetchone(self):
        pass

    def fetchmany(self, size):
        items = deepcopy(self.rows)
        self.rows = []
        return items

    def fetchall(self):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


class Connection(BaseConnection):
    rows = []
    _cursor = None

    server_version = 1

    def commit(self):
        pass

    def close(self):
        '''
        Close connection to the server
        '''
        if self._socket:
            self._socket.close()
            self._socket = None

    def rollback(self):
        pass

    @classmethod
    def cache(cls, cursor):
        cls._cursor = cursor

    def cursor(self, params=None):
        if not self._cursor:
            self._cursor = Cursor(self)
        return self._cursor
