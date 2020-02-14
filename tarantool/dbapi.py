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

    def execute(self, query, params=None):
        def convert_param(p):
            print('PARAM: ', p)
            if isinstance(p, bool):
                return str(p)
            elif p is None:
                return "NULL"
            return "'%s'" % p

        if params:
            query = query % tuple(convert_param(param) for param in params)

        print(query)
        response = self._c.execute(query)

        if len(response.body) > 1:
            self.rows = tuple(response.body.values())[1]
        else:
            self.rows = []

        rc_pattern = re.compile(r'^(UPDATE|INSERT)')
        if rc_pattern.match(query):
            try:
                self._rowcount = response.rowcount
            except InterfaceError:
                pass
        else:
            self._rowcount = 1
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
        self._lastrowid += size
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
