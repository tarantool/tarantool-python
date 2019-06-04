# -*- coding: utf-8 -*-


class Cursor(object):

    description = None
    rowcount = None
    arraysize = None

    def __init__(self):
        pass

    def callproc(self, procname, *params):
        pass
    
    def close(self):
        pass
    
    def execute(self, query, params):
        pass
    
    def executemany(self, query, params):
        pass
    
    def fetchone(self):
        pass

    def fetchmany(self):
        pass
    
    def fetchall(self):
        pass

    def setinputsizes(self, sizes):
        pass
    
    def setoutputsize(self, size, column=None):
        pass

class Connection(object):

    def __init__(self):
        pass
    
    def close(self):
        pass
    
    def commit(self):
        pass

    def rollback(self):
        pass
    
    def cursor(self):
        pass