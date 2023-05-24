"""
This module tests compatibility with DBAPI standards.
"""
# pylint: disable=missing-class-docstring,missing-function-docstring,protected-access,fixme

import sys
import unittest

import dbapi20

import tarantool
from tarantool import dbapi
from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_sql_test


class TestSuiteDBAPI(dbapi20.DatabaseAPI20Test):
    table_prefix = 'dbapi20test_'  # If you need to specify a prefix for tables

    ddl0 = f'create table {table_prefix} (id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
           'name varchar(20))'
    ddl1 = f'create table {table_prefix}booze (name varchar(20) primary key)'
    ddl2 = f'create table {table_prefix}barflys (name varchar(20) primary key, ' \
           'drink varchar(30))'

    @classmethod
    def setUpClass(cls):
        print(' DBAPI '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        # Select scans are not allowed with compat.sql_seq_scan_default = "new",
        # but tests create cursors with fullscan.
        cls.srv = TarantoolServer(sql_seq_scan_default="old")
        cls.srv.script = 'test/suites/box.lua'
        cls.srv.start()
        cls.con = tarantool.Connection(cls.srv.host, cls.srv.args['primary'])
        cls.driver = dbapi
        cls.connect_kw_args = {
            "host": cls.srv.host,
            "port": cls.srv.args['primary']
        }

    @skip_or_run_sql_test
    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()
        self.con.flush_schema()

        # grant full access to guest
        self.srv.admin("box.schema.user.grant('guest', 'create,read,write,"
                       "execute', 'universe')")

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        cls.srv.stop()
        cls.srv.clean()

    def help_nextset_setUp(self, cur):
        # pylint: disable=unused-argument
        pass

    def help_nextset_tearDown(self, cur):
        # pylint: disable=unused-argument
        pass

    def test_rowcount(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            dbapi20._failUnless(
                self, cur.rowcount in (-1, 1),
                'cursor.rowcount should be -1 or 1 after executing no-result '
                'statements' + str(cur.rowcount)
            )
            cur.execute(f"{self.insert} into {self.table_prefix}booze values ('Victoria Bitter')")
            dbapi20._failUnless(
                self, cur.rowcount == 1,
                'cursor.rowcount should == number or rows inserted, or '
                'set to -1 after executing an insert statement'
            )
            cur.execute(f"select name from {self.table_prefix}booze")
            dbapi20._failUnless(
                self, cur.rowcount == -1,
                'cursor.rowcount should == number of rows returned, or '
                'set to -1 after executing a select statement'
            )
            self.executeDDL2(cur)
            dbapi20._failUnless(
                self, cur.rowcount in (-1, 1),
                'cursor.rowcount should be -1 or 1 after executing no-result '
                'statements'
            )
        finally:
            con.close()

    @unittest.skip('Not implemented')
    def test_Binary(self):
        pass

    @unittest.skip('Not implemented')
    def test_STRING(self):
        pass

    @unittest.skip('Not implemented')
    def test_BINARY(self):
        pass

    @unittest.skip('Not implemented')
    def test_NUMBER(self):
        pass

    @unittest.skip('Not implemented')
    def test_DATETIME(self):
        pass

    @unittest.skip('Not implemented')
    def test_ROWID(self):
        pass

    @unittest.skip('Not implemented')
    def test_Date(self):
        pass

    @unittest.skip('Not implemented')
    def test_Time(self):
        pass

    @unittest.skip('Not implemented')
    def test_Timestamp(self):
        pass

    @unittest.skip('Not implemented as optional.')
    def test_nextset(self):
        pass

    @unittest.skip('Not implemented')
    def test_callproc(self):
        pass

    def test_setoutputsize(self):  # Do nothing
        pass

    @unittest.skip('Not implemented')
    def test_description(self):
        pass
