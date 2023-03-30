"""
This module tests API for running SQL on a server.
"""
# pylint: disable=missing-class-docstring,missing-function-docstring,duplicate-code

import sys
import unittest

import tarantool
from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_sql_test


class TestSuiteExecute(unittest.TestCase):
    ddl = 'create table %s (id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
          'name varchar(20))'

    dml_params = [
        {'id': None, 'name': 'Michael'},
        {'id': None, 'name': 'Mary'},
        {'id': None, 'name': 'John'},
        {'id': None, 'name': 'Ruth'},
        {'id': None, 'name': 'Rachel'}
    ]

    @classmethod
    def setUpClass(cls):
        print(' EXECUTE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        cls.srv = TarantoolServer()
        cls.srv.script = 'test/suites/box.lua'
        cls.srv.start()
        cls.con = tarantool.Connection(cls.srv.host, cls.srv.args['primary'])

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

    def _populate_data(self, table_name):
        query = f"insert into {table_name} values (:id, :name)"
        for param in self.dml_params:
            self.con.execute(query, param)

    def _create_table(self, table_name):
        return self.con.execute(self.ddl % table_name)

    def test_dml_response(self):
        table_name = 'foo'
        response = self._create_table(table_name)
        self.assertEqual(response.autoincrement_ids, None)
        self.assertEqual(response.affected_row_count, 1)
        self.assertEqual(response.data, None)

        query = f"insert into {table_name} values (:id, :name)"

        for num, param in enumerate(self.dml_params, start=1):
            response = self.con.execute(query, param)
            self.assertEqual(response.autoincrement_ids[0], num)
            self.assertEqual(response.affected_row_count, 1)
            self.assertEqual(response.data, None)

        query = f"delete from {table_name} where id in (4, 5)"
        response = self.con.execute(query)
        self.assertEqual(response.autoincrement_ids, None)
        self.assertEqual(response.affected_row_count, 2)
        self.assertEqual(response.data, None)

    def test_dql_response(self):
        table_name = 'bar'
        self._create_table(table_name)
        self._populate_data(table_name)

        select_query = f"select name from {table_name} where id in (1, 3, 5)"
        response = self.con.execute(select_query)
        self.assertEqual(response.autoincrement_ids, None)
        self.assertEqual(response.affected_row_count, None)
        expected_data = [['Michael'], ['John'], ['Rachel']]
        self.assertListEqual(response.data, expected_data)
