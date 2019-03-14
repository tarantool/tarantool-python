# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import unittest
import tarantool
from .lib.tarantool_server import TarantoolServer

class TestSuite_Schema(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' SCHEMA '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'unit/suites/box.lua'
        self.srv.start()
        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'])
        self.sch = self.con.schema

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

    def test_00_authenticate(self):
        self.assertIsNone(self.srv.admin("box.schema.user.create('test', { password = 'test' })"))
        self.assertIsNone(self.srv.admin("box.schema.user.grant('test', 'read,write', 'space', '_space')"))
        self.assertIsNone(self.srv.admin("box.schema.user.grant('test', 'read,write', 'space', '_index')"))
        self.assertEqual(self.con.authenticate('test', 'test')._data, None)

    def test_01_space_bad(self):
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_space(0)
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_space(0)
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_space('bad_name')

    def test_02_index_bad(self):
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_index(0, 'primary')
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_index('bad_space', 'primary')
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no index.*'):
            self.sch.get_index(280, 'bad_index')
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no index.*'):
            self.sch.get_index(280, 'bad_index')
        with self.assertRaisesRegexp(tarantool.SchemaError,
                'There\'s no index.*'):
            self.sch.get_index(280, 3)

    def test_03_01_space_name__(self):
        self.con.flush_schema()
        space = self.sch.get_space('_schema')
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_space')
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_index')
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

    def test_03_02_space_number(self):
        self.con.flush_schema()
        space = self.sch.get_space(272)
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space(280)
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space(288)
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

    def test_04_space_cached(self):
        space = self.sch.get_space('_schema')
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_space')
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_index')
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

    def test_05_01_index_name___name__(self):
        self.con.flush_schema()
        index = self.sch.get_index('_index', 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 'name')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_space', 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index('_space', 'name')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

    def test_05_02_index_name___number(self):
        self.con.flush_schema()
        index = self.sch.get_index('_index', 0)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_space', 0)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index('_space', 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

    def test_05_03_index_number_name__(self):
        self.con.flush_schema()
        index = self.sch.get_index(288, 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(288, 'name')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 'name')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

    def test_05_04_index_number_number(self):
        self.con.flush_schema()
        index = self.sch.get_index(288, 0)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(288, 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 0)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

    def test_06_index_cached(self):
        index = self.sch.get_index('_index', 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

    def test_07_schema_version_update(self):
        _space_len = len(self.con.select('_space'))
        self.srv.admin("box.schema.create_space('ttt22')")
        self.assertEqual(len(self.con.select('_space')), _space_len + 1)

    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()
