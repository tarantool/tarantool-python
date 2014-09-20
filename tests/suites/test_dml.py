# -*- coding: utf-8 -*-
import yaml
import unittest
import tarantool
from lib.tarantool_server import TarantoolServer

class Request(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print ' DML '.center(70, '=')
        print '-' * 70
        self.srv = TarantoolServer()
        self.srv.script = 'tests/suites/box.lua'
        self.srv.start()
        self.con = tarantool.Connection('localhost', self.srv.args['primary'])
        self.adm = self.srv.admin
        self.space_created = self.adm("box.schema.create_space('space_1')")
        self.adm("box.space['space_1']:create_index('primary', {type = 'tree', parts = {1, 'num'}, unique = true})")
        self.adm("box.space['space_1']:create_index('secondary', {type = 'tree', parts = {2, 'num', 3, 'str'}, unique = true})")
        self.adm("json = require('json')")
        self.adm("fiber = require('fiber')")
        self.adm("uuid = require('uuid')")

    def test_00_00_authenticate(self):
        self.assertIsNone(self.srv.admin("box.schema.user.create('test', { password = 'test' })"))
        self.assertIsNone(self.srv.admin("box.schema.user.grant('test', 'execute,read,write', 'universe')"))
        self.assertEqual(self.con.authenticate('test', 'test'), [])

    def test_00_01_space_created(self):
        # Check that space is created in setUpClass
        self.assertEqual(self.space_created[1], 'created')

    def test_00_02_fill_space(self):
        # Fill space with values
        for i in xrange(1, 500):
            self.assertEqual(
                    self.con.insert('space_1', [i, i%5, 'tuple_'+str(i)])[0],
                    (i, i%5, 'tuple_'+str(i))
            )
    def test_00_03_answer_repr(self):
        repr_str = \
'''---
- [1, 1, 'tuple_1']
...'''
        self.assertEqual(repr(self.con.select('space_1', 1)), repr_str)

    def test_02_select(self):
        # Check that select with different keys are Ok. (With and without index names)
        self.assertEqual(self.con.select('space_1', 20), [(20, 0, 'tuple_20')])
        self.assertEqual(self.con.select('space_1', [21]), [(21, 1, 'tuple_21')])
        self.assertEqual(self.con.select('space_1', [22], index='primary'), [(22, 2, 'tuple_22')])
        self.assertEqual(self.con.select('space_1', [23], index='primary'), [(23, 3, 'tuple_23')])
        # Check that Offset and Limit args are working fine.
        self.assertEqual(self.con.select('space_1', [20], index='primary', limit=1), [(20, 0, 'tuple_20')])
        # With other indexes too
        self.assertEqual(
                sorted(
                    self.con.select('space_1', [0], index='secondary', offset=3, limit=0),
                    key = lambda x: x[0]),
                []
            )
        self.assertEqual(
                sorted(
                    self.con.select('space_1', [0], index='secondary', offset=3, limit=1),
                    key = lambda x: x[0]),
                [(110, 0, 'tuple_110')]
            )
        self.assertEqual(
                sorted(
                    self.con.select('space_1', [0], index='secondary', offset=3, limit=2),
                    key = lambda x: x[0]),
                [(110, 0, 'tuple_110'),\
                (115, 0, 'tuple_115')]
            )

        select_req = self.con.select('space_1', [0], index='secondary')
        self.assertEqual(len(select_req), 99)
        for i in select_req:
            self.assertTrue(not (i[0] % 5))
            self.assertTrue(not i[1])
            self.assertTrue(i[2] == 'tuple_' + str(i[0]))

        # Check limit again.
        self.assertEqual(len(self.con.select('space_1', [0, 'tuple_20'], index='secondary', limit=0)), 0)
        self.assertEqual(len(self.con.select('space_1', [0], index='secondary', limit=0)), 0)
        self.assertEqual(len(self.con.select('space_1', [0], index='secondary', limit=100)), 99)
        self.assertEqual(len(self.con.select('space_1', [0], index='secondary', limit=50)), 50)

        # TODO: Check iterator_types

    def test_03_delete(self):
        # Check that delete works fine
        self.assertEqual(self.con.delete('space_1', 20), [(20, 0, 'tuple_20')])
        self.assertEqual(self.con.delete('space_1', [20]), [])
        self.assertEqual(self.con.select('space_1', [20], index='primary'), [])
        # Check that <index_id> field has no meaning, yet.
        with self.assertRaisesRegexp(tarantool.DatabaseError,
                '(19, .*)'):
                self.con.delete('space_1', [1, 'tuple_21'])
        self.assertEqual(self.con.select('space_1', [21], index='primary'), [(21, 1, 'tuple_21')])

    def test_04_replace(self):
        # Check replace that is Ok.
        self.assertEqual(self.con.replace('space_1', [2, 2, 'tuple_3']), [(2, 2, 'tuple_3')])
        self.assertEqual(self.con.select('space_1', 2), [(2, 2, 'tuple_3')])
        # Check replace that isn't Ok.
        with self.assertRaisesRegexp(tarantool.DatabaseError,
                '(39, .*)'):
            self.assertEqual(self.con.replace('space_1', [2, 2]), [(2, 2, 'tuple_2')])

    def test_05_ping(self):
        # Simple ping test
        # * No exceptions are raised
        # * Ping time > 0
        self.assertTrue(self.con.ping() > 0)
        self.assertEqual(self.con.ping(notime=True), "Success")

    def test_06_update(self):
        self.assertEqual(self.con.update('space_1', (2,), [('+', 1, 3)]),
                [(2, 5, 'tuple_3')])
        self.assertEqual(self.con.update('space_1', (2,), [('-', 1, 3)]),
                [(2, 2, 'tuple_3')])
        self.assertEqual(self.con.update('space_1', (2,), [(':', 2, 3, 2, 'lalal')]),
                [(2, 2, 'tuplalal_3')])
        self.assertEqual(self.con.update('space_1', (2,), [('!', 2, '1')]),
                [(2, 2, '1', 'tuplalal_3')])
        self.assertEqual(self.con.update('space_1', (2,), [('!', 2, 'oingo, boingo')]),
                [(2, 2, 'oingo, boingo', '1', 'tuplalal_3')])
        self.assertEqual(self.con.update('space_1', (2,), [('#', 2, 2)]),
                [(2, 2, 'tuplalal_3')])

    def test_07_call(self):
        self.assertEqual(self.con.call('json.decode', '[123, 234, 345]'), [(123, 234, 345)])
        self.assertEqual(self.con.call('json.decode', ['[123, 234, 345]']), [(123, 234, 345)])
        self.assertEqual(self.con.call('json.decode', ('[123, 234, 345]',)), [(123, 234, 345)])
        with self.assertRaisesRegexp(tarantool.DatabaseError,
                '(32, .*)'):
            self.con.call('json.decode')
        with self.assertRaisesRegexp(tarantool.DatabaseError,
                '(22, .*)'):
            self.con.call('json.decode', '{"hello": "world"}')
        ans = self.con.call('fiber.time')
        self.assertEqual(len(ans), 1)
        self.assertEqual(len(ans[0]), 1)
        self.assertIsInstance(ans[0][0], float)
        ans = self.con.call('fiber.time64')
        self.assertEqual(len(ans), 1)
        self.assertEqual(len(ans[0]), 1)
        self.assertIsInstance(ans[0][0], (int, long))
        ans = self.con.call('uuid.str')
        self.assertEqual(len(ans), 1)
        self.assertEqual(len(ans[0]), 1)
        self.assertIsInstance(ans[0][0], str)
#        ans = self.con.call('uuid.hex')
#        self.assertEqual(len(ans), 1)
#        self.assertEqual(len(ans[0]), 1)
#        self.assertIsInstance(ans[0][0], str)
#        with self.assertRaisesRegexp(tarantool.DatabaseError,
#                '(12345, \'lol, error\')'):
#            self.con.call('box.error', [12345, 'lol, error'])

        self.assertEqual(self.con.call('box.tuple.new', [1, 2, 3, 'fld_1']), [(1, 2, 3, 'fld_1')])
        self.assertEqual(self.con.call('box.tuple.new', 'fld_1'), [('fld_1',)])

    @classmethod
    def tearDownClass(self):
        self.srv.stop()
        self.srv.clean()
