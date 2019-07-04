# -*- coding: utf-8 -*-
import unittest

import tarantool
from .lib.tarantool15 import TarantoolServer

schema = {
    0: {
        'default_type': tarantool.STR,
        'fields': {
            0: tarantool.STR,
            1: tarantool.STR,
            2: tarantool.STR
        },
        'indexes': {
            0: [0],
            1: [1, 2],
        },
    },
    1: {
        'default_type': tarantool.STR,
        'fields': {
            0: tarantool.STR,
            1: tarantool.STR,
            2: tarantool.NUM
        },
        'indexes': {
            0: [0, 1],
            1: [0]
        }
    },
    2: {
        'default_type': tarantool.STR,
        'fields': {
            0: tarantool.NUM,
            1: tarantool.NUM64,
            2: tarantool.NUM,
        },
        'indexes': {
            0: [0, 1],
            1: [1, 0]
        }
    }
}

class Server15Tests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.srv = TarantoolServer()
        self.srv.script = 'tests/share/init.lua'
        self.srv.config = 'tests/share/tarantool.cfg'
        self.srv.start()
        self.con = tarantool.Connection(
                'localhost',
                int(self.srv.args['primary']),
                schema = schema
        )

    def test_01_insert(self):
        list_0 = [
            ('hell1', 'mikk1', 'mouse'),
            ('hell2', 'mikki', 'mous1'),
            ('hell3', 'mikki', 'mouse'),
            ('hell4', 'mikk1', 'mous1')
        ]
        list_1 = [
            ('hell1', 'mikk1', 1),
            ('hell1', 'mikk2', 2),
            ('hell2', 'mikk1', 3),
            ('hell2', 'mikk2', 4)
        ]
        list_2 = [
            (1, 68719476736),
            (1, 17179869184),
            (1, 34359738368),
            (2, 34359738368),
            (3, 8589934592 ),
            (4, 17179869184)
        ]
        for val in list_0:
            self.assertEqual(list(self.con.store(0, val)), [val])
        for val in list_1:
            self.assertEqual(list(self.con.store(1, val)), [val])
        for val in list_2:
            self.assertEqual(list(self.con.store(2, val)), [val])
        self.assertEqual(self.con.replace(0, list_0[1]), [list_0[1]])
        with self.assertRaises(tarantool.DatabaseError):
            self.con.replace(0, ('hell5', 'a', 'b'))
        with self.assertRaises(tarantool.DatabaseError):
            self.con.insert(0, list_0[1])
        self.assertEqual(
                self.con.insert( 0,
                    ('hell5', 'a', 'b')
                ),
                [('hell5', 'a', 'b')]
        )

    def test_02_select(self):
        self.assertEqual(self.con.select(0, [[]]), [
            ('hell1', 'mikk1', 'mouse'),
            ('hell2', 'mikki', 'mous1'),
            ('hell3', 'mikki', 'mouse'),
            ('hell4', 'mikk1', 'mous1'),
            ('hell5', 'a', 'b')
        ])
        self.assertEqual(self.con.select(0, 'hell1'), [
            ('hell1', 'mikk1', 'mouse'),
        ])
        self.assertEqual(self.con.select(0, ['hell1']), [
            ('hell1', 'mikk1', 'mouse'),
        ])
        self.assertEqual(self.con.select(0, ['mikki'], index=1), [
            ('hell2', 'mikki', 'mous1'),
            ('hell3', 'mikki', 'mouse')
        ])
        self.assertEqual(self.con.select(0, [['mikki', 'mous1']], index=1), [
            ('hell2', 'mikki', 'mous1')
        ])
        self.assertEqual(self.con.select(0, [['mikki', 'mouse']], index=1), [
            ('hell3', 'mikki', 'mouse')
        ])
        self.assertEqual(self.con.select(0, [
                ('mikki', 'mouse'),
                ('mikki', 'mous1')
            ], index=1), [
                ('hell3', 'mikki', 'mouse'),
                ('hell2', 'mikki', 'mous1')
        ])
        self.assertEqual(self.con.select(0, [['mikki'], ['mikk1']], index=1), [
            ('hell2', 'mikki', 'mous1'),
            ('hell3', 'mikki', 'mouse'),
            ('hell4', 'mikk1', 'mous1'),
            ('hell1', 'mikk1', 'mouse')
        ])
        self.assertEqual(self.con.select(2, [1]), [
                (1, 17179869184),
                (1, 34359738368),
                (1, 68719476736)
        ])
        self.assertEqual(self.con.select(2, [[1, 68719476736]]), [
                (1, 68719476736)
        ])
        self.assertEqual(self.con.select(2, [34359738368], index=1), [
                (1, 34359738368),
                (2, 34359738368)
        ])
        self.assertEqual(self.con.select(2, [[34359738368, 1]], index=1), [
                (1, 34359738368)
        ])

    def test_03_ping(self):
        self.assertTrue(self.con.ping() < 1)

    def test_04_delete(self):
        data = [
            (('hell1', 'mikk1'), [('hell1', 'mikk1', 1)]),
            (('hell1', 'mikk2'), [('hell1', 'mikk2', 2)]),
            (('hell2', 'mikk1'), [('hell2', 'mikk1', 3)]),
            (('hell2', 'mikk2'), [('hell2', 'mikk2', 4)])
        ]
        for (left, right) in data:
            self.assertEqual(self.con.delete(1, left), right)

    def test_05_update(self):
        self.assertEqual(
            self.con.update(2, [1, 17179869184], [(1, '+', 1), (2, '=', 1)]),
            [(1, 17179869185, 1)]
        )
        self.assertEqual(
            self.con.update(2, [1, 17179869185], [(2, 'del')]),
            [(1, 17179869185)]
        )
        self.assertEqual(
                self.con.update(0, 'hell1', [(2, ':', 2, 2, 'boy')]),
            [('hell1', 'mikk1', 'moboye')]
        )

    def test_06_call(self):
        self.assertIsInstance(
                self.con.call('box.time', [], field_defs=[(0, int)])[0][0],
                int
        ),
        self.assertIsInstance(
                self.con.call('box.time64', [], field_defs=[(0, int)])[0][0],
                int
        ),
        self.assertEqual(
                self.con.call('test.a', 1, 2, 'hello', 'world', field_defs=
                    [(0, str), (1, str), (2, int), (3, int)]
                ),
                [('hello', 'world', 2, 1)]
        )
        self.assertEqual(
                self.con.call('test.b', '12345', field_defs=
                    [(0, int), (1, int)]),
                [(12345, 12345)]
        )

    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop ()
        self.srv.clean()
