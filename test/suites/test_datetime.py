# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import re
import unittest
import msgpack
import warnings
import tarantool
import pandas

from tarantool.msgpack_ext.packer import default as packer_default
from tarantool.msgpack_ext.unpacker import ext_hook as unpacker_ext_hook

from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_datetime_test
from tarantool.error import MsgpackError, MsgpackWarning

class TestSuite_Datetime(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' DATETIME EXT TYPE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()

        self.adm = self.srv.admin
        self.adm(r"""
            _, datetime = pcall(require, 'datetime')

            box.schema.space.create('test')
            box.space['test']:create_index('primary', {
                type = 'tree',
                parts = {1, 'string'},
                unique = true})

            box.schema.user.create('test', {password = 'test', if_not_exists = true})
            box.schema.user.grant('test', 'read,write,execute', 'universe')
        """)

        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        user='test', password='test')

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

        self.adm("box.space['test']:truncate()")


    def test_Datetime_class_API(self):
        dt = tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                nsec=308543321)

        self.assertEqual(dt.year, 2022)
        self.assertEqual(dt.month, 8)
        self.assertEqual(dt.day, 31)
        self.assertEqual(dt.hour, 18)
        self.assertEqual(dt.minute, 7)
        self.assertEqual(dt.sec, 54)
        self.assertEqual(dt.nsec, 308543321)
        # Both Tarantool and pandas prone to precision loss for timestamp() floats
        self.assertEqual(dt.timestamp, 1661969274.308543)
        self.assertEqual(dt.value, 1661969274308543321)


    datetime_class_invalid_init_cases = {
        'positional_year': {
            'args': [2022],
            'kwargs': {},
            'type': ValueError,
            'msg': 'data argument (first positional argument) expected to be a "bytes" instance'
        },
        'positional_date': {
            'args': [2022, 8, 31],
            'kwargs': {},
            'type': TypeError,
            'msg': '__init__() takes from 1 to 2 positional arguments but 4 were given'
        },
        'mixing_date_and_timestamp': {
            'args': [],
            'kwargs': {'year': 2022, 'timestamp': 1661969274},
            'type': ValueError,
            'msg': 'Cannot provide both timestamp and year, month, day, hour, minute, sec'
        },
        'mixing_float_timestamp_and_nsec': {
            'args': [],
            'kwargs': {'timestamp': 1661969274.308543, 'nsec': 308543321},
            'type': ValueError,
            'msg': 'timestamp must be int if nsec provided'
        },
    }

    def test_Datetime_class_invalid_init(self):
        for name in self.datetime_class_invalid_init_cases.keys():
            with self.subTest(msg=name):
                case = self.datetime_class_invalid_init_cases[name]
                self.assertRaisesRegex(
                    case['type'], re.escape(case['msg']),
                    lambda: tarantool.Datetime(*case['args'], **case['kwargs']))


    integration_cases = {
        'date': {
            'python': tarantool.Datetime(year=2022, month=8, day=31),
            'msgpack': (b'\x80\xa4\x0e\x63\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31})",
        },
        'date_unix_start': {
            'python': tarantool.Datetime(year=1970, month=1, day=1),
            'msgpack': (b'\x00\x00\x00\x00\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({year=1970, month=1, day=1})",
        },
        'date_before_1970': {
            'python': tarantool.Datetime(year=1900, month=1, day=1),
            'msgpack': (b'\x80\x81\x55\x7c\xff\xff\xff\xff'),
            'tarantool': r"datetime.new({year=1900, month=1, day=1})",
        },
        'datetime_with_minutes': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7),
            'msgpack': (b'\x44\xa3\x0f\x63\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7})",
        },
        'datetime_with_seconds': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54),
            'msgpack': (b'\x7a\xa3\x0f\x63\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54})",
        },
        'datetime_with_microseconds': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543000),
            'msgpack': (b'\x7a\xa3\x0f\x63\x00\x00\x00\x00\x18\xfe\x63\x12\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543000})",
        },
        'datetime_with_nanoseconds': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543321),
            'msgpack': (b'\x7a\xa3\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543321})",
        },
        'date_before_1970_with_nanoseconds': {
            'python': tarantool.Datetime(year=1900, month=1, day=1, nsec=308543321),
            'msgpack': (b'\x80\x81\x55\x7c\xff\xff\xff\xff\x59\xff\x63\x12\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({year=1900, month=1, day=1, nsec=308543321})",
        },
        'timestamp': {
            'python': tarantool.Datetime(timestamp=1661969274),
            'msgpack': (b'\x7a\xa3\x0f\x63\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({timestamp=1661969274})",
        },
        'timestamp_with_nanoseconds': {
            'python': tarantool.Datetime(timestamp=1661969274, nsec=308543321),
            'msgpack': (b'\x7a\xa3\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\x00\x00\x00\x00'),
            'tarantool': r"datetime.new({timestamp=1661969274, nsec=308543321})",
        },
    }

    def test_msgpack_decode(self):
        for name in self.integration_cases.keys():
            with self.subTest(msg=name):
                case = self.integration_cases[name]

                self.assertEqual(unpacker_ext_hook(4, case['msgpack']),
                                 case['python'])

    @skip_or_run_datetime_test
    def test_tarantool_decode(self):
        for name in self.integration_cases.keys():
            with self.subTest(msg=name):
                case = self.integration_cases[name]

                self.adm(f"box.space['test']:replace{{'{name}', {case['tarantool']}, 'field'}}")

                self.assertSequenceEqual(self.con.select('test', name),
                                         [[name, case['python'], 'field']])

    def test_msgpack_encode(self):
        for name in self.integration_cases.keys():
            with self.subTest(msg=name):
                case = self.integration_cases[name]

                self.assertEqual(packer_default(case['python']),
                                 msgpack.ExtType(code=4, data=case['msgpack']))

    @skip_or_run_datetime_test
    def test_tarantool_encode(self):
        for name in self.integration_cases.keys():
            with self.subTest(msg=name):
                case = self.integration_cases[name]

                self.con.insert('test', [name, case['python'], 'field'])

                lua_eval = f"""
                    local dt = {case['tarantool']}
                    
                    local tuple = box.space['test']:get('{name}')
                    assert(tuple ~= nil)

                    if tuple[2] == dt then
                        return true
                    else
                        return nil, ('%s is not equal to expected %s'):format(
                            tostring(tuple[2]), tostring(dt))
                    end
                """

                self.assertSequenceEqual(self.adm(lua_eval), [True])


    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()
