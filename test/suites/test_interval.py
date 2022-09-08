# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import unittest
import msgpack
import warnings
import tarantool
import pandas
import pytz

from tarantool.msgpack_ext.packer import default as packer_default
from tarantool.msgpack_ext.unpacker import ext_hook as unpacker_ext_hook

from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_datetime_test
from tarantool.error import MsgpackError

class TestSuite_Interval(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' INTERVAL EXT TYPE '.center(70, '='), file=sys.stderr)
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


    cases = {
        'year': {
            'python': tarantool.Interval(year=1),
            'msgpack': (b'\x02\x00\x01\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1})",
        },
        'big_year': {
            'python': tarantool.Interval(year=1000),
            'msgpack': (b'\x02\x00\xcd\x03\xe8\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1000})",
        },
        'date': {
            'python': tarantool.Interval(year=1, month=2, day=3),
            'msgpack': (b'\x04\x00\x01\x01\x02\x03\x03\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3})",
        },
        'big_month_date': {
            'python': tarantool.Interval(year=1, month=100000, day=3),
            'msgpack': (b'\x04\x00\x01\x01\xce\x00\x01\x86\xa0\x03\x03\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=100000, day=3})",
        },
        'time': {
            'python': tarantool.Interval(hour=1, minute=2, sec=3),
            'msgpack': (b'\x04\x04\x01\x05\x02\x06\x03\x08\x01'),
            'tarantool': r"datetime.interval.new({hour=1, min=2, sec=3})",
        },
        'big_seconds_time': {
            'python': tarantool.Interval(hour=1, minute=2, sec=3000),
            'msgpack': (b'\x04\x04\x01\x05\x02\x06\xcd\x0b\xb8\x08\x01'),
            'tarantool': r"datetime.interval.new({hour=1, min=2, sec=3000})",
        },
        'datetime': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2, sec=3000),
            'msgpack': (b'\x07\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, min=2, sec=3000})",
        },
        'nanoseconds': {
            'python': tarantool.Interval(nsec=10000000),
            'msgpack': (b'\x02\x07\xce\x00\x98\x96\x80\x08\x01'),
            'tarantool': r"datetime.interval.new({nsec=10000000})",
        },
        'datetime_with_nanoseconds': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000),
            'msgpack': (b'\x08\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce' +
                        b'\x00\x98\x96\x80\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, " +
                         r"min=2, sec=3000, nsec=10000000})",
        },
        'datetime_none_adjust': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000,
                                         adjust=tarantool.IntervalAdjust.NONE),
            'msgpack': (b'\x08\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce' +
                        b'\x00\x98\x96\x80\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, " +
                         r"min=2, sec=3000, nsec=10000000, adjust='none'})",
        },
        'datetime_excess_adjust': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000,
                                         adjust=tarantool.IntervalAdjust.EXCESS),
            'msgpack': (b'\x07\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce' +
                        b'\x00\x98\x96\x80'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, " +
                         r"min=2, sec=3000, nsec=10000000, adjust='excess'})",
        },
        'datetime_last_adjust': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000,
                                         adjust=tarantool.IntervalAdjust.LAST),
            'msgpack': (b'\x08\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce' +
                        b'\x00\x98\x96\x80\x08\x02'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, " +
                         r"min=2, sec=3000, nsec=10000000, adjust='last'})",
        },
        'all_zeroes': {
            'python': tarantool.Interval(adjust=tarantool.IntervalAdjust.EXCESS),
            'msgpack': (b'\x00'),
            'tarantool': r"datetime.interval.new({adjust='excess'})",
        },
    }

    def test_msgpack_decode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.assertEqual(unpacker_ext_hook(6, case['msgpack']),
                                 case['python'])

    @skip_or_run_datetime_test
    def test_tarantool_decode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.adm(f"box.space['test']:replace{{'{name}', {case['tarantool']}, 'field'}}")

                self.assertSequenceEqual(self.con.select('test', name),
                                         [[name, case['python'], 'field']])

    def test_msgpack_encode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.assertEqual(packer_default(case['python']),
                                 msgpack.ExtType(code=6, data=case['msgpack']))

    @skip_or_run_datetime_test
    def test_tarantool_encode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.con.insert('test', [name, case['python'], 'field'])

                lua_eval = f"""
                    local interval = {case['tarantool']}
                    
                    local tuple = box.space['test']:get('{name}')
                    assert(tuple ~= nil)

                    if tuple[2] == interval then
                        return true
                    else
                        return nil, ('%s is not equal to expected %s'):format(
                            tostring(tuple[2]), tostring(interval))
                    end
                """

                self.assertSequenceEqual(self.adm(lua_eval), [True])


    def test_unknown_field_decode(self):
        case = b'\x01\x09\xce\x00\x98\x96\x80'
        self.assertRaisesRegex(
            MsgpackError, 'Unknown interval field id 9',
            lambda: unpacker_ext_hook(6, case))

    def test_unknown_adjust_decode(self):
        case = b'\x02\x07\xce\x00\x98\x96\x80\x08\x03'
        self.assertRaisesRegex(
            MsgpackError, '3 is not a valid Adjust',
            lambda: unpacker_ext_hook(6, case))


    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()
