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

class TestSuite_DatetimeArithmetic(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' DATETIME TYPE ARITHMETIC '.center(70, '='), file=sys.stderr)
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

            local function add(arg1, arg2)
                return arg1 + arg2
            end
            rawset(_G, 'add', add)

            local function sub(arg1, arg2)
                return arg1 - arg2
            end
            rawset(_G, 'sub', sub)
        """)

        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        user='test', password='test')

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

        self.adm("box.space['test']:truncate()")


    interval_cases = {
        'year': {
            'arg_1': tarantool.Interval(year=2),
            'arg_2': tarantool.Interval(year=1),
            'res_add': tarantool.Interval(year=3),
            'res_sub': tarantool.Interval(year=1),
        },
        'date': {
            'arg_1': tarantool.Interval(year=1, month=2, day=3),
            'arg_2': tarantool.Interval(year=3, month=2, day=1),
            'res_add': tarantool.Interval(year=4, month=4, day=4),
            'res_sub': tarantool.Interval(year=-2, month=0, day=2),
        },
        'time': {
            'arg_1': tarantool.Interval(hour=10, minute=20, sec=30),
            'arg_2': tarantool.Interval(hour=2, minute=15, sec=50),
            'res_add': tarantool.Interval(hour=12, minute=35, sec=80),
            'res_sub': tarantool.Interval(hour=8, minute=5, sec=-20),
        },
        'datetime': {
            'arg_1': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2, sec=3000),
            'arg_2': tarantool.Interval(year=2, month=1, day=31, hour=-3, minute=0, sec=-2000),
            'res_add': tarantool.Interval(year=3, month=3, day=34, hour=-2, minute=2, sec=1000),
            'res_sub': tarantool.Interval(year=-1, month=1, day=-28, hour=4, minute=2, sec=5000),
        },
        'datetime_with_nsec': {
            'arg_1': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                           sec=3000, nsec=10000000),
            'arg_2': tarantool.Interval(year=2, month=1, day=31, hour=-3, minute=0,
                                           sec=1000, nsec=9876543),
            'res_add': tarantool.Interval(year=3, month=3, day=34, hour=-2, minute=2,
                                          sec=4000, nsec=19876543),
            'res_sub': tarantool.Interval(year=-1, month=1, day=-28, hour=4, minute=2,
                                          sec=2000, nsec=123457),
        },
        'heterogenous': {
            'arg_1': tarantool.Interval(year=1, month=2, day=3),
            'arg_2': tarantool.Interval(sec=3000, nsec=9876543),
            'res_add': tarantool.Interval(year=1, month=2, day=3,
                                          sec=3000, nsec=9876543),
            'res_sub': tarantool.Interval(year=1, month=2, day=3,
                                          sec=-3000, nsec=-9876543),
        },
        'same_adjust': {
            'arg_1': tarantool.Interval(year=2, adjust=tarantool.IntervalAdjust.LAST),
            'arg_2': tarantool.Interval(year=1, adjust=tarantool.IntervalAdjust.LAST),
            'res_add': tarantool.Interval(year=3, adjust=tarantool.IntervalAdjust.LAST),
            'res_sub': tarantool.Interval(year=1, adjust=tarantool.IntervalAdjust.LAST),
        },
        'different_adjust': {
            'arg_1': tarantool.Interval(year=2, adjust=tarantool.IntervalAdjust.LAST),
            'arg_2': tarantool.Interval(year=1, adjust=tarantool.IntervalAdjust.EXCESS),
            'res_add': tarantool.Interval(year=3, adjust=tarantool.IntervalAdjust.LAST),
            'res_sub': tarantool.Interval(year=1, adjust=tarantool.IntervalAdjust.LAST),
        },
    }

    def test_python_interval_add(self):
        for name in self.interval_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_cases[name]

                self.assertEqual(case['arg_1'] + case['arg_2'], case['res_add'])

    def test_python_interval_sub(self):
        for name in self.interval_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_cases[name]

                self.assertEqual(case['arg_1'] - case['arg_2'], case['res_sub'])

    @skip_or_run_datetime_test
    def test_tarantool_interval_add(self):
        for name in self.interval_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_cases[name]

                self.assertSequenceEqual(self.con.call('add', case['arg_1'], case['arg_2']),
                                         [case['res_add']])

    @skip_or_run_datetime_test
    def test_tarantool_interval_sub(self):
        for name in self.interval_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_cases[name]

                self.assertSequenceEqual(self.con.call('sub', case['arg_1'], case['arg_2']),
                                         [case['res_sub']])


    datetime_sub_cases = {
        'date': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Datetime(year=2010, month=2, day=1),
            'res': tarantool.Interval(year=-2, month=0, day=2),
        },
        'datetime': {
            'arg_1': tarantool.Datetime(year=2001, month=2, day=3, hour=1, minute=2, sec=30),
            'arg_2': tarantool.Datetime(year=2002, month=1, day=31, hour=3, minute=0, sec=20),
            'res': tarantool.Interval(year=-1, month=1, day=-28, hour=-2, minute=2, sec=10),
        },
        'datetime_with_nsec': {
            'arg_1': tarantool.Datetime(year=2001, month=2, day=3, hour=1, minute=2,
                                        sec=30, nsec=10000000),
            'arg_2': tarantool.Datetime(year=2002, month=1, day=31, hour=3, minute=0,
                                        sec=10, nsec=9876543),
            'res': tarantool.Interval(year=-1, month=1, day=-28, hour=-2, minute=2,
                                      sec=20, nsec=123457),
        },
        'heterogenous': {
            'arg_1': tarantool.Datetime(year=2001, month=2, day=3, hour=1, minute=2),
            'arg_2': tarantool.Datetime(year=2001, month=2, day=3, sec=30,
                                        nsec=9876543),
            'res': tarantool.Interval(hour=1, minute=2, sec=-30, nsec=-9876543),
        },
    }

    def test_python_datetime_sub(self):
        for name in self.datetime_sub_cases.keys():
            with self.subTest(msg=name):
                case = self.datetime_sub_cases[name]

                self.assertEqual(case['arg_1'] - case['arg_2'], case['res'])

    @skip_or_run_datetime_test
    def test_tarantool_datetime_sub(self):
        for name in self.datetime_sub_cases.keys():
            with self.subTest(msg=name):
                case = self.datetime_sub_cases[name]

                self.assertSequenceEqual(self.con.call('sub', case['arg_1'], case['arg_2']),
                                         [case['res']])


    datetime_sub_different_timezones_case = {
        'arg_1': tarantool.Datetime(year=2001, month=2, day=3, tz='UTC'),
        'arg_2': tarantool.Datetime(year=2001, month=2, day=3, tz='MSK'),
        'res': tarantool.Interval(day=1, hour=-21),
    }

    def test_python_datetime_sub_different_timezones(self):
        case = self.datetime_sub_different_timezones_case

        self.assertEqual(case['arg_1'] - case['arg_2'], case['res'])

    @skip_or_run_datetime_test
    @unittest.expectedFailure # See https://github.com/tarantool/tarantool/issues/7698
    def test_tarantool_datetime_sub_different_timezones(self):
        case = self.datetime_sub_different_timezones_case

        self.assertSequenceEqual(self.con.call('sub', case['arg_1'], case['arg_2']),
                                         [case['res']])


    datetime_add_cases = {
        'year': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(year=1),
            'res': tarantool.Datetime(year=2009, month=2, day=3),
        },
        'date': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(year=1, month=2, day=3),
            'res': tarantool.Datetime(year=2009, month=4, day=6),
        },
        'date_days_overflow': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(year=1, month=2, day=30),
            'res': tarantool.Datetime(year=2009, month=5, day=3),
        },
        'time': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(hour=1, minute=2, sec=3),
            'res': tarantool.Datetime(year=2008, month=2, day=3, hour=1, minute=2, sec=3),
        },
        'time_secs_overflow': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(hour=1, minute=2, sec=13003),
            'res': tarantool.Datetime(year=2008, month=2, day=3, hour=4, minute=38, sec=43),
        },
        'nsecs': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43),
            'arg_2': tarantool.Interval(nsec=10000023),
            'res': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43,
                                      nsec=10000023),
        },
        'zero': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43),
            'arg_2': tarantool.Interval(),
            'res': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43),
        },
        'month_non_last_day_none_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=1, day=30),
            'arg_2': tarantool.Interval(month=13, adjust=tarantool.IntervalAdjust.NONE),
            'res': tarantool.Datetime(year=2010, month=2, day=28),
        },
        'month_non_last_day_excess_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=1, day=30),
            'arg_2': tarantool.Interval(month=13, adjust=tarantool.IntervalAdjust.EXCESS),
            'res': tarantool.Datetime(year=2010, month=3, day=2),
        },
        'month_non_last_day_last_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=3, day=30),
            'arg_2': tarantool.Interval(month=2, adjust=tarantool.IntervalAdjust.LAST),
            'res': tarantool.Datetime(year=2009, month=5, day=30),
        },
        'month_overflow_last_day_last_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=2, day=28),
            'arg_2': tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.LAST),
            'res': tarantool.Datetime(year=2009, month=3, day=31),
        },
    }

    def test_python_datetime_add(self):
        for name in self.datetime_add_cases.keys():
            with self.subTest(msg=name):
                case = self.datetime_add_cases[name]

                self.assertEqual(case['arg_1'] + case['arg_2'], case['res'])

    @skip_or_run_datetime_test
    def test_tarantool_datetime_add(self):
        for name in self.datetime_add_cases.keys():
            with self.subTest(msg=name):
                case = self.datetime_add_cases[name]

                self.assertSequenceEqual(self.con.call('add', case['arg_1'], case['arg_2']),
                                         [case['res']])

    datetime_add_winter_time_switch_case = {
        'arg_1': tarantool.Datetime(year=2008, month=1, day=1, tz='Europe/Moscow'),
        'arg_2': tarantool.Interval(month=6),
        'res': tarantool.Datetime(year=2008, month=7, day=1, tz='Europe/Moscow'),
    }

    def test_python_datetime_add_winter_time_switch(self):
        case = self.datetime_add_winter_time_switch_case

        self.assertEqual(case['arg_1'] + case['arg_2'], case['res'])

    @skip_or_run_datetime_test
    @unittest.expectedFailure # See https://github.com/tarantool/tarantool/issues/7700
    def test_tarantool_datetime_add_winter_time_switch(self):
        case = self.datetime_add_winter_time_switch_case

        self.assertSequenceEqual(self.con.call('add', case['arg_1'], case['arg_2']),
                                 [case['res']])


    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()
