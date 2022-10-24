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

            pcall(function()
                box.schema.space.create('test_pk')
                box.space['test_pk']:create_index('primary', {
                    type = 'tree',
                    parts = {1, 'datetime'},
                    unique = true})
            end)

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


    def test_Datetime_class_API(self):
        dt = tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                nsec=308543321, tzoffset=180)

        self.assertEqual(dt.year, 2022)
        self.assertEqual(dt.month, 8)
        self.assertEqual(dt.day, 31)
        self.assertEqual(dt.hour, 18)
        self.assertEqual(dt.minute, 7)
        self.assertEqual(dt.sec, 54)
        self.assertEqual(dt.nsec, 308543321)
        # Both Tarantool and pandas prone to precision loss for timestamp() floats
        self.assertEqual(dt.timestamp, 1661958474.308543)
        self.assertEqual(dt.tzoffset, 180)
        self.assertEqual(dt.tz, '')
        self.assertEqual(dt.value, 1661958474308543321)

    def test_Datetime_class_API_wth_tz(self):
        dt = tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                nsec=308543321, tzoffset=123, tz='Europe/Moscow')

        self.assertEqual(dt.year, 2022)
        self.assertEqual(dt.month, 8)
        self.assertEqual(dt.day, 31)
        self.assertEqual(dt.hour, 18)
        self.assertEqual(dt.minute, 7)
        self.assertEqual(dt.sec, 54)
        self.assertEqual(dt.nsec, 308543321)
        # Both Tarantool and pandas prone to precision loss for timestamp() floats
        self.assertEqual(dt.timestamp, 1661958474.308543)
        self.assertEqual(dt.tzoffset, 180)
        self.assertEqual(dt.tz, 'Europe/Moscow')
        self.assertEqual(dt.value, 1661958474308543321)


    datetime_class_invalid_init_cases = {
        'positional_year': {
            'args': [2022],
            'kwargs': {},
            'type': TypeError,
            'msg': '__init__() takes 1 positional argument but 2 were given'
        },
        'positional_date': {
            'args': [2022, 8, 31],
            'kwargs': {},
            'type': TypeError,
            'msg': '__init__() takes 1 positional argument but 4 were given'
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
        'unknown_tz': {
            'args': [],
            'kwargs': {'year': 2022, 'month': 8, 'day': 31, 'tz': 'Moskva'},
            'type': ValueError,
            'msg': 'Unknown Tarantool timezone "Moskva"'
        },
        'abbrev_tz': {
            'args': [],
            'kwargs': {'year': 2022, 'month': 8, 'day': 31, 'tz': 'AET'},
            'type': ValueError,
            'msg': 'Failed to create datetime with ambiguous timezone "AET"'
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
        'datetime_with_positive_offset': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543321, tzoffset=180),
            'msgpack': (b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\xb4\x00\x00\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543321, tzoffset=180})",
        },
        'datetime_with_negative_offset': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543321, tzoffset=-60),
            'msgpack': (b'\x8a\xb1\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\xc4\xff\x00\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543321, tzoffset=-60})",
        },
        'timestamp_with_positive_offset': {
            'python': tarantool.Datetime(timestamp=1661969274, tzoffset=180),
            'msgpack': (b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x00\x00\x00\x00\xb4\x00\x00\x00'),
            'tarantool': r"datetime.new({timestamp=1661969274, tzoffset=180})",
        },
        'timestamp_with_negative_offset': {
            'python': tarantool.Datetime(timestamp=1661969274, tzoffset=-60),
            'msgpack': (b'\x8a\xb1\x0f\x63\x00\x00\x00\x00\x00\x00\x00\x00\xc4\xff\x00\x00'),
            'tarantool': r"datetime.new({timestamp=1661969274, tzoffset=-60})",
        },
        'date_with_utc_tz': {
            'python': tarantool.Datetime(year=1970, month=1, day=1, tz='UTC'),
            'msgpack': (b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x01'),
            'tarantool': r"datetime.new({year=1970, month=1, day=1, tz='UTC'})",
        },
        'date_with_tz': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, tz='Europe/Moscow'),
            'msgpack': (b'\x50\x7a\x0e\x63\x00\x00\x00\x00\x00\x00\x00\x00\xb4\x00\xb3\x03'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, tz='Europe/Moscow'})",
        },
        'datetime_with_tz': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543321, tz='Europe/Moscow'),
            'msgpack': (b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\xb4\x00\xb3\x03'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543321, tz='Europe/Moscow'})",
        },
        'datetime_with_tz_winter_time': {
            'python': tarantool.Datetime(year=2008, month=8, day=1, tz='Europe/Moscow'),
            'msgpack': (b'\xc0\x19\x92\x48\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x00\xb3\x03'),
            'tarantool': r"datetime.new({year=2008, month=8, day=1, tz='Europe/Moscow'})",
        },
        'datetime_with_tz_and_offset': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543321, tz='Europe/Moscow', tzoffset=123),
            'msgpack': (b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\xb4\x00\xb3\x03'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543321, tz='Europe/Moscow', tzoffset=123})",
        },
        'datetime_with_abbrev_tz': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543321, tz='MSK'),
            'msgpack': (b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\xb4\x00\xee\x00'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543321, tz='MSK'})",
        },
        'datetime_with_abbrev_tz_and_zero_offset': {
            'python': tarantool.Datetime(year=2022, month=8, day=31, hour=18, minute=7, sec=54,
                                         nsec=308543321, tz='AZODT'),
            'msgpack': (b'\x7a\xa3\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\x00\x00\x12\x02'),
            'tarantool': r"datetime.new({year=2022, month=8, day=31, hour=18, min=7, sec=54, " +
                         r"nsec=308543321, tz='AZODT'})",
        },
        'timestamp_since_utc_epoch': {
            'python': tarantool.Datetime(timestamp=1661958474, nsec=308543321,
                                         tz='Europe/Moscow', timestamp_since_utc_epoch=True),
            'msgpack': (b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\xb4\x00\xb3\x03'),
            'tarantool': r"datetime.new({timestamp=1661969274, nsec=308543321, tz='Europe/Moscow'})",
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

    def test_msgpack_decode_unknown_tzindex(self):
        case = b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\xb4\x00\xff\xff'
        self.assertRaisesRegex(
            MsgpackError, 'Failed to decode datetime with unknown tzindex "-1"',
            lambda: unpacker_ext_hook(4, case))

    def test_msgpack_decode_ambiguous_tzindex(self):
        case = b'\x4a\x79\x0f\x63\x00\x00\x00\x00\x59\xff\x63\x12\x00\x00\x82\x00'
        self.assertRaisesRegex(
            ValueError, 'Failed to create datetime with ambiguous timezone "AET"',
            lambda: unpacker_ext_hook(4, case))


    datetime_subtraction_cases = {
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

    def test_python_datetime_subtraction(self):
        for name in self.datetime_subtraction_cases.keys():
            with self.subTest(msg=name):
                case = self.datetime_subtraction_cases[name]

                self.assertEqual(case['arg_1'] - case['arg_2'], case['res'])

    @skip_or_run_datetime_test
    def test_tarantool_datetime_subtraction(self):
        for name in self.datetime_subtraction_cases.keys():
            with self.subTest(msg=name):
                case = self.datetime_subtraction_cases[name]

                self.assertSequenceEqual(self.con.call('sub', case['arg_1'], case['arg_2']),
                                         [case['res']])


    datetime_subtraction_different_timezones_case = {
        'arg_1': tarantool.Datetime(year=2001, month=2, day=3, tz='UTC'),
        'arg_2': tarantool.Datetime(year=2001, month=2, day=3, tz='MSK'),
        'res': tarantool.Interval(day=1, hour=-21),
    }

    def test_python_datetime_subtraction_different_timezones(self):
        case = self.datetime_subtraction_different_timezones_case

        self.assertEqual(case['arg_1'] - case['arg_2'], case['res'])

    @skip_or_run_datetime_test
    @unittest.skip('See https://github.com/tarantool/tarantool/issues/7698')
    def test_tarantool_datetime_subtraction_different_timezones(self):
        case = self.datetime_subtraction_different_timezones_case

        self.assertSequenceEqual(self.con.call('sub', case['arg_1'], case['arg_2']),
                                         [case['res']])


    interval_arithmetic_cases = {
        'year': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(year=1),
            'res_add': tarantool.Datetime(year=2009, month=2, day=3),
            'res_sub': tarantool.Datetime(year=2007, month=2, day=3),
        },
        'date': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(year=1, month=2, day=3),
            'res_add': tarantool.Datetime(year=2009, month=4, day=6),
            'res_sub': tarantool.Datetime(year=2006, month=11, day=30),
        },
        'date_days_overflow': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(year=1, month=2, day=30),
            'res_add': tarantool.Datetime(year=2009, month=5, day=3),
            'res_sub': tarantool.Datetime(year=2006, month=11, day=3),
        },
        'time': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(hour=1, minute=2, sec=3),
            'res_add': tarantool.Datetime(year=2008, month=2, day=3, hour=1, minute=2, sec=3),
            'res_sub': tarantool.Datetime(year=2008, month=2, day=2, hour=22, minute=57, sec=57),
        },
        'time_secs_overflow': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3),
            'arg_2': tarantool.Interval(hour=1, minute=2, sec=13003),
            'res_add': tarantool.Datetime(year=2008, month=2, day=3, hour=4, minute=38, sec=43),
            'res_sub': tarantool.Datetime(year=2008, month=2, day=2, hour=19, minute=21, sec=17),
        },
        'nsecs': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43),
            'arg_2': tarantool.Interval(nsec=10000023),
            'res_add': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43,
                                      nsec=10000023),
            'res_sub': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=42,
                                      nsec=989999977),
        },
        'zero': {
            'arg_1': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43),
            'arg_2': tarantool.Interval(),
            'res_add': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43),
            'res_sub': tarantool.Datetime(year=2008, month=2, day=3, hour=3, minute=36, sec=43),
        },
        'month_non_last_day_none_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=1, day=30),
            'arg_2': tarantool.Interval(month=13, adjust=tarantool.IntervalAdjust.NONE),
            'res_add': tarantool.Datetime(year=2010, month=2, day=28),
            'res_sub': tarantool.Datetime(year=2007, month=12, day=30),
        },
        'month_non_last_day_none_adjust_negative': {
            'arg_1': tarantool.Datetime(year=2009, month=1, day=30),
            'arg_2': tarantool.Interval(month=11, adjust=tarantool.IntervalAdjust.NONE),
            'res_add': tarantool.Datetime(year=2009, month=12, day=30),
            'res_sub': tarantool.Datetime(year=2008, month=2, day=29),
        },
        'month_non_last_day_excess_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=1, day=30),
            'arg_2': tarantool.Interval(month=13, adjust=tarantool.IntervalAdjust.EXCESS),
            'res_add': tarantool.Datetime(year=2010, month=3, day=2),
            'res_sub': tarantool.Datetime(year=2007, month=12, day=30),
        },
        'month_non_last_day_excess_adjust_negative': {
            'arg_1': tarantool.Datetime(year=2009, month=1, day=30),
            'arg_2': tarantool.Interval(month=11, adjust=tarantool.IntervalAdjust.EXCESS),
            'res_add': tarantool.Datetime(year=2009, month=12, day=30),
            'res_sub': tarantool.Datetime(year=2008, month=3, day=1),
        },
        'month_non_last_day_last_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=3, day=30),
            'arg_2': tarantool.Interval(month=2, adjust=tarantool.IntervalAdjust.LAST),
            'res_add': tarantool.Datetime(year=2009, month=5, day=30),
            'res_sub': tarantool.Datetime(year=2009, month=1, day=30),
        },
        'month_overflow_last_day_last_adjust': {
            'arg_1': tarantool.Datetime(year=2009, month=2, day=28),
            'arg_2': tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.LAST),
            'res_add': tarantool.Datetime(year=2009, month=3, day=31),
            'res_sub': tarantool.Datetime(year=2009, month=1, day=31),
        },
    }

    def test_python_datetime_addition(self):
        for name in self.interval_arithmetic_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_arithmetic_cases[name]

                self.assertEqual(case['arg_1'] + case['arg_2'], case['res_add'])

    def test_python_datetime_subtraction(self):
        for name in self.interval_arithmetic_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_arithmetic_cases[name]

                self.assertEqual(case['arg_1'] - case['arg_2'], case['res_sub'])

    @skip_or_run_datetime_test
    def test_tarantool_datetime_addition(self):
        for name in self.interval_arithmetic_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_arithmetic_cases[name]

                self.assertSequenceEqual(self.con.call('add', case['arg_1'], case['arg_2']),
                                         [case['res_add']])

    @skip_or_run_datetime_test
    def test_tarantool_datetime_subtraction(self):
        for name in self.interval_arithmetic_cases.keys():
            with self.subTest(msg=name):
                case = self.interval_arithmetic_cases[name]

                self.assertSequenceEqual(self.con.call('sub', case['arg_1'], case['arg_2']),
                                         [case['res_sub']])


    datetime_addition_winter_time_switch_case = {
        'arg_1': tarantool.Datetime(year=2008, month=1, day=1, hour=12, tz='Europe/Moscow'),
        'arg_2': tarantool.Interval(month=6),
        'res': tarantool.Datetime(year=2008, month=7, day=1, hour=12, tz='Europe/Moscow'),
    }

    def test_python_datetime_addition_winter_time_switch(self):
        case = self.datetime_addition_winter_time_switch_case

        self.assertEqual(case['arg_1'] + case['arg_2'], case['res'])

    @skip_or_run_datetime_test
    @unittest.skip('See https://github.com/tarantool/tarantool/issues/7700')
    def test_tarantool_datetime_addition_winter_time_switch(self):
        case = self.datetime_addition_winter_time_switch_case

        self.assertSequenceEqual(self.con.call('add', case['arg_1'], case['arg_2']),
                                 [case['res']])


    @skip_or_run_datetime_test
    def test_primary_key(self):
        data = [tarantool.Datetime(year=1970, month=1, day=1), 'content']

        self.assertSequenceEqual(self.con.insert('test_pk', data), [data])
        self.assertSequenceEqual(self.con.select('test_pk', data[0]), [data])

    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()
