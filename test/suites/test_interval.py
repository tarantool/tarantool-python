"""
This module tests work with datetime interval type.
"""
# pylint: disable=missing-class-docstring,missing-function-docstring,protected-access,too-many-function-args,duplicate-code

import re
import sys
import unittest

import msgpack

import tarantool
from tarantool.error import MsgpackError
from tarantool.msgpack_ext.packer import default as packer_default
from tarantool.msgpack_ext.unpacker import ext_hook as unpacker_ext_hook
from tarantool.msgpack_ext.types.interval import (
    MAX_YEAR_RANGE,
    MAX_MONTH_RANGE,
    MAX_WEEK_RANGE,
    MAX_DAY_RANGE,
    MAX_HOUR_RANGE,
    MAX_MIN_RANGE,
    MAX_SEC_RANGE,
    MAX_NSEC_RANGE,
)

from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_datetime_test


class TestSuiteInterval(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(' INTERVAL EXT TYPE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        cls.srv = TarantoolServer()
        cls.srv.script = 'test/suites/box.lua'
        cls.srv.start()

        cls.adm = cls.srv.admin
        cls.adm(r"""
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

        cls.con = tarantool.Connection(cls.srv.host, cls.srv.args['primary'],
                                       user='test', password='test')

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

        self.adm("box.space['test']:truncate()")

    def test_interval_positional_init(self):
        self.assertRaisesRegex(
            TypeError, re.escape('__init__() takes 1 positional argument but 2 were given'),
            lambda: tarantool.Interval(1))

    cases = {
        'year': {
            'python': tarantool.Interval(year=1),
            'msgpack': (b'\x02\x00\x01\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1})",
            'str': 'tarantool.Interval(year=1, month=0, week=0, day=0, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'big_year': {
            'python': tarantool.Interval(year=1000),
            'msgpack': (b'\x02\x00\xcd\x03\xe8\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1000})",
            'str': 'tarantool.Interval(year=1000, month=0, week=0, day=0, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'date': {
            'python': tarantool.Interval(year=1, month=2, day=3),
            'msgpack': (b'\x04\x00\x01\x01\x02\x03\x03\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3})",
            'str': 'tarantool.Interval(year=1, month=2, week=0, day=3, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'big_month_date': {
            'python': tarantool.Interval(year=1, month=100000, day=3),
            'msgpack': (b'\x04\x00\x01\x01\xce\x00\x01\x86\xa0\x03\x03\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=100000, day=3})",
            'str': 'tarantool.Interval(year=1, month=100000, week=0, day=3, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'time': {
            'python': tarantool.Interval(hour=1, minute=2, sec=3),
            'msgpack': (b'\x04\x04\x01\x05\x02\x06\x03\x08\x01'),
            'tarantool': r"datetime.interval.new({hour=1, min=2, sec=3})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=1, '
                   'minute=2, sec=3, nsec=0, adjust=Adjust.NONE)',
        },
        'big_seconds_time': {
            'python': tarantool.Interval(hour=1, minute=2, sec=3000),
            'msgpack': (b'\x04\x04\x01\x05\x02\x06\xcd\x0b\xb8\x08\x01'),
            'tarantool': r"datetime.interval.new({hour=1, min=2, sec=3000})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=1, '
                   'minute=2, sec=3000, nsec=0, adjust=Adjust.NONE)',
        },
        'datetime': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2, sec=3000),
            'msgpack': (b'\x07\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, "
                         r"min=2, sec=3000})",
            'str': 'tarantool.Interval(year=1, month=2, week=0, day=3, hour=1, '
                   'minute=2, sec=3000, nsec=0, adjust=Adjust.NONE)',
        },
        'nanoseconds': {
            'python': tarantool.Interval(nsec=10000000),
            'msgpack': (b'\x02\x07\xce\x00\x98\x96\x80\x08\x01'),
            'tarantool': r"datetime.interval.new({nsec=10000000})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   'minute=0, sec=0, nsec=10000000, adjust=Adjust.NONE)',
        },
        'datetime_with_nanoseconds': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000),
            'msgpack': (b'\x08\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce'
                        b'\x00\x98\x96\x80\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, "
                         r"min=2, sec=3000, nsec=10000000})",
            'str': 'tarantool.Interval(year=1, month=2, week=0, day=3, hour=1, '
                   'minute=2, sec=3000, nsec=10000000, adjust=Adjust.NONE)',
        },
        'datetime_none_adjust': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000,
                                         adjust=tarantool.IntervalAdjust.NONE),
            'msgpack': (b'\x08\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce'
                        b'\x00\x98\x96\x80\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, "
                         r"min=2, sec=3000, nsec=10000000, adjust='none'})",
            'str': 'tarantool.Interval(year=1, month=2, week=0, day=3, hour=1, '
                   'minute=2, sec=3000, nsec=10000000, adjust=Adjust.NONE)',
        },
        'datetime_excess_adjust': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000,
                                         adjust=tarantool.IntervalAdjust.EXCESS),
            'msgpack': (b'\x07\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce'
                        b'\x00\x98\x96\x80'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, "
                         r"min=2, sec=3000, nsec=10000000, adjust='excess'})",
            'str': 'tarantool.Interval(year=1, month=2, week=0, day=3, hour=1, '
                   'minute=2, sec=3000, nsec=10000000, adjust=Adjust.EXCESS)',
        },
        'datetime_last_adjust': {
            'python': tarantool.Interval(year=1, month=2, day=3, hour=1, minute=2,
                                         sec=3000, nsec=10000000,
                                         adjust=tarantool.IntervalAdjust.LAST),
            'msgpack': (b'\x08\x00\x01\x01\x02\x03\x03\x04\x01\x05\x02\x06\xcd\x0b\xb8\x07\xce'
                        b'\x00\x98\x96\x80\x08\x02'),
            'tarantool': r"datetime.interval.new({year=1, month=2, day=3, hour=1, "
                         r"min=2, sec=3000, nsec=10000000, adjust='last'})",
            'str': 'tarantool.Interval(year=1, month=2, week=0, day=3, hour=1, '
                   'minute=2, sec=3000, nsec=10000000, adjust=Adjust.LAST)',
        },
        'all_zeroes': {
            'python': tarantool.Interval(adjust=tarantool.IntervalAdjust.EXCESS),
            'msgpack': (b'\x00'),
            'tarantool': r"datetime.interval.new({adjust='excess'})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.EXCESS)',
        },
        'weeks': {
            'python': tarantool.Interval(week=3),
            'msgpack': (b'\x02\x02\x03\x08\x01'),
            'tarantool': r"datetime.interval.new({week=3})",
            'str': 'tarantool.Interval(year=0, month=0, week=3, day=0, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'date_with_week': {
            'python': tarantool.Interval(year=1, month=2, week=3, day=4),
            'msgpack': (b'\x05\x00\x01\x01\x02\x02\x03\x03\x04\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, week=3, day=4})",
            'str': 'tarantool.Interval(year=1, month=2, week=3, day=4, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'datetime_with_week': {
            'python': tarantool.Interval(year=1, month=2, week=3, day=4, hour=1, minute=2,
                                         sec=3000, nsec=10000000),
            'msgpack': (b'\x09\x00\x01\x01\x02\x02\x03\x03\x04\x04\x01\x05\x02\x06\xcd\x0b\xb8'
                        b'\x07\xce\x00\x98\x96\x80\x08\x01'),
            'tarantool': r"datetime.interval.new({year=1, month=2, week=3, day=4, hour=1, "
                         r"min=2, sec=3000, nsec=10000000})",
            'str': 'tarantool.Interval(year=1, month=2, week=3, day=4, hour=1, '
                   'minute=2, sec=3000, nsec=10000000, adjust=Adjust.NONE)',
        },
        'min_year_interval': {
            'python': tarantool.Interval(year=-int(MAX_YEAR_RANGE)),
            'msgpack': (b'\x02\x00\xd2\xff\x4c\x91\x8b\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{year=-{int(MAX_YEAR_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{year=-{int(MAX_YEAR_RANGE)} + 1}}) - "
                         r"datetime.interval.new({year=1})",
            'str': f'tarantool.Interval(year=-{int(MAX_YEAR_RANGE)}, month=0, week=0, day=0, '
                   'hour=0, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'max_year_interval': {
            'python': tarantool.Interval(year=int(MAX_YEAR_RANGE)),
            'msgpack': (b'\x02\x00\xce\x00\xb3\x6e\x75\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{year={int(MAX_YEAR_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{year={int(MAX_YEAR_RANGE)} - 1}}) + "
                         r"datetime.interval.new({year=1})",
            'str': f'tarantool.Interval(year={int(MAX_YEAR_RANGE)}, month=0, week=0, day=0, '
                   'hour=0, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'min_month_interval': {
            'python': tarantool.Interval(month=-int(MAX_MONTH_RANGE)),
            'msgpack': (b'\x02\x01\xd2\xf7\x96\xd2\x84\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{month=-{int(MAX_MONTH_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{month=-{int(MAX_MONTH_RANGE)} + 1}}) - "
                         r"datetime.interval.new({month=1})",
            'str': f'tarantool.Interval(year=0, month=-{int(MAX_MONTH_RANGE)}, week=0, day=0, '
                   'hour=0, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'max_month_interval': {
            'python': tarantool.Interval(month=int(MAX_MONTH_RANGE)),
            'msgpack': (b'\x02\x01\xce\x08\x69\x2d\x7c\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{month={int(MAX_MONTH_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{month={int(MAX_MONTH_RANGE)} - 1}}) + "
                         r"datetime.interval.new({month=1})",
            'str': f'tarantool.Interval(year=0, month={int(MAX_MONTH_RANGE)}, week=0, day=0, '
                   'hour=0, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'min_week_interval': {
            'python': tarantool.Interval(week=-int(MAX_WEEK_RANGE)),
            'msgpack': (b'\x02\x02\xd2\xdb\x6d\x85\xa8\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{week=-{int(MAX_WEEK_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{week=-{int(MAX_WEEK_RANGE)} + 1}}) - "
                         r"datetime.interval.new({week=1})",
            'str': f'tarantool.Interval(year=0, month=0, week=-{int(MAX_WEEK_RANGE)}, day=0, '
                   'hour=0, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'max_week_interval': {
            'python': tarantool.Interval(week=int(MAX_WEEK_RANGE)),
            'msgpack': (b'\x02\x02\xce\x24\x92\x7a\x58\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{week={int(MAX_WEEK_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{week={int(MAX_WEEK_RANGE)} - 1}}) + "
                         r"datetime.interval.new({week=1})",
            'str': f'tarantool.Interval(year=0, month=0, week={int(MAX_WEEK_RANGE)}, day=0, '
                   'hour=0, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'min_day_interval': {
            'python': tarantool.Interval(day=-int(MAX_DAY_RANGE)),
            'msgpack': (b'\x02\x03\xd3\xff\xff\xff\xfe\xff\xfe\xa7\x92\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{day=-{int(MAX_DAY_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{day=-{int(MAX_DAY_RANGE)} + 1}}) - "
                         r"datetime.interval.new({day=1})",
            'str': f'tarantool.Interval(year=0, month=0, week=0, day=-{int(MAX_DAY_RANGE)}, '
                   'hour=0, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'max_day_interval': {
            'python': tarantool.Interval(day=int(MAX_DAY_RANGE)),
            'msgpack': (b'\x02\x03\xcf\x00\x00\x00\x01\x00\x01\x58\x6e\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{day={int(MAX_DAY_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{day={int(MAX_DAY_RANGE)} - 1}}) + "
                         r"datetime.interval.new({day=1})",
            'str': f'tarantool.Interval(year=0, month=0, week=0, day={int(MAX_DAY_RANGE)}, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'min_int32_day_interval': {
            'python': tarantool.Interval(day=-2147483648),
            'msgpack': (b'\x02\x03\xd2\x80\x00\x00\x00\x08\x01'),
            'tarantool': r"datetime.interval.new({day=-2147483648})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=-2147483648, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'max_int32_day_interval': {
            'python': tarantool.Interval(day=2147483647),
            'msgpack': (b'\x02\x03\xce\x7f\xff\xff\xff\x08\x01'),
            'tarantool': r"datetime.interval.new({day=2147483647})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=2147483647, hour=0, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'min_hour_interval': {
            'python': tarantool.Interval(hour=-int(MAX_HOUR_RANGE)),
            'msgpack': (b'\x02\x04\xd3\xff\xff\xff\xe7\xff\xdf\xb5\xaa\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{hour=-{int(MAX_HOUR_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{hour=-{int(MAX_HOUR_RANGE)} + 1}}) - "
                         r"datetime.interval.new({hour=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, '
                   f'hour=-{int(MAX_HOUR_RANGE)}, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'max_hour_interval': {
            'python': tarantool.Interval(hour=int(MAX_HOUR_RANGE)),
            'msgpack': (b'\x02\x04\xcf\x00\x00\x00\x18\x00\x20\x4a\x56\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{hour={int(MAX_HOUR_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{hour={int(MAX_HOUR_RANGE)} - 1}}) + "
                         r"datetime.interval.new({hour=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, '
                   f'hour={int(MAX_HOUR_RANGE)}, minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'min_int32_hour_interval': {
            'python': tarantool.Interval(hour=-2147483648),
            'msgpack': (b'\x02\x04\xd2\x80\x00\x00\x00\x08\x01'),
            'tarantool': r"datetime.interval.new({hour=-2147483648})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=-2147483648, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'max_int32_hour_interval': {
            'python': tarantool.Interval(hour=2147483647),
            'msgpack': (b'\x02\x04\xce\x7f\xff\xff\xff\x08\x01'),
            'tarantool': r"datetime.interval.new({hour=2147483647})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=2147483647, '
                   'minute=0, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'min_minute_interval': {
            'python': tarantool.Interval(minute=-int(MAX_MIN_RANGE)),
            'msgpack': (b'\x02\x05\xd3\xff\xff\xfa\x5f\xf8\x6e\x93\xd8\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{min=-{int(MAX_MIN_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{min=-{int(MAX_MIN_RANGE)} + 1}}) - "
                         r"datetime.interval.new({min=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   f'minute=-{int(MAX_MIN_RANGE)}, sec=0, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'max_minute_interval': {
            'python': tarantool.Interval(minute=int(MAX_MIN_RANGE)),
            'msgpack': (b'\x02\x05\xcf\x00\x00\x05\xa0\x07\x91\x6c\x28\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{min={int(MAX_MIN_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{min={int(MAX_MIN_RANGE)} - 1}}) + "
                         r"datetime.interval.new({min=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   f'minute={int(MAX_MIN_RANGE)}, sec=0, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'min_int32_minute_interval': {
            'python': tarantool.Interval(minute=-2147483648),
            'msgpack': (b'\x02\x05\xd2\x80\x00\x00\x00\x08\x01'),
            'tarantool': r"datetime.interval.new({min=-2147483648})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   'minute=-2147483648, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'max_int32_minute_interval': {
            'python': tarantool.Interval(minute=2147483647),
            'msgpack': (b'\x02\x05\xce\x7f\xff\xff\xff\x08\x01'),
            'tarantool': r"datetime.interval.new({min=2147483647})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   'minute=2147483647, sec=0, nsec=0, adjust=Adjust.NONE)',
        },
        'min_sec_interval': {
            'python': tarantool.Interval(sec=-int(MAX_SEC_RANGE)),
            'msgpack': (b'\x02\x06\xd3\xff\xfe\xae\x7e\x39\xea\xa6\xa0\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{sec=-{int(MAX_SEC_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{sec=-{int(MAX_SEC_RANGE)} + 1}}) - "
                         r"datetime.interval.new({sec=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   f'minute=0, sec=-{int(MAX_SEC_RANGE)}, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'max_sec_interval': {
            'python': tarantool.Interval(sec=int(MAX_SEC_RANGE)),
            'msgpack': (b'\x02\x06\xcf\x00\x01\x51\x81\xc6\x15\x59\x60\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{sec={int(MAX_SEC_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{sec={int(MAX_SEC_RANGE)} - 1}}) + "
                         r"datetime.interval.new({sec=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   f'minute=0, sec={int(MAX_SEC_RANGE)}, nsec=0, adjust=Adjust.NONE)',
            'tarantool_8887_issue': True,
        },
        'min_int32_sec_interval': {
            'python': tarantool.Interval(sec=-2147483648),
            'msgpack': (b'\x02\x06\xd2\x80\x00\x00\x00\x08\x01'),
            'tarantool': r"datetime.interval.new({sec=-2147483648})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   'minute=0, sec=-2147483648, nsec=0, adjust=Adjust.NONE)',
        },
        'max_int32_sec_interval': {
            'python': tarantool.Interval(sec=2147483647),
            'msgpack': (b'\x02\x06\xce\x7f\xff\xff\xff\x08\x01'),
            'tarantool': r"datetime.interval.new({sec=2147483647})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   'minute=0, sec=2147483647, nsec=0, adjust=Adjust.NONE)',
        },
        'min_nsec_interval': {
            'python': tarantool.Interval(nsec=-int(MAX_NSEC_RANGE)),
            'msgpack': (b'\x02\x07\xd2\x80\x00\x00\x01\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{nsec=-{int(MAX_NSEC_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{nsec=-{int(MAX_NSEC_RANGE)} + 1}}) - "
                         r"datetime.interval.new({nsec=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   f'minute=0, sec=0, nsec=-{int(MAX_NSEC_RANGE)}, adjust=Adjust.NONE)',
        },
        'max_nsec_interval': {
            'python': tarantool.Interval(nsec=int(MAX_NSEC_RANGE)),
            'msgpack': (b'\x02\x07\xce\x7f\xff\xff\xff\x08\x01'),
            # Reason why Tarantool datetime is so weird here:
            # https://github.com/tarantool/tarantool/issues/8878
            # Replace with f"datetime.interval.new({{nsec={int(MAX_NSEC_RANGE)}}})
            # after fix.
            'tarantool': f"datetime.interval.new({{nsec={int(MAX_NSEC_RANGE)} - 1}}) + "
                         r"datetime.interval.new({nsec=1})",
            'str': 'tarantool.Interval(year=0, month=0, week=0, day=0, hour=0, '
                   f'minute=0, sec=0, nsec={int(MAX_NSEC_RANGE)}, adjust=Adjust.NONE)',
        },
    }

    def test_msgpack_decode(self):
        for name, case in self.cases.items():
            with self.subTest(msg=name):
                self.assertEqual(
                    unpacker_ext_hook(
                        6,
                        case['msgpack'],
                        self.con._unpacker_factory(),
                    ),
                    case['python'])

    @skip_or_run_datetime_test
    def test_tarantool_decode(self):
        for name, case in self.cases.items():
            with self.subTest(msg=name):
                if ('tarantool_8887_issue' in case) and (case['tarantool_8887_issue'] is True):
                    self.skipTest('See https://github.com/tarantool/tarantool/issues/8887')

                self.adm(f"box.space['test']:replace{{'{name}', {case['tarantool']}, 'field'}}")

                self.assertSequenceEqual(self.con.select('test', name),
                                         [[name, case['python'], 'field']])

    def test_msgpack_encode(self):
        for name, case in self.cases.items():
            with self.subTest(msg=name):
                self.assertEqual(packer_default(case['python']),
                                 msgpack.ExtType(code=6, data=case['msgpack']))

    @skip_or_run_datetime_test
    def test_tarantool_encode(self):
        for name, case in self.cases.items():
            with self.subTest(msg=name):
                if ('tarantool_8887_issue' in case) and (case['tarantool_8887_issue'] is True):
                    self.skipTest('See https://github.com/tarantool/tarantool/issues/8887')

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

    def test_class_string(self):
        for name, case in self.cases.items():
            with self.subTest(msg=name):
                self.assertEqual(str(case['python']), case['str'])
                self.assertEqual(repr(case['python']), case['str'])

    def test_unknown_field_decode(self):
        case = b'\x01\x09\xce\x00\x98\x96\x80'
        self.assertRaisesRegex(
            MsgpackError, 'Unknown interval field id 9',
            lambda: unpacker_ext_hook(6, case, self.con._unpacker_factory()))

    def test_unknown_adjust_decode(self):
        case = b'\x02\x07\xce\x00\x98\x96\x80\x08\x03'
        self.assertRaisesRegex(
            MsgpackError, '3 is not a valid Adjust',
            lambda: unpacker_ext_hook(6, case, self.con._unpacker_factory()))

    out_of_range_cases = {
        'year_too_small': {
            'kwargs': {'year': -int(MAX_YEAR_RANGE + 1)},
            'range': MAX_YEAR_RANGE,
        },
        'year_too_large': {
            'kwargs': {'year': int(MAX_YEAR_RANGE + 1)},
            'range': MAX_YEAR_RANGE,
        },
        'month_too_small': {
            'kwargs': {'month': -int(MAX_MONTH_RANGE + 1)},
            'range': MAX_MONTH_RANGE,
        },
        'month_too_big': {
            'kwargs': {'month': int(MAX_MONTH_RANGE + 1)},
            'range': MAX_MONTH_RANGE,
        },
        'week_too_small': {
            'kwargs': {'week': -int(MAX_WEEK_RANGE + 1)},
            'range': MAX_WEEK_RANGE,
        },
        'week_too_big': {
            'kwargs': {'week': int(MAX_WEEK_RANGE + 1)},
            'range': MAX_WEEK_RANGE,
        },
        'day_too_small': {
            'kwargs': {'day': -int(MAX_DAY_RANGE + 1)},
            'range': MAX_DAY_RANGE,
        },
        'day_too_big': {
            'kwargs': {'day': int(MAX_DAY_RANGE + 1)},
            'range': MAX_DAY_RANGE,
        },
        'hour_too_small': {
            'kwargs': {'hour': -int(MAX_HOUR_RANGE + 1)},
            'range': MAX_HOUR_RANGE,
        },
        'hour_too_big': {
            'kwargs': {'hour': int(MAX_HOUR_RANGE + 1)},
            'range': MAX_HOUR_RANGE,
        },
        'minute_too_small': {
            'kwargs': {'minute': -int(MAX_MIN_RANGE + 1)},
            'range': MAX_MIN_RANGE,
        },
        'minute_too_big': {
            'kwargs': {'minute': int(MAX_MIN_RANGE + 1)},
            'range': MAX_MIN_RANGE,
        },
        'sec_too_small': {
            'kwargs': {'sec': -int(MAX_SEC_RANGE + 1)},
            'range': MAX_SEC_RANGE,
        },
        'sec_too_big': {
            'kwargs': {'sec': int(MAX_SEC_RANGE + 1)},
            'range': MAX_SEC_RANGE,
        },
        'nsec_too_small': {
            'kwargs': {'nsec': -int(MAX_NSEC_RANGE + 1)},
            'range': MAX_NSEC_RANGE,
        },
        'nsec_too_big': {
            'kwargs': {'nsec': int(MAX_NSEC_RANGE + 1)},
            'range': MAX_NSEC_RANGE,
        },
    }

    def test_out_of_range(self):
        # pylint: disable=cell-var-from-loop

        for name, case in self.out_of_range_cases.items():
            with self.subTest(msg=name):
                name = next(iter(case['kwargs']))
                val = case['kwargs'][name]
                self.assertRaisesRegex(
                    ValueError, re.escape(
                        f"value {val} of {name} is out of "
                        f"allowed range [{-case['range']}, {case['range']}]"
                    ),
                    lambda: tarantool.Interval(**case['kwargs']))

    arithmetic_cases = {
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
        'weeks': {
            'arg_1': tarantool.Interval(week=2),
            'arg_2': tarantool.Interval(week=1),
            'res_add': tarantool.Interval(week=3),
            'res_sub': tarantool.Interval(week=1),
        },
        'date_with_week': {
            'arg_1': tarantool.Interval(year=1, month=2, week=3, day=4),
            'arg_2': tarantool.Interval(year=4, month=3, week=2, day=1),
            'res_add': tarantool.Interval(year=5, month=5, week=5, day=5),
            'res_sub': tarantool.Interval(year=-3, month=-1, week=1, day=3),
        },
        'datetime_with_week': {
            'arg_1': tarantool.Interval(year=1, month=2, week=3, day=4, hour=1, minute=2,
                                        sec=3000, nsec=10000000),
            'arg_2': tarantool.Interval(year=2, month=1, week=-1, day=31, hour=-3, minute=0,
                                        sec=1000, nsec=9876543),
            'res_add': tarantool.Interval(year=3, month=3, week=2, day=35, hour=-2, minute=2,
                                          sec=4000, nsec=19876543),
            'res_sub': tarantool.Interval(year=-1, month=1, week=4, day=-27, hour=4, minute=2,
                                          sec=2000, nsec=123457),
        },
    }

    def test_python_interval_addition(self):
        for name, case in self.arithmetic_cases.items():
            with self.subTest(msg=name):
                self.assertEqual(case['arg_1'] + case['arg_2'], case['res_add'])

    def test_python_interval_subtraction(self):
        for name, case in self.arithmetic_cases.items():
            with self.subTest(msg=name):
                self.assertEqual(case['arg_1'] - case['arg_2'], case['res_sub'])

    @skip_or_run_datetime_test
    def test_tarantool_interval_addition(self):
        for name, case in self.arithmetic_cases.items():
            with self.subTest(msg=name):
                self.assertSequenceEqual(self.con.call('add', case['arg_1'], case['arg_2']),
                                         [case['res_add']])

    @skip_or_run_datetime_test
    def test_tarantool_interval_subtraction(self):
        for name, case in self.arithmetic_cases.items():
            with self.subTest(msg=name):
                self.assertSequenceEqual(self.con.call('sub', case['arg_1'], case['arg_2']),
                                         [case['res_sub']])

    def test_addition_overflow(self):
        self.assertRaisesRegex(
            ValueError, re.escape(
                f"value {int(MAX_YEAR_RANGE) + 1} of year is out of "
                f"allowed range [{-MAX_YEAR_RANGE}, {MAX_YEAR_RANGE}]"
            ),
            lambda: tarantool.Interval(year=int(MAX_YEAR_RANGE)) + tarantool.Interval(year=1))

    def test_subtraction_overflow(self):
        self.assertRaisesRegex(
            ValueError, re.escape(
                f"value {-int(MAX_YEAR_RANGE) - 1} of year is out of "
                f"allowed range [{-MAX_YEAR_RANGE}, {MAX_YEAR_RANGE}]"
            ),
            lambda: tarantool.Interval(year=-int(MAX_YEAR_RANGE)) - tarantool.Interval(year=1))

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        cls.srv.stop()
        cls.srv.clean()
