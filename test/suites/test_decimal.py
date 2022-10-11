import sys
import unittest
import decimal
import msgpack
import warnings
import tarantool

from tarantool.msgpack_ext.packer import default as packer_default
from tarantool.msgpack_ext.unpacker import ext_hook as unpacker_ext_hook

from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_decimal_test
from tarantool.error import MsgpackError, MsgpackWarning

class TestSuite_Decimal(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' DECIMAL EXT TYPE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()

        self.adm = self.srv.admin
        self.adm(r"""
            _, decimal = pcall(require, 'decimal')

            box.schema.space.create('test')
            box.space['test']:create_index('primary', {
                type = 'tree',
                parts = {1, 'string'},
                unique = true})

            pcall(function()
                box.schema.space.create('test_pk')
                box.space['test_pk']:create_index('primary', {
                    type = 'tree',
                    parts = {1, 'decimal'},
                    unique = true})
            end)

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


    valid_cases = {
        'simple_decimal_1': {
            'python': decimal.Decimal('0.7'),
            'msgpack': (b'\x01\x7c'),
            'tarantool': "decimal.new('0.7')",
        },
        'simple_decimal_2': {
            'python': decimal.Decimal('0.3'),
            'msgpack': (b'\x01\x3c'),
            'tarantool': "decimal.new('0.3')",
        },
        'simple_decimal_3': {
            'python': decimal.Decimal('-18.34'),
            'msgpack': (b'\x02\x01\x83\x4d'),
            'tarantool': "decimal.new('-18.34')",
        },
        'simple_decimal_4': {
            'python': decimal.Decimal('-108.123456789'),
            'msgpack': (b'\x09\x01\x08\x12\x34\x56\x78\x9d'),
            'tarantool': "decimal.new('-108.123456789')",
        },
        'simple_decimal_5': {
            'python': decimal.Decimal('100'),
            'msgpack': (b'\x00\x10\x0c'),
            'tarantool': "decimal.new('100')",
        },
        'simple_decimal_6': {
            'python': decimal.Decimal('0.1'),
            'msgpack': (b'\x01\x1c'),
            'tarantool': "decimal.new('0.1')",
        },
        'simple_decimal_7': {
            'python': decimal.Decimal('-0.1'),
            'msgpack': (b'\x01\x1d'),
            'tarantool': "decimal.new('-0.1')",
        },
        'simple_decimal_8': {
            'python': decimal.Decimal('-12.34'),
            'msgpack': (b'\x02\x01\x23\x4d'),
            'tarantool': "decimal.new('-12.34')",
        },
        'simple_decimal_9': {
            'python': decimal.Decimal('12.34'),
            'msgpack': (b'\x02\x01\x23\x4c'),
            'tarantool': "decimal.new('12.34')",
        },
        'simple_decimal_10': {
            'python': decimal.Decimal('1.4'),
            'msgpack': (b'\x01\x01\x4c'),
            'tarantool': "decimal.new('1.4')",
        },
        'simple_decimal_11': {
            'python': decimal.Decimal('2.718281828459045'),
            'msgpack': (b'\x0f\x02\x71\x82\x81\x82\x84\x59\x04\x5c'),
            'tarantool': "decimal.new('2.718281828459045')",
        },
        'simple_decimal_12': {
            'python': decimal.Decimal('-2.718281828459045'),
            'msgpack': (b'\x0f\x02\x71\x82\x81\x82\x84\x59\x04\x5d'),
            'tarantool': "decimal.new('-2.718281828459045')",
        },
        'simple_decimal_13': {
            'python': decimal.Decimal('3.141592653589793'),
            'msgpack': (b'\x0f\x03\x14\x15\x92\x65\x35\x89\x79\x3c'),
            'tarantool': "decimal.new('3.141592653589793')",
        },
        'simple_decimal_14': {
            'python': decimal.Decimal('-3.141592653589793'),
            'msgpack': (b'\x0f\x03\x14\x15\x92\x65\x35\x89\x79\x3d'),
            'tarantool': "decimal.new('-3.141592653589793')",
        },
        'simple_decimal_15': {
            'python': decimal.Decimal('1'),
            'msgpack': (b'\x00\x1c'),
            'tarantool': "decimal.new('1')",
        },
        'simple_decimal_16': {
            'python': decimal.Decimal('-1'),
            'msgpack': (b'\x00\x1d'),
            'tarantool': "decimal.new('-1')",
        },
        'simple_decimal_17': {
            'python': decimal.Decimal('0'),
            'msgpack': (b'\x00\x0c'),
            'tarantool': "decimal.new('0')",
        },
        'simple_decimal_18': {
            'python': decimal.Decimal('-0'),
            'msgpack': (b'\x00\x0d'),
            'tarantool': "decimal.new('-0')",
        },
        'simple_decimal_19': {
            'python': decimal.Decimal('0.01'),
            'msgpack': (b'\x02\x1c'),
            'tarantool': "decimal.new('0.01')",
        },
        'simple_decimal_20': {
            'python': decimal.Decimal('0.001'),
            'msgpack': (b'\x03\x1c'),
            'tarantool': "decimal.new('0.001')",
        },
        'decimal_limits_1': {
            'python': decimal.Decimal('11111111111111111111111111111111111111'),
            'msgpack': (b'\x00\x01\x11\x11\x11\x11\x11\x11\x11\x11\x11' +
                        b'\x11\x11\x11\x11\x11\x11\x11\x11\x11\x1c'),
            'tarantool': "decimal.new('11111111111111111111111111111111111111')",
        },
        'decimal_limits_2': {
            'python': decimal.Decimal('-11111111111111111111111111111111111111'),
            'msgpack': (b'\x00\x01\x11\x11\x11\x11\x11\x11\x11\x11\x11' +
                        b'\x11\x11\x11\x11\x11\x11\x11\x11\x11\x1d'),
            'tarantool': "decimal.new('-11111111111111111111111111111111111111')",
        },
        'decimal_limits_3': {
            'python': decimal.Decimal('0.0000000000000000000000000000000000001'),
            'msgpack': (b'\x25\x1c'),
            'tarantool': "decimal.new('0.0000000000000000000000000000000000001')",
        },
        'decimal_limits_4': {
            'python': decimal.Decimal('-0.0000000000000000000000000000000000001'),
            'msgpack': (b'\x25\x1d'),
            'tarantool': "decimal.new('-0.0000000000000000000000000000000000001')",
        },
        'decimal_limits_5': {
            'python': decimal.Decimal('0.00000000000000000000000000000000000001'),
            'msgpack': (b'\x26\x1c'),
            'tarantool': "decimal.new('0.00000000000000000000000000000000000001')",
        },
        'decimal_limits_6': {
            'python': decimal.Decimal('-0.00000000000000000000000000000000000001'),
            'msgpack': (b'\x26\x1d'),
            'tarantool': "decimal.new('-0.00000000000000000000000000000000000001')",
        },
        'decimal_limits_7': {
            'python': decimal.Decimal('0.00000000000000000000000000000000000009'),
            'msgpack': (b'\x26\x9c'),
            'tarantool': "decimal.new('0.00000000000000000000000000000000000009')",
        },
        'decimal_limits_8': {
            'python': decimal.Decimal('0.00000000000000000000000000000000000009'),
            'msgpack': (b'\x26\x9c'),
            'tarantool': "decimal.new('0.00000000000000000000000000000000000009')",
        },
        'decimal_limits_9': {
            'python': decimal.Decimal('99999999999999999999999999999999999999'),
            'msgpack': (b'\x00\x09\x99\x99\x99\x99\x99\x99\x99\x99\x99' +
                        b'\x99\x99\x99\x99\x99\x99\x99\x99\x99\x9c'),
            'tarantool': "decimal.new('99999999999999999999999999999999999999')",
        },
        'decimal_limits_10': {
            'python': decimal.Decimal('-99999999999999999999999999999999999999'),
            'msgpack': (b'\x00\x09\x99\x99\x99\x99\x99\x99\x99\x99\x99' +
                        b'\x99\x99\x99\x99\x99\x99\x99\x99\x99\x9d'),
            'tarantool': "decimal.new('-99999999999999999999999999999999999999')",
        },
        'decimal_limits_11': {
            'python': decimal.Decimal('1234567891234567890.0987654321987654321'),
            'msgpack': (b'\x13\x01\x23\x45\x67\x89\x12\x34\x56\x78\x90' +
                        b'\x09\x87\x65\x43\x21\x98\x76\x54\x32\x1c'),
            'tarantool': "decimal.new('1234567891234567890.0987654321987654321')",
        },
        'decimal_limits_12': {
            'python': decimal.Decimal('-1234567891234567890.0987654321987654321'),
            'msgpack': (b'\x13\x01\x23\x45\x67\x89\x12\x34\x56\x78\x90' +
                        b'\x09\x87\x65\x43\x21\x98\x76\x54\x32\x1d'),
            'tarantool': "decimal.new('-1234567891234567890.0987654321987654321')",
        },
    }

    def test_msgpack_decode(self):
        for name in self.valid_cases.keys():
            with self.subTest(msg=name):
                case = self.valid_cases[name]

                self.assertEqual(unpacker_ext_hook(1, case['msgpack']),
                                 case['python'])

    @skip_or_run_decimal_test
    def test_tarantool_decode(self):
        for name in self.valid_cases.keys():
            with self.subTest(msg=name):
                case = self.valid_cases[name]

                self.adm(f"box.space['test']:replace{{'{name}', {case['tarantool']}}}")

                self.assertSequenceEqual(
                    self.con.select('test', name),
                    [[name, case['python']]])

    def test_msgpack_encode(self):
        for name in self.valid_cases.keys():
            with self.subTest(msg=name):
                case = self.valid_cases[name]

                self.assertEqual(packer_default(case['python']),
                                 msgpack.ExtType(code=1, data=case['msgpack']))

    @skip_or_run_decimal_test
    def test_tarantool_encode(self):
        for name in self.valid_cases.keys():
            with self.subTest(msg=name):
                case = self.valid_cases[name]

                self.con.insert('test', [name, case['python']])

                lua_eval = f"""
                    local tuple = box.space['test']:get('{name}')
                    assert(tuple ~= nil)

                    local dec = {case['tarantool']}
                    if tuple[2] == dec then
                        return true
                    else
                        return nil, ('%s is not equal to expected %s'):format(
                            tostring(tuple[2]), tostring(dec))
                    end
                """

                self.assertSequenceEqual(self.con.eval(lua_eval), [True])


    error_cases = {
        'decimal_limit_break_head_1': {
            'python': decimal.Decimal('999999999999999999999999999999999999999'),
        },
        'decimal_limit_break_head_2': {
            'python': decimal.Decimal('-999999999999999999999999999999999999999'),
        },
        'decimal_limit_break_head_3': {
            'python': decimal.Decimal('999999999999999999900000099999999999999999999'),
        },
        'decimal_limit_break_head_4': {
            'python': decimal.Decimal('-999999999999999999900000099999999999999999999'),
        },
        'decimal_limit_break_head_5': {
            'python': decimal.Decimal('100000000000000000000000000000000000000.1'),
        },
        'decimal_limit_break_head_6': {
            'python': decimal.Decimal('-100000000000000000000000000000000000000.1'),
        },
        'decimal_limit_break_head_7': {
            'python': decimal.Decimal('1000000000000000000011110000000000000000000.1'),
        },
        'decimal_limit_break_head_8': {
            'python': decimal.Decimal('-1000000000000000000011110000000000000000000.1'),
        },
    }

    def test_msgpack_encode_error(self):
        for name in self.error_cases.keys():
            with self.subTest(msg=name):
                case = self.error_cases[name]

                msg = 'Decimal cannot be encoded: Tarantool decimal ' + \
                      'supports a maximum of 38 digits.'
                self.assertRaisesRegex(
                    MsgpackError, msg,
                    lambda: packer_default(case['python']))

    @skip_or_run_decimal_test
    def test_tarantool_encode_error(self):
        for name in self.error_cases.keys():
            with self.subTest(msg=name):
                case = self.error_cases[name]

                msg = 'Decimal cannot be encoded: Tarantool decimal ' + \
                      'supports a maximum of 38 digits.'
                self.assertRaisesRegex(
                    MsgpackError, msg,
                    lambda: self.con.insert('test', [name, case['python']]))


    precision_loss_cases = {
        'decimal_limit_break_tail_1': {
            'python': decimal.Decimal('1.00000000000000000000000000000000000001'),
            'msgpack': (b'\x00\x1c'),
            'tarantool': "decimal.new('1')",
        },
        'decimal_limit_break_tail_2': {
            'python': decimal.Decimal('-1.00000000000000000000000000000000000001'),
            'msgpack': (b'\x00\x1d'),
            'tarantool': "decimal.new('-1')",
        },
        'decimal_limit_break_tail_3': {
            'python': decimal.Decimal('0.000000000000000000000000000000000000001'),
            'msgpack': (b'\x00\x0c'),
            'tarantool': "decimal.new('0.000000000000000000000000000000000000001')",
        },
        'decimal_limit_break_tail_4': {
            'python': decimal.Decimal('-0.000000000000000000000000000000000000001'),
            'msgpack': (b'\x00\x0d'),
            'tarantool': "decimal.new('-0.000000000000000000000000000000000000001')",
        },
        'decimal_limit_break_tail_5': {
            'python': decimal.Decimal('9999999.99999900000000000000000000000000000000000001'),
            'msgpack': (b'\x06\x99\x99\x99\x99\x99\x99\x9c'),
            'tarantool': "decimal.new('9999999.999999')",
        },
        'decimal_limit_break_tail_6': {
            'python': decimal.Decimal('-9999999.99999900000000000000000000000000000000000001'),
            'msgpack': (b'\x06\x99\x99\x99\x99\x99\x99\x9d'),
            'tarantool': "decimal.new('-9999999.999999')",
        },
        'decimal_limit_break_tail_7': {
            'python': decimal.Decimal('99999999999999999999999999999999999999.1'),
            'msgpack': (b'\x00\x09\x99\x99\x99\x99\x99\x99\x99\x99\x99' +
                        b'\x99\x99\x99\x99\x99\x99\x99\x99\x99\x9c'),
            'tarantool': "decimal.new('99999999999999999999999999999999999999')",
        },
        'decimal_limit_break_tail_8': {
            'python': decimal.Decimal('-99999999999999999999999999999999999999.1'),
            'msgpack': (b'\x00\x09\x99\x99\x99\x99\x99\x99\x99\x99\x99' +
                        b'\x99\x99\x99\x99\x99\x99\x99\x99\x99\x9d'),
            'tarantool': "decimal.new('-99999999999999999999999999999999999999')",
        },
        'decimal_limit_break_tail_9': {
            'python': decimal.Decimal('99999999999999999999999999999999999999.1111111111111111111111111'),
            'msgpack': (b'\x00\x09\x99\x99\x99\x99\x99\x99\x99\x99\x99' +
                        b'\x99\x99\x99\x99\x99\x99\x99\x99\x99\x9c'),
            'tarantool': "decimal.new('99999999999999999999999999999999999999')",
        },
        'decimal_limit_break_tail_10': {
            'python': decimal.Decimal('-99999999999999999999999999999999999999.1111111111111111111111111'),
            'msgpack': (b'\x00\x09\x99\x99\x99\x99\x99\x99\x99\x99\x99' +
                        b'\x99\x99\x99\x99\x99\x99\x99\x99\x99\x9d'),
            'tarantool': "decimal.new('-99999999999999999999999999999999999999')",
        },
    }

    def test_msgpack_encode_with_precision_loss(self):
        for name in self.precision_loss_cases.keys():
            with self.subTest(msg=name):
                case = self.precision_loss_cases[name]

                msg = 'Decimal encoded with loss of precision: ' + \
                      'Tarantool decimal supports a maximum of 38 digits.'

                self.assertWarnsRegex(
                    MsgpackWarning, msg,
                    lambda: self.assertEqual(
                                packer_default(case['python']),
                                msgpack.ExtType(code=1, data=case['msgpack'])
                            )
                    )
                

    @skip_or_run_decimal_test
    def test_tarantool_encode_with_precision_loss(self):
        for name in self.precision_loss_cases.keys():
            with self.subTest(msg=name):
                case = self.precision_loss_cases[name]

                msg = 'Decimal encoded with loss of precision: ' + \
                      'Tarantool decimal supports a maximum of 38 digits.'

                self.assertWarnsRegex(
                    MsgpackWarning, msg,
                    lambda: self.con.insert('test', [name, case['python']]))

                lua_eval = f"""
                    local tuple = box.space['test']:get('{name}')
                    assert(tuple ~= nil)

                    local dec = {case['tarantool']}
                    if tuple[2] == dec then
                        return true
                    else
                        return nil, ('%s is not equal to expected %s'):format(
                            tostring(tuple[2]), tostring(dec))
                    end
                """

                self.assertSequenceEqual(self.con.eval(lua_eval), [True])


    @skip_or_run_decimal_test
    def test_primary_key(self):
        data = [decimal.Decimal('0'), 'content']

        self.assertSequenceEqual(self.con.insert('test_pk', data), [data])
        self.assertSequenceEqual(self.con.select('test_pk', data[0]), [data])


    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()
