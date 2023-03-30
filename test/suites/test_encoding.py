"""
This module tests various type encoding cases.
"""
# pylint: disable=missing-class-docstring,missing-function-docstring

import sys
import unittest

import tarantool
from tarantool.error import DatabaseError

from .lib.skip import skip_or_run_varbinary_test, skip_or_run_error_extra_info_test
from .lib.tarantool_server import TarantoolServer


class TestSuiteEncoding(unittest.TestCase):
    # pylint: disable=invalid-name

    @classmethod
    def setUpClass(cls):
        print(' ENCODING '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        cls.srv = TarantoolServer()
        cls.srv.script = 'test/suites/box.lua'
        cls.srv.start()

        cls.srv.admin("""
            box.schema.user.create('test', { password = 'test' })
            box.schema.user.grant('test', 'execute,read,write', 'universe')
        """)

        args = [cls.srv.host, cls.srv.args['primary']]
        kwargs = {'user': 'test', 'password': 'test'}
        cls.con_encoding_utf8 = tarantool.Connection(*args, encoding='utf-8', **kwargs)
        cls.con_encoding_none = tarantool.Connection(*args, encoding=None, **kwargs)
        cls.conns = [cls.con_encoding_utf8, cls.con_encoding_none]

        cls.srv.admin("box.schema.create_space('space_str')")
        cls.srv.admin("""
            box.space['space_str']:create_index('primary', {
                type = 'tree',
                parts = {1, 'str'},
                unique = true})
        """.replace('\n', ' '))

        cls.srv.admin("box.schema.create_space('space_varbin')")
        cls.srv.admin(r"""
            box.space['space_varbin']:format({
                {
                    'id',
                    type = 'number',
                    is_nullable = false
                },
                {
                    'varbin',
                    type = 'varbinary',
                    is_nullable = false,
                }
            })
        """.replace('\n', ' '))
        cls.srv.admin("""
            box.space['space_varbin']:create_index('id', {
                type = 'tree',
                parts = {1, 'number'},
                unique = true})
        """.replace('\n', ' '))
        cls.srv.admin("""
            box.space['space_varbin']:create_index('varbin', {
                type = 'tree',
                parts = {2, 'varbinary'},
                unique = true})
        """.replace('\n', ' '))

    def assertNotRaises(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as exc:  # pylint: disable=bad-option-value,broad-exception-caught,broad-except
            self.fail(f'Function raised Exception: {repr(exc)}')

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

    # encoding = 'utf-8'
    #
    # Python 3 -> Tarantool          -> Python 3
    # str      -> mp_str (string)    -> str
    # bytes    -> mp_bin (varbinary) -> bytes
    def test_01_01_str_encode_for_encoding_utf8_behavior(self):
        data = 'test_01_01'
        space = 'space_str'

        self.assertNotRaises(self.con_encoding_utf8.insert, space, [data])

        resp = self.con_encoding_utf8.select(space, [data])
        self.assertSequenceEqual(resp, [[data]])

    def test_01_02_string_decode_for_encoding_utf8_behavior(self):
        data = 'test_01_02'
        space = 'space_str'

        self.srv.admin(f"box.space['{space}']:insert{{'{data}'}}")

        resp = self.con_encoding_utf8.eval(f"return box.space['{space}']:get('{data}')")
        self.assertSequenceEqual(resp, [[data]])

    @skip_or_run_varbinary_test
    def test_01_03_bytes_encode_for_encoding_utf8_behavior(self):
        data_id = 103
        data = bytes(bytearray.fromhex('DEADBEAF0103'))
        space = 'space_varbin'

        self.assertNotRaises(self.con_encoding_utf8.insert, space, [data_id, data])

        resp = self.con_encoding_utf8.select(space, [data], index='varbin')
        self.assertSequenceEqual(resp, [[data_id, data]])

    @skip_or_run_varbinary_test
    def test_01_04_varbinary_decode_for_encoding_utf8_behavior(self):
        data_id = 104
        data_hex = 'DEADBEAF0104'
        data = bytes(bytearray.fromhex(data_hex))
        space = 'space_varbin'

        self.con_encoding_utf8.execute(f"""
            INSERT INTO "{space}" VALUES ({data_id}, x'{data_hex}');
        """)

        resp = self.con_encoding_utf8.execute(f"""
            SELECT * FROM "{space}" WHERE "varbin" == x'{data_hex}';
        """)
        self.assertSequenceEqual(resp, [[data_id, data]])

    # encoding = None
    #
    # Python 3 -> Tarantool          -> Python 3
    # bytes    -> mp_str (string)    -> bytes
    # str      -> mp_str (string)    -> bytes
    #             mp_bin (varbinary) -> bytes
    def test_02_01_str_encode_for_encoding_none_behavior(self):
        data = 'test_02_01'
        space = 'space_str'

        self.assertNotRaises(self.con_encoding_none.insert, space, [data])

        resp = self.con_encoding_utf8.select(space, [data])
        self.assertSequenceEqual(resp, [[data]])

    def test_02_02_string_decode_for_encoding_none_behavior(self):
        data = 'test_02_02'
        data_decoded = b'test_02_02'
        space = 'space_str'

        self.srv.admin(f"box.space['{space}']:insert{{'{data}'}}")

        resp = self.con_encoding_none.eval(f"return box.space['{space}']:get('{data}')")
        self.assertSequenceEqual(resp, [[data_decoded]])

    def test_02_03_bytes_encode_for_encoding_none_behavior(self):
        data = b'test_02_03'
        space = 'space_str'

        self.assertNotRaises(self.con_encoding_none.insert, space, [data])

        resp = self.con_encoding_none.select(space, [data])
        self.assertSequenceEqual(resp, [[data]])

    @skip_or_run_varbinary_test
    def test_02_04_varbinary_decode_for_encoding_none_behavior(self):
        data_id = 204
        data_hex = 'DEADBEAF0204'
        data = bytes(bytearray.fromhex(data_hex))
        space = 'space_varbin'

        self.con_encoding_none.execute(f"""
            INSERT INTO "{space}" VALUES ({data_id}, x'{data_hex}');
        """)

        resp = self.con_encoding_none.execute(f"""
            SELECT * FROM "{space}" WHERE "varbin" == x'{data_hex}';
        """)
        self.assertSequenceEqual(resp, [[data_id, data]])

    @skip_or_run_error_extra_info_test
    def test_01_05_error_extra_info_decode_for_encoding_utf8_behavior(self):
        try:
            self.con_encoding_utf8.eval("not a Lua code")
        except DatabaseError as exc:
            self.assertEqual(exc.extra_info.type, 'LuajitError')
            self.assertEqual(exc.extra_info.message, "eval:1: unexpected symbol near 'not'")
        else:
            self.fail('Expected error')

    @skip_or_run_error_extra_info_test
    def test_02_05_error_extra_info_decode_for_encoding_none_behavior(self):
        try:
            self.con_encoding_none.eval("not a Lua code")
        except DatabaseError as exc:
            self.assertEqual(exc.extra_info.type, b'LuajitError')
            self.assertEqual(exc.extra_info.message, b"eval:1: unexpected symbol near 'not'")
        else:
            self.fail('Expected error')

    @classmethod
    def tearDownClass(cls):
        for con in cls.conns:
            con.close()
        cls.srv.stop()
        cls.srv.clean()
