import sys
import unittest

import tarantool
from tarantool.error import DatabaseError

from .lib.skip import skip_or_run_varbinary_test, skip_or_run_error_extra_info_test
from .lib.tarantool_server import TarantoolServer

class TestSuite_Encoding(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' ENCODING '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()

        self.srv.admin("""
            box.schema.user.create('test', { password = 'test' })
            box.schema.user.grant('test', 'execute,read,write', 'universe')
        """)

        args = [self.srv.host, self.srv.args['primary']]
        kwargs = { 'user': 'test', 'password': 'test' }
        self.con_encoding_utf8 = tarantool.Connection(*args, encoding='utf-8', **kwargs)
        self.con_encoding_none = tarantool.Connection(*args, encoding=None, **kwargs)
        self.conns = [self.con_encoding_utf8, self.con_encoding_none]

        self.srv.admin("box.schema.create_space('space_str')")
        self.srv.admin("""
            box.space['space_str']:create_index('primary', {
                type = 'tree',
                parts = {1, 'str'},
                unique = true})
        """.replace('\n', ' '))

        self.srv.admin("box.schema.create_space('space_varbin')")
        self.srv.admin(r"""
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
        self.srv.admin("""
            box.space['space_varbin']:create_index('id', {
                type = 'tree',
                parts = {1, 'number'},
                unique = true})
        """.replace('\n', ' '))
        self.srv.admin("""
            box.space['space_varbin']:create_index('varbin', {
                type = 'tree',
                parts = {2, 'varbinary'},
                unique = true})
        """.replace('\n', ' '))

    def assertNotRaises(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            self.fail('Function raised Exception: %s' % repr(e))

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

        self.srv.admin("box.space['%s']:insert{'%s'}" % (space, data))

        resp = self.con_encoding_utf8.eval("return box.space['%s']:get('%s')" % (space, data))
        self.assertSequenceEqual(resp, [[data]])

    @skip_or_run_varbinary_test
    def test_01_03_bytes_encode_for_encoding_utf8_behavior(self):
        data_id = 103
        data = bytes(bytearray.fromhex('DEADBEAF0103'))
        space = 'space_varbin'

        self.assertNotRaises(self.con_encoding_utf8.insert, space, [data_id, data])

        resp = self.con_encoding_utf8.select(space, [ data ], index='varbin')
        self.assertSequenceEqual(resp, [[data_id, data]])

    @skip_or_run_varbinary_test
    def test_01_04_varbinary_decode_for_encoding_utf8_behavior(self):
        data_id = 104
        data_hex = 'DEADBEAF0104'
        data = bytes(bytearray.fromhex(data_hex))
        space = 'space_varbin'

        self.con_encoding_utf8.execute("""
            INSERT INTO "%s" VALUES (%d, x'%s');
        """ % (space, data_id, data_hex))

        resp = self.con_encoding_utf8.execute("""
            SELECT * FROM "%s" WHERE "varbin" == x'%s';
        """ % (space, data_hex))
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

        self.srv.admin("box.space['%s']:insert{'%s'}" % (space, data))

        resp = self.con_encoding_none.eval("return box.space['%s']:get('%s')" % (space, data))
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

        self.con_encoding_none.execute("""
            INSERT INTO "%s" VALUES (%d, x'%s');
        """ % (space, data_id, data_hex))

        resp = self.con_encoding_none.execute("""
            SELECT * FROM "%s" WHERE "varbin" == x'%s';
        """ % (space, data_hex))
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
    def tearDownClass(self):
        for con in self.conns:
            con.close()
        self.srv.stop()
        self.srv.clean()
