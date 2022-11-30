import sys
import unittest

import decimal
import msgpack

import tarantool
import tarantool.msgpack_ext.decimal as ext_decimal

from .lib.skip import skip_or_run_decimal_test, skip_or_run_varbinary_test
from .lib.tarantool_server import TarantoolServer

class TestSuite_Connection(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' CONNECTION '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()

        self.adm = self.srv.admin
        self.adm(r"""
            box.schema.user.create('test', {password = 'test', if_not_exists = true})
            box.schema.user.grant('test', 'read,write,execute', 'universe')

            box.schema.create_space('space_varbin')

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

            box.space['space_varbin']:create_index('id', {
                type = 'tree',
                parts = {1, 'number'},
                unique = true})

            box.space['space_varbin']:create_index('varbin', {
                type = 'tree',
                parts = {2, 'varbinary'},
                unique = true})
        """)

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

    @skip_or_run_decimal_test
    def test_custom_packer(self):
        def my_ext_type_encoder(obj):
            if isinstance(obj, decimal.Decimal):
                obj = obj + 1
                return msgpack.ExtType(ext_decimal.EXT_ID, ext_decimal.encode(obj, None))
            raise TypeError("Unknown type: %r" % (obj,))

        def my_packer_factory(_):
            return msgpack.Packer(default=my_ext_type_encoder)

        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        user='test', password='test',
                                        packer_factory=my_packer_factory)

        resp = self.con.eval("return ...", (decimal.Decimal('27756'),))
        self.assertSequenceEqual(resp, [decimal.Decimal('27757')])

    def test_custom_packer_supersedes_encoding(self):
        def my_packer_factory(_):
            return msgpack.Packer(use_bin_type=False)

        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        user='test', password='test',
                                        encoding='utf-8',
                                        packer_factory=my_packer_factory)

        # bytes -> mp_str (string) for encoding=None
        # bytes -> mp_bin (varbinary) for encoding='utf-8'
        resp = self.con.eval("return type(...)", (bytes(bytearray.fromhex('DEADBEAF0103')),))
        self.assertSequenceEqual(resp, ['string'])

    @skip_or_run_decimal_test
    def test_custom_unpacker(self):
        def my_ext_type_decoder(code, data):
            if code == ext_decimal.EXT_ID:
                return ext_decimal.decode(data, None) - 1
            raise NotImplementedError("Unknown msgpack extension type code %d" % (code,))

        def my_unpacker_factory(_):
            if msgpack.version >= (1, 0, 0):
                return msgpack.Unpacker(ext_hook=my_ext_type_decoder, strict_map_key=False)
            return msgpack.Unpacker(ext_hook=my_ext_type_decoder)


        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        user='test', password='test',
                                        unpacker_factory=my_unpacker_factory)

        resp = self.con.eval("return require('decimal').new('27756')")
        self.assertSequenceEqual(resp, [decimal.Decimal('27755')])

    @skip_or_run_varbinary_test
    def test_custom_unpacker_supersedes_encoding(self):
        def my_unpacker_factory(_):
            if msgpack.version >= (0, 5, 2):
                if msgpack.version >= (1, 0, 0):
                    return msgpack.Unpacker(raw=True, strict_map_key=False)

                return msgpack.Unpacker(raw=True)
            return msgpack.Unpacker(encoding=None)

        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        user='test', password='test',
                                        encoding='utf-8',
                                        unpacker_factory=my_unpacker_factory)

        data_id = 1
        data_hex = 'DEADBEAF'
        data = bytes(bytearray.fromhex(data_hex))
        space = 'space_varbin'

        self.con.execute("""
            INSERT INTO "%s" VALUES (%d, x'%s');
        """ % (space, data_id, data_hex))

        resp = self.con.execute("""
            SELECT * FROM "%s" WHERE "varbin" == x'%s';
        """ % (space, data_hex))
        self.assertSequenceEqual(resp, [[data_id, data]])

    def test_custom_unpacker_supersedes_use_list(self):
        def my_unpacker_factory(_):
            if msgpack.version >= (1, 0, 0):
                return msgpack.Unpacker(use_list=False, strict_map_key=False)
            return msgpack.Unpacker(use_list=False)

        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        user='test', password='test',
                                        use_list=True,
                                        unpacker_factory=my_unpacker_factory)

        resp = self.con.eval("return {1, 2, 3}")
        self.assertIsInstance(resp[0], tuple)

    @classmethod
    def tearDown(self):
        if hasattr(self, 'con'):
            self.con.close()

    @classmethod
    def tearDownClass(self):
        self.srv.stop()
        self.srv.clean()
