import sys
import unittest
import uuid
import msgpack
import warnings
import tarantool

from tarantool.msgpack_ext.packer import default as packer_default
from tarantool.msgpack_ext.unpacker import ext_hook as unpacker_ext_hook

from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_UUID_test
from tarantool.error import MsgpackError, MsgpackWarning

class TestSuite_UUID(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' UUID EXT TYPE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()

        self.adm = self.srv.admin
        self.adm(r"""
            _, uuid = pcall(require, 'uuid')

            box.schema.space.create('test')
            box.space['test']:create_index('primary', {
                type = 'tree',
                parts = {1, 'string'},
                unique = true})

            pcall(function()
                box.schema.space.create('test_pk')
                box.space['test_pk']:create_index('primary', {
                    type = 'tree',
                    parts = {1, 'uuid'},
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


    cases = {
        'uuid_1': {
            'python': uuid.UUID('ae28d4f6-076c-49dd-8227-7f9fae9592d0'),
            'msgpack': (b'\xae\x28\xd4\xf6\x07\x6c\x49\xdd\x82\x27\x7f\x9f\xae\x95\x92\xd0'),
            'tarantool': "uuid.fromstr('ae28d4f6-076c-49dd-8227-7f9fae9592d0')",
        },
        'uuid_2': {
            'python': uuid.UUID('b3121301-9300-4038-a652-ead943fb9c39'),
            'msgpack': (b'\xb3\x12\x13\x01\x93\x00\x40\x38\xa6\x52\xea\xd9\x43\xfb\x9c\x39'),
            'tarantool': "uuid.fromstr('b3121301-9300-4038-a652-ead943fb9c39')",
        },
        'uuid_3': {
            'python': uuid.UUID('dfa69f02-92e6-44a5-abb5-84b39292ff93'),
            'msgpack': (b'\xdf\xa6\x9f\x02\x92\xe6\x44\xa5\xab\xb5\x84\xb3\x92\x92\xff\x93'),
            'tarantool': "uuid.fromstr('dfa69f02-92e6-44a5-abb5-84b39292ff93')",
        },
        'uuid_4': {
            'python': uuid.UUID('8b69a1ce-094a-4e21-a5dc-4cdae7cd8960'),
            'msgpack': (b'\x8b\x69\xa1\xce\x09\x4a\x4e\x21\xa5\xdc\x4c\xda\xe7\xcd\x89\x60'),
            'tarantool': "uuid.fromstr('8b69a1ce-094a-4e21-a5dc-4cdae7cd8960')",
        },
        'uuid_5': {
            'python': uuid.UUID('25932334-1d42-4686-9299-ec1a7165227c'),
            'msgpack': (b'\x25\x93\x23\x34\x1d\x42\x46\x86\x92\x99\xec\x1a\x71\x65\x22\x7c'),
            'tarantool': "uuid.fromstr('25932334-1d42-4686-9299-ec1a7165227c')",
        },
    }

    def test_msgpack_decode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.assertEqual(unpacker_ext_hook(2, case['msgpack']),
                                 case['python'])

    @skip_or_run_UUID_test
    def test_tarantool_decode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.adm(f"box.space['test']:replace{{'{name}', {case['tarantool']}}}")

                self.assertSequenceEqual(self.con.select('test', name),
                                         [[name, case['python']]])

    def test_msgpack_encode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.assertEqual(packer_default(case['python']),
                                 msgpack.ExtType(code=2, data=case['msgpack']))

    @skip_or_run_UUID_test
    def test_tarantool_encode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]

                self.con.insert('test', [name, case['python']])

                lua_eval = f"""
                    local tuple = box.space['test']:get('{name}')
                    assert(tuple ~= nil)

                    local id = {case['tarantool']}
                    if tuple[2] == id then
                        return true
                    else
                        return nil, ('%s is not equal to expected %s'):format(
                            tostring(tuple[2]), tostring(id))
                    end
                """

                self.assertSequenceEqual(self.con.eval(lua_eval), [True])


    @skip_or_run_UUID_test
    def test_primary_key(self):
        data = [uuid.UUID('ae28d4f6-076c-49dd-8227-7f9fae9592d0'), 'content']

        self.assertSequenceEqual(self.con.insert('test_pk', data), [data])
        self.assertSequenceEqual(self.con.select('test_pk', data[0]), [data])


    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()
