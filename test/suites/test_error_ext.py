import sys
import unittest
import uuid
import msgpack
import warnings
import tarantool
import pkg_resources

from tarantool.msgpack_ext.packer import default as packer_default
from tarantool.msgpack_ext.unpacker import ext_hook as unpacker_ext_hook

from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_error_ext_type_test

class TestSuite_ErrorExt(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' ERROR EXT TYPE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()

        self.adm = self.srv.admin
        self.adm(r"""
            box.schema.space.create('test')
            box.space['test']:create_index('primary', {
                type = 'tree',
                parts = {1, 'string'},
                unique = true})

            box.schema.user.create('test', {password = 'test', if_not_exists = true})
            box.schema.user.grant('test', 'read,write,execute,create', 'universe')

            box.schema.user.create('no_grants', {if_not_exists = true})
        """)

        self.conn_encoding_utf8 = tarantool.Connection(
            self.srv.host, self.srv.args['primary'],
            user='test', password='test',
            encoding='utf-8')
        self.conn_encoding_none = tarantool.Connection(
            self.srv.host, self.srv.args['primary'],
            user='test', password='test',
            encoding=None)

        if self.adm.tnt_version >= pkg_resources.parse_version('2.10.0'):
            self.conn_encoding_utf8.eval(r"""
                local err = box.error.new(box.error.UNKNOWN)
                rawset(_G, 'simple_error', err)
            """)

            # https://github.com/tarantool/tarantool/blob/125c13c81abb302708771ba04d59382d44a4a512/test/box-tap/extended_error.test.lua
            self.conn_encoding_utf8.eval(r"""
                local user = box.session.user()
                box.schema.func.create('forbidden_function', {body = 'function() end'})
                box.session.su('no_grants')
                _, access_denied_error = pcall(function() box.func.forbidden_function:call() end)
                box.session.su(user)
                rawset(_G, 'access_denied_error', access_denied_error)
            """)

            # https://github.com/tarantool/tarantool/blob/125c13c81abb302708771ba04d59382d44a4a512/test/box-tap/extended_error.test.lua
            self.conn_encoding_utf8.eval(r"""
                local e1 = box.error.new(box.error.UNKNOWN)
                local e2 = box.error.new(box.error.UNKNOWN)
                e2:set_prev(e1)
                rawset(_G, 'chained_error', e2)
            """)

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

        self.adm("box.space['test']:truncate()")


    # msgpack data for different encodings are actually the same,
    # but sometimes python msgpack module use different string
    # types (str8 and str16) for the same strings depending on use_bin_type: 
    #
    # >>> msgpack.Packer(use_bin_type=True).pack('[string "                     local err = box.error.ne..."]')
    # b'\xd9;[string "                     local err = box.error.ne..."]'
    # >>> msgpack.Packer(use_bin_type=False).pack('[string "                     local err = box.error.ne..."]')
    # b'\xda\x00;[string "                     local err = box.error.ne..."]'

    cases = {
        'simple_error_for_encoding_utf8': {
            'conn': 'conn_encoding_utf8',
            'str_type': str,
            'python': tarantool.BoxError(
                type='ClientError',
                file='eval',
                line=1,
                message='Unknown error',
                errno=0,
                errcode=0,
            ),
            'msgpack': (b'\x81\x00\x91\x86\x00\xab\x43\x6c\x69\x65\x6e\x74' +
                        b'\x45\x72\x72\x6f\x72\x01\xa4\x65\x76\x61\x6c\x02' +
                        b'\x01\x03\xad\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x65' +
                        b'\x72\x72\x6f\x72\x04\x00\x05\x00'),
            'tarantool': "simple_error",
        },
        'simple_error_for_encoding_none': {
            'conn': 'conn_encoding_none',
            'str_type': lambda obj: bytes(obj, encoding='utf8'),
            'python': tarantool.BoxError(
                type=b'ClientError',
                file=b'eval',
                line=1,
                message=b'Unknown error',
                errno=0,
                errcode=0,
            ),
            'msgpack': (b'\x81\x00\x91\x86\x00\xab\x43\x6c\x69\x65\x6e\x74' +
                        b'\x45\x72\x72\x6f\x72\x01\xa4\x65\x76\x61\x6c\x02' +
                        b'\x01\x03\xad\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x65' +
                        b'\x72\x72\x6f\x72\x04\x00\x05\x00'),
            'tarantool': "simple_error",
        },
        'error_with_fields_for_encoding_utf8': {
            'conn': 'conn_encoding_utf8',
            'str_type': str,
            'python': tarantool.BoxError(
                type='AccessDeniedError',
                file='/__w/sdk/sdk/tarantool-2.10/tarantool/src/box/func.c',
                line=535,
                message="Execute access to function 'forbidden_function' is denied for user 'no_grants'",
                errno=0,
                errcode=42,
                fields={
                    'object_type': 'function',
                    'object_name': 'forbidden_function',
                    'access_type': 'Execute',
                },
            ),
            'msgpack': (b'\x81\x00\x91\x87\x00\xb1\x41\x63\x63\x65\x73\x73' +
                        b'\x44\x65\x6e\x69\x65\x64\x45\x72\x72\x6f\x72\x01' +
                        b'\xd9\x34\x2f\x5f\x5f\x77\x2f\x73\x64\x6b\x2f\x73' +
                        b'\x64\x6b\x2f\x74\x61\x72\x61\x6e\x74\x6f\x6f\x6c' +
                        b'\x2d\x32\x2e\x31\x30\x2f\x74\x61\x72\x61\x6e\x74' +
                        b'\x6f\x6f\x6c\x2f\x73\x72\x63\x2f\x62\x6f\x78\x2f' +
                        b'\x66\x75\x6e\x63\x2e\x63\x02\xcd\x02\x17\x03\xd9' +
                        b'\x4e\x45\x78\x65\x63\x75\x74\x65\x20\x61\x63\x63' +
                        b'\x65\x73\x73\x20\x74\x6f\x20\x66\x75\x6e\x63\x74' +
                        b'\x69\x6f\x6e\x20\x27\x66\x6f\x72\x62\x69\x64\x64' +
                        b'\x65\x6e\x5f\x66\x75\x6e\x63\x74\x69\x6f\x6e\x27' +
                        b'\x20\x69\x73\x20\x64\x65\x6e\x69\x65\x64\x20\x66' +
                        b'\x6f\x72\x20\x75\x73\x65\x72\x20\x27\x6e\x6f\x5f' +
                        b'\x67\x72\x61\x6e\x74\x73\x27\x04\x00\x05\x2a\x06' +
                        b'\x83\xab\x6f\x62\x6a\x65\x63\x74\x5f\x74\x79\x70' +
                        b'\x65\xa8\x66\x75\x6e\x63\x74\x69\x6f\x6e\xab\x6f' +
                        b'\x62\x6a\x65\x63\x74\x5f\x6e\x61\x6d\x65\xb2\x66' +
                        b'\x6f\x72\x62\x69\x64\x64\x65\x6e\x5f\x66\x75\x6e' +
                        b'\x63\x74\x69\x6f\x6e\xab\x61\x63\x63\x65\x73\x73' +
                        b'\x5f\x74\x79\x70\x65\xa7\x45\x78\x65\x63\x75\x74' +
                        b'\x65'),
            'tarantool': "access_denied_error",
            'ignore_file_info': True,
        },
        'error_with_fields_for_encoding_none': {
            'conn': 'conn_encoding_none',
            'str_type': lambda obj: bytes(obj, encoding='utf8'),
            'python': tarantool.BoxError(
                type=b'AccessDeniedError',
                file=b'/__w/sdk/sdk/tarantool-2.10/tarantool/src/box/func.c',
                line=535,
                message=b"Execute access to function 'forbidden_function' is denied for user 'no_grants'",
                errno=0,
                errcode=42,
                fields={
                    b'object_type': b'function',
                    b'object_name': b'forbidden_function',
                    b'access_type': b'Execute',
                },
            ),
            'msgpack': (b'\x81\x00\x91\x87\x00\xb1\x41\x63\x63\x65\x73\x73' +
                        b'\x44\x65\x6e\x69\x65\x64\x45\x72\x72\x6f\x72\x01' +
                        b'\xda\x00\x34\x2f\x5f\x5f\x77\x2f\x73\x64\x6b\x2f' +
                        b'\x73\x64\x6b\x2f\x74\x61\x72\x61\x6e\x74\x6f\x6f' +
                        b'\x6c\x2d\x32\x2e\x31\x30\x2f\x74\x61\x72\x61\x6e' +
                        b'\x74\x6f\x6f\x6c\x2f\x73\x72\x63\x2f\x62\x6f\x78' +
                        b'\x2f\x66\x75\x6e\x63\x2e\x63\x02\xcd\x02\x17\x03' +
                        b'\xda\x00\x4e\x45\x78\x65\x63\x75\x74\x65\x20\x61' +
                        b'\x63\x63\x65\x73\x73\x20\x74\x6f\x20\x66\x75\x6e' +
                        b'\x63\x74\x69\x6f\x6e\x20\x27\x66\x6f\x72\x62\x69' +
                        b'\x64\x64\x65\x6e\x5f\x66\x75\x6e\x63\x74\x69\x6f' +
                        b'\x6e\x27\x20\x69\x73\x20\x64\x65\x6e\x69\x65\x64' +
                        b'\x20\x66\x6f\x72\x20\x75\x73\x65\x72\x20\x27\x6e' +
                        b'\x6f\x5f\x67\x72\x61\x6e\x74\x73\x27\x04\x00\x05' +
                        b'\x2a\x06\x83\xab\x6f\x62\x6a\x65\x63\x74\x5f\x74' +
                        b'\x79\x70\x65\xa8\x66\x75\x6e\x63\x74\x69\x6f\x6e' +
                        b'\xab\x6f\x62\x6a\x65\x63\x74\x5f\x6e\x61\x6d\x65' +
                        b'\xb2\x66\x6f\x72\x62\x69\x64\x64\x65\x6e\x5f\x66' +
                        b'\x75\x6e\x63\x74\x69\x6f\x6e\xab\x61\x63\x63\x65' +
                        b'\x73\x73\x5f\x74\x79\x70\x65\xa7\x45\x78\x65\x63' +
                        b'\x75\x74\x65'),
            'tarantool': "access_denied_error",
            'ignore_file_info': True,
        },
        'error_chain_for_encoding_utf8': {
            'conn': 'conn_encoding_utf8',
            'str_type': str,
            'python': tarantool.BoxError(
                type='ClientError',
                file='eval',
                line=3,
                message='Unknown error',
                errno=0,
                errcode=0,
                prev=tarantool.BoxError(
                    type='ClientError',
                    file='eval',
                    line=2,
                    message='Unknown error',
                    errno=0,
                    errcode=0,
                ),
            ),
            'msgpack': (b'\x81\x00\x92\x86\x00\xab\x43\x6c\x69\x65\x6e\x74' +
                        b'\x45\x72\x72\x6f\x72\x01\xa4\x65\x76\x61\x6c\x02' +
                        b'\x03\x03\xad\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x65' +
                        b'\x72\x72\x6f\x72\x04\x00\x05\x00\x86\x00\xab\x43' +
                        b'\x6c\x69\x65\x6e\x74\x45\x72\x72\x6f\x72\x01\xa4' +
                        b'\x65\x76\x61\x6c\x02\x02\x03\xad\x55\x6e\x6b\x6e' +
                        b'\x6f\x77\x6e\x20\x65\x72\x72\x6f\x72\x04\x00\x05\x00'),
            'tarantool': "chained_error",
            'ignore_file_info': False,
        },
        'error_chain_for_encoding_none': {
            'conn': 'conn_encoding_none',
            'str_type': lambda obj: bytes(obj, encoding='utf8'),
            'python': tarantool.BoxError(
                type=b'ClientError',
                file=b'eval',
                line=3,
                message=b'Unknown error',
                errno=0,
                errcode=0,
                prev=tarantool.BoxError(
                    type=b'ClientError',
                    file=b'eval',
                    line=2,
                    message=b'Unknown error',
                    errno=0,
                    errcode=0,
                ),
            ),
            'msgpack': (b'\x81\x00\x92\x86\x00\xab\x43\x6c\x69\x65\x6e\x74' +
                        b'\x45\x72\x72\x6f\x72\x01\xa4\x65\x76\x61\x6c\x02' +
                        b'\x03\x03\xad\x55\x6e\x6b\x6e\x6f\x77\x6e\x20\x65' +
                        b'\x72\x72\x6f\x72\x04\x00\x05\x00\x86\x00\xab\x43' +
                        b'\x6c\x69\x65\x6e\x74\x45\x72\x72\x6f\x72\x01\xa4' +
                        b'\x65\x76\x61\x6c\x02\x02\x03\xad\x55\x6e\x6b\x6e' +
                        b'\x6f\x77\x6e\x20\x65\x72\x72\x6f\x72\x04\x00\x05\x00'),
            'tarantool': "chained_error",
            'ignore_file_info': False,
        }
    }


    def test_msgpack_decode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]
                conn = getattr(self, case['conn'])

                self.assertEqual(
                    unpacker_ext_hook(
                        3,
                        case['msgpack'],
                        conn._unpacker_factory(),
                    ),
                    case['python'])

    @skip_or_run_error_ext_type_test
    def test_tarantool_decode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]
                conn = getattr(self, case['conn'])

                self.adm(f"""
                    local err = rawget(_G, '{case['tarantool']}')
                    box.space['test']:replace{{'{name}', err, 'payload'}}
                """)

                res = conn.select('test', case['str_type'](name))
                self.assertEqual(len(res), 1)

                # Tarantool error file and line could differ even between
                # different patches.
                #
                # Also, in Tarantool errors are not comparable at all.
                #
                # tarantool> msgpack.decode(error_str) == msgpack.decode(error_str)
                # ---
                # - false
                # ...

                self.assertEqual(res[0][0], case['str_type'](name))
                self.assertEqual(res[0][2], case['str_type']('payload'))

                err = res[0][1]
                self.assertTrue(
                    isinstance(err, tarantool.BoxError),
                    f'{err} is expected to be a BoxError object')

                expected_err = case['python']
                while err is not None:
                    self.assertEqual(err.type, expected_err.type)
                    self.assertEqual(err.message, expected_err.message)
                    self.assertEqual(err.errno, expected_err.errno)
                    self.assertEqual(err.errcode, expected_err.errcode)
                    self.assertEqual(err.fields, expected_err.fields)

                    err = err.prev
                    expected_err = expected_err.prev

                self.assertEqual(err, expected_err)


    def test_msgpack_encode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]
                conn = getattr(self, case['conn'])

                self.assertEqual(packer_default(case['python'], conn._packer_factory()),
                                 msgpack.ExtType(code=3, data=case['msgpack']))

    @skip_or_run_error_ext_type_test
    def test_tarantool_encode(self):
        for name in self.cases.keys():
            with self.subTest(msg=name):
                case = self.cases[name]
                conn = getattr(self, case['conn'])

                conn.insert(
                    'test',
                    [case['str_type'](name), case['python'], case['str_type']('payload')])

                lua_eval = f"""
                    local err = rawget(_G, '{case['tarantool']}')

                    local tuple = box.space['test']:get('{name}')
                    assert(tuple ~= nil)

                    local tuple_err = tuple[2]

                    local fields = {{'type', 'message', 'errno', 'errcode', 'fields'}}

                    local json = require('json')

                    local function compare_errors(err1, err2)
                        if (err1 == nil) and (err2 ~= nil) then
                            return nil, ('Test error stack is empty, but expected error ' ..
                                         'has previous %s (%s) error'):format(
                                         err2.type, err2.message)
                        end

                        if (err1 ~= nil) and (err2 == nil) then
                            return nil, ('Expected error stack is empty, but test error ' ..
                                         'has previous %s (%s) error'):format(
                                         err1.type, err1.message)
                        end

                        for _, field in ipairs(fields) do
                            if json.encode(err1[field]) ~= json.encode(err2[field]) then
                                return nil, ('%s %s is not equal to expected %s'):format(
                                    field,
                                    json.encode(err1[field]),
                                    json.encode(err2[field]))
                            end
                        end

                        if (err1.prev ~= nil) or (err2.prev ~= nil) then
                            return compare_errors(err1.prev, err2.prev)
                        end

                        return true
                    end

                    return compare_errors(tuple_err, err)
                """

                self.assertSequenceEqual(conn.eval(lua_eval), [True])


    @classmethod
    def tearDownClass(self):
        self.conn_encoding_utf8.close()
        self.conn_encoding_none.close()
        self.srv.stop()
        self.srv.clean()
