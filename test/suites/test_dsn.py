"""
This module tests DSN parsing and connecting with a DSN.
"""
# pylint: disable=missing-class-docstring,missing-function-docstring,duplicate-code
# pylint: disable=protected-access,too-many-public-methods

import sys
import unittest

from tarantool import dbapi
from tarantool.error import ConfigurationError, InterfaceError
from tarantool.utils import parse_dsn

from .lib.tarantool_server import TarantoolServer
from .utils import assert_admin_success


class TestSuiteDsnParse(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print(' DSN PARSE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

    def _assert_parsed(self, cases):
        for dsn, expected in cases.items():
            with self.subTest(dsn=dsn):
                self.assertEqual(parse_dsn(dsn), expected)

    def _assert_rejected(self, cases):
        for dsn in cases:
            with self.subTest(dsn=dsn):
                with self.assertRaises(ConfigurationError):
                    parse_dsn(dsn)

    def test_address(self):
        self._assert_parsed({
            '3301':
                {'host': '127.0.0.1', 'port': 3301},
            'localhost:3301':
                {'host': 'localhost', 'port': 3301},
            '192.168.10.10:3301':
                {'host': '192.168.10.10', 'port': 3301},
            'server001.example.com:3301':
                {'host': 'server001.example.com', 'port': 3301},
        })

    def test_ipv6_address(self):
        self._assert_parsed({
            '[::1]:3301':
                {'host': '::1', 'port': 3301},
            '[2a00:1148:b0ba:2016::10]:3301':
                {'host': '2a00:1148:b0ba:2016::10', 'port': 3301},
            '[::ffff:127.0.0.1]:3301':
                {'host': '::ffff:127.0.0.1', 'port': 3301},
        })

    def test_ipv6_address_requires_brackets(self):
        self._assert_rejected(['::1:3301', '2a00:1148:b0ba:2016::10:3301'])

    def test_invalid_ipv6_address(self):
        # Tarantool allows an IPv6 address only inside the brackets:
        # neither a host name nor a zone index is accepted there.
        self._assert_rejected([
            '[localhost]:3301',
            '[fe80::1%eth0]:3301',
            '[]:3301',
            '[::1:3301',
            # inet_pton() raises a ValueError, not an OSError, on it.
            '[\x00::1]:3301',
        ])

    def test_unix_socket(self):
        self._assert_parsed({
            'unix/:/tmp/tt.sock':
                {'host': None, 'port': '/tmp/tt.sock'},
            'unix/:./var/run/tt.iproto':
                {'host': None, 'port': './var/run/tt.iproto'},
            '/tmp/unix_domain_socket.sock':
                {'host': None, 'port': '/tmp/unix_domain_socket.sock'},
            'user:pass@unix/:/tmp/tt.sock':
                {'user': 'user', 'password': 'pass',
                 'host': None, 'port': '/tmp/tt.sock'},
            'unix/:/tmp/tt.sock?transport=ssl':
                {'host': None, 'port': '/tmp/tt.sock', 'transport': 'ssl'},
        })

    def test_unix_socket_path_with_at_sign(self):
        # Tarantool reads a "@" as a part of a socket path, since a
        # user name may contain no "/".
        self._assert_parsed({
            '/var/run/tt@1.sock':
                {'host': None, 'port': '/var/run/tt@1.sock'},
            'unix/:/tmp/a@b.sock':
                {'host': None, 'port': '/tmp/a@b.sock'},
        })

    def test_empty_unix_socket_path(self):
        self._assert_rejected(['unix/:'])

    def test_relative_unix_socket_path(self):
        # Tarantool takes a path as a socket address only if it is
        # absolute or started with "./".
        self._assert_rejected(['unix/:x.sock', 'unix/:relative/x.sock'])

    def test_user_password(self):
        self._assert_parsed({
            'user@localhost:3301':
                {'user': 'user', 'host': 'localhost', 'port': 3301},
            'user:pass@localhost:3301':
                {'user': 'user', 'password': 'pass',
                 'host': 'localhost', 'port': 3301},
            'user:@localhost:3301':
                {'user': 'user', 'password': '',
                 'host': 'localhost', 'port': 3301},
        })

    def test_values_are_not_percent_decoded(self):
        # Tarantool uri.parse() does not decode percent-encoded values.
        self._assert_parsed({
            'user:p%40ss@localhost:3301':
                {'user': 'user', 'password': 'p%40ss',
                 'host': 'localhost', 'port': 3301},
        })

    def test_at_sign_is_not_allowed_in_userinfo(self):
        self._assert_rejected(['user:p@ss@localhost:3301'])

    def test_empty_user(self):
        self._assert_rejected(['@localhost:3301', ':pass@localhost:3301'])

    def test_colon_is_not_allowed_in_userinfo(self):
        self._assert_rejected(['user:pa:ss@localhost:3301'])

    def test_slash_is_not_allowed_in_userinfo(self):
        # Tarantool reinterprets such a DSN as a host with a service,
        # silently connecting to a wrong address.
        self._assert_rejected(['user:pa/ss@localhost:3301'])

    def test_scheme_is_ignored(self):
        self._assert_parsed({
            'tcp://localhost:3301':
                {'host': 'localhost', 'port': 3301},
            'TCP://localhost:3301':
                {'host': 'localhost', 'port': 3301},
            'tcp+ssl://localhost:3301':
                {'host': 'localhost', 'port': 3301},
            'tarantool://user:pass@localhost:3301':
                {'user': 'user', 'password': 'pass',
                 'host': 'localhost', 'port': 3301},
        })

    def test_scheme_is_recognized_only_at_the_beginning(self):
        # A "://" in the middle of a DSN is not a scheme delimiter,
        # so a query value may contain an URL of its own.
        self._assert_parsed({
            'localhost:3301?ssl_ca_file=https://ca.example.com/ca.crt':
                {'host': 'localhost', 'port': 3301,
                 'ssl_ca_file': 'https://ca.example.com/ca.crt'},
        })
        self._assert_rejected([
            '://localhost:3301',
            '1tcp://localhost:3301',
            'user:pass://localhost:3301',
        ])

    def test_whitespace_is_not_allowed(self):
        self._assert_rejected([
            ' localhost:3301',
            'localhost:3301 ',
            'localhost: 3301',
            'ho st:3301',
            'localhost:33\n01',
            'localhost:3301?transport=s sl',
        ])

    def test_options(self):
        dsn = ('localhost:3301?transport=ssl'
               '&ssl_key_file=k.key'
               '&ssl_cert_file=c.crt'
               '&ssl_ca_file=ca.crt'
               '&ssl_ciphers=ECDHE-RSA-AES256-GCM-SHA384'
               '&ssl_password=secret'
               '&ssl_password_file=/etc/pw.txt'
               '&auth_type=pap-sha256')
        self._assert_parsed({
            dsn: {
                'host': 'localhost',
                'port': 3301,
                'transport': 'ssl',
                'ssl_key_file': 'k.key',
                'ssl_cert_file': 'c.crt',
                'ssl_ca_file': 'ca.crt',
                'ssl_ciphers': 'ECDHE-RSA-AES256-GCM-SHA384',
                'ssl_password': 'secret',
                'ssl_password_file': '/etc/pw.txt',
                'auth_type': 'pap-sha256',
            },
        })

    def test_last_option_value_wins(self):
        self._assert_parsed({
            'localhost:3301?transport=ssl&transport=plain':
                {'host': 'localhost', 'port': 3301, 'transport': 'plain'},
        })

    def test_invalid_options(self):
        self._assert_rejected([
            'localhost:3301?fetch_schema=false',
            'localhost:3301?unknown=1',
            'localhost:3301?transport',
        ])

    def test_no_port(self):
        self._assert_rejected(['localhost', 'localhost:', '[::1]'])

    def test_invalid_port(self):
        self._assert_rejected([
            'localhost:notaport',
            'localhost:0',
            'localhost:99999',
        ])

    def test_port_is_ascii_digits_only(self):
        # int() is more permissive than Tarantool: it also takes a
        # sign, underscore separators and non-ASCII digits.
        self._assert_rejected([
            'localhost:+3301',
            'localhost:-3301',
            'localhost:3_301',
            'localhost:0x10',
            'localhost:٠٣٣٠١',
            '٠٣٣٠١',
        ])
        self._assert_parsed({
            'localhost:03301': {'host': 'localhost', 'port': 3301},
        })

    def test_empty_host(self):
        self._assert_rejected([':3301'])

    def test_empty_dsn(self):
        self._assert_rejected([''])

    def test_dsn_is_not_a_string(self):
        for dsn in (3301, None, ['localhost:3301']):
            with self.subTest(dsn=dsn):
                with self.assertRaises(ConfigurationError):
                    parse_dsn(dsn)


class TestSuiteDsnConnect(unittest.TestCase):
    EVAL_USER = "return box.session.user()"

    @classmethod
    def setUpClass(cls):
        print(' DSN CONNECT '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

        cls.srv = TarantoolServer()
        cls.srv.script = 'test/suites/box.lua'
        cls.srv.start()

        resp = cls.srv.admin("""
            box.schema.user.create('test', {password = 'test', if_not_exists = true})
            box.schema.user.grant('test', 'read,write,execute', 'universe',
                                  nil, {if_not_exists = true})

            return true
        """)
        assert_admin_success(resp)

        if sys.platform.startswith("win"):
            cls.sock_srv = None
        else:
            cls.sock_srv = TarantoolServer(create_unix_socket=True)
            cls.sock_srv.script = 'test/suites/box.lua'
            cls.sock_srv.start()

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

    @property
    def address(self):
        return f'{self.srv.host}:{self.srv.args["primary"]}'

    def test_connect_host_port(self):
        conn = dbapi.connect(dsn=self.address)
        try:
            self.assertEqual(conn.ping(notime=True), "Success")
        finally:
            conn.close()

    def test_connect_user_password(self):
        conn = dbapi.connect(dsn=f'test:test@{self.address}')
        try:
            self.assertSequenceEqual(conn.eval(self.EVAL_USER), ["test"])
        finally:
            conn.close()

    def test_connect_scheme_is_ignored(self):
        conn = dbapi.connect(dsn=f'tcp://test:test@{self.address}')
        try:
            self.assertSequenceEqual(conn.eval(self.EVAL_USER), ["test"])
        finally:
            conn.close()

    @unittest.skipIf(sys.platform.startswith("win"),
                     'Unix sockets are not supported on Windows')
    def test_connect_unix_socket(self):
        conn = dbapi.connect(dsn=f'unix/:{self.sock_srv.args["primary"]}')
        try:
            self.assertEqual(conn.ping(notime=True), "Success")
        finally:
            conn.close()

    @unittest.skipIf(sys.platform.startswith("win"),
                     'Unix sockets are not supported on Windows')
    def test_connect_unix_socket_bare_path(self):
        conn = dbapi.connect(dsn=str(self.sock_srv.args["primary"]))
        try:
            self.assertEqual(conn.ping(notime=True), "Success")
        finally:
            conn.close()

    def test_explicit_args_take_precedence(self):
        dsn = f'wronguser:wrongpass@{self.address}'
        conn = dbapi.connect(dsn=dsn, user='test', password='test')
        try:
            self.assertSequenceEqual(conn.eval(self.EVAL_USER), ["test"])
        finally:
            conn.close()

    def test_explicit_kwargs_take_precedence(self):
        conn = dbapi.connect(dsn=f'{self.address}?transport=ssl',
                             transport='', connect_now=False)
        self.assertEqual(conn.transport, '')

    def test_dsn_options_are_applied(self):
        dsn = (f'{self.address}?transport=ssl'
               '&ssl_key_file=k.key'
               '&ssl_cert_file=c.crt'
               '&ssl_ca_file=ca.crt'
               '&ssl_ciphers=ECDHE-RSA-AES256-GCM-SHA384'
               '&ssl_password=secret'
               '&ssl_password_file=/etc/pw.txt'
               '&auth_type=chap-sha1')
        conn = dbapi.connect(dsn=dsn, connect_now=False)
        self.assertEqual(conn.transport, 'ssl')
        self.assertEqual(conn.ssl_key_file, 'k.key')
        self.assertEqual(conn.ssl_cert_file, 'c.crt')
        self.assertEqual(conn.ssl_ca_file, 'ca.crt')
        self.assertEqual(conn.ssl_ciphers, 'ECDHE-RSA-AES256-GCM-SHA384')
        self.assertEqual(conn.ssl_password, 'secret')
        self.assertEqual(conn.ssl_password_file, '/etc/pw.txt')
        self.assertEqual(conn._client_auth_type, 'chap-sha1')

    def test_dsn_unix_socket_params(self):
        conn = dbapi.connect(dsn='unix/:/tmp/tt.sock', connect_now=False)
        self.assertIsNone(conn.host)
        self.assertEqual(conn.port, '/tmp/tt.sock')

    def test_invalid_dsn(self):
        # PEP-249 requires the connect() errors to be a part of the
        # DB-API exception hierarchy.
        for dsn in ('localhost', 'localhost:notaport', 'localhost:3301?unknown=1'):
            with self.subTest(dsn=dsn):
                with self.assertRaises(InterfaceError):
                    dbapi.connect(dsn=dsn)

    @classmethod
    def tearDownClass(cls):
        cls.srv.stop()
        cls.srv.clean()

        if cls.sock_srv is not None:
            cls.sock_srv.stop()
            cls.sock_srv.clean()
