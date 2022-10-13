import os
import sys
import unittest
import warnings
from tarantool.error import (
    NetworkWarning
)
from tarantool.const import (
    DEFAULT_TRANSPORT,
    SSL_TRANSPORT
)
import tarantool
from .lib.tarantool_server import TarantoolServer


def is_test_ssl():
    env = os.getenv("TEST_TNT_SSL")
    if env:
        env = env.upper()
        return env == "1" or env == "TRUE"
    return False


@unittest.skipIf(not is_test_ssl(), "TEST_TNT_SSL is not set.")
class TestSuite_Ssl(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' SSL '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

        test_suites_dir = os.path.dirname(__file__)
        test_data_dir = os.path.join(test_suites_dir, "..", "data")
        self.cert_file = os.path.join(test_data_dir, "localhost.crt")
        self.invalidhost_cert_file = os.path.join(test_data_dir,
                                                  "invalidhost.crt")
        self.key_file = os.path.join(test_data_dir, "localhost.key")
        self.ca_file = os.path.join(test_data_dir, "ca.crt")
        self.empty_file = os.path.join(test_data_dir, "empty")
        self.invalid_file = "any_invalid_path"

    def stop_srv(self, srv):
        if srv:
            srv.stop()
            srv.clean()

    def stop_con(self, con):
        if con:
            con.close()

    def stop_mesh(self, mesh):
        if mesh:
            mesh.stop()

    def stop_pool(self, pool):
        if pool:
            pool.close()

    def test_single(self):
        class SslTestCase:
            def __init__(self,
                         name="",
                         ok=False,
                         server_transport=SSL_TRANSPORT,
                         server_key_file=None,
                         server_cert_file=None,
                         server_ca_file=None,
                         server_ciphers=None,
                         client_cert_file=None,
                         client_key_file=None,
                         client_ca_file=None,
                         client_ciphers=None):
                self.name = name
                self.ok = ok
                self.server_transport = server_transport
                self.server_key_file = server_key_file
                self.server_cert_file = server_cert_file
                self.server_ca_file = server_ca_file
                self.server_ciphers = server_ciphers
                self.client_cert_file = client_cert_file
                self.client_key_file = client_key_file
                self.client_ca_file = client_ca_file
                self.client_ciphers = client_ciphers

        # Requirements from Tarantool Enterprise Edition manual:
        # https://www.tarantool.io/en/enterprise_doc/security/#configuration
        #
        # For a server:
        # ssl_key_file - mandatory
        # ssl_cert_file - mandatory
        # ssl_ca_file - optional
        # ssl_ciphers - optional
        #
        # For a client:
        # ssl_key_file - optional, mandatory if server.CaFile set
        # ssl_cert_file - optional, mandatory if server.CaFile set
        # ssl_ca_file - optional
        # ssl_ciphers - optional
        testcases = [
            SslTestCase(
                name="no_ssl_server",
                ok=False,
                server_transport=DEFAULT_TRANSPORT),
            SslTestCase(
                name="key_crt_server",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file),
            SslTestCase(
                name="key_crt_server_and_client",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file),
            SslTestCase(
                name="key_crt_ca_server",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file),
            SslTestCase(
                name="key_crt_ca_server_and_crt_client",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_cert_file=self.cert_file),
            SslTestCase(
                name="key_crt_ca_server_and_key_crt_client",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file),
            SslTestCase(
                name="key_crt_ca_server_and_client",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestCase(
                name="key_invalidhost_crt_ca_server_and_key_crt_ca_client",
                # A Tarantool implementation does not check hostname. It's
                # the expected behavior. We don't do that too.
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.invalidhost_cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestCase(
                name="key_crt_ca_server_and_client_invalid_crt",
                ok=False,
                client_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_key_file=self.key_file,
                client_cert_file=self.invalid_file,
                client_ca_file=self.ca_file),
            SslTestCase(
                name="key_crt_ca_server_and_client_invalid_key",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.invalid_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestCase(
                name="key_crt_ca_server_and_client_invalid_ca",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.invalid_file),
            SslTestCase(
                name="key_crt_ca_server_and_client_empty_crt",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.empty_file,
                client_ca_file=self.ca_file),
            SslTestCase(
                name="key_crt_ca_server_and_client_empty_key",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.empty_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestCase(
                name="key_crt_ca_server_and_client_empty_ca",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.empty_file),
            SslTestCase(
                name="key_crt_ca_ciphers_server_and_key_crt_ca_client",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
            SslTestCase(
                name="key_crt_ca_ciphers_server_and_client",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_ciphers="ECDHE-RSA-AES256-GCM-SHA384"),
            SslTestCase(
                name="non_equal_ciphers",
                ok=False,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_ciphers="TLS_AES_128_GCM_SHA256"),
        ]
        for t in testcases:
            with self.subTest(msg=t.name):
                srv = None
                con = None
                try:
                    srv = TarantoolServer(
                        transport=t.server_transport,
                        ssl_key_file=t.server_key_file,
                        ssl_cert_file=t.server_cert_file,
                        ssl_ca_file=t.server_ca_file,
                        ssl_ciphers=t.server_ciphers)
                    srv.script = 'test/suites/box.lua'
                    srv.start()
                    srv.admin("box.schema.create_space('space_1')")
                    srv.admin("""
                    box.space['space_1']:create_index('primary', {
                        type = 'tree',
                        parts = {1, 'num'},
                        unique = true})
                    """.replace('\n', ' '))
                    srv.admin("""
                    box.schema.user.create('test', { password = 'test' })
                    """.replace('\n', ' '))
                    srv.admin("""
                    box.schema.user.grant('test', 'execute,read,write',
                                          'universe')
                    """.replace('\n', ' '))
                    con = tarantool.Connection(
                        srv.host, srv.args['primary'],
                        user="test",
                        password="test",
                        transport="ssl",
                        ssl_key_file=t.client_key_file,
                        ssl_cert_file=t.client_cert_file,
                        ssl_ca_file=t.client_ca_file,
                        ssl_ciphers=t.client_ciphers,
                        connection_timeout=0.5,
                        socket_timeout=0.5)

                    self.assertEqual(con.insert('space_1', [1])[0], [1])
                    self.assertEqual(len(con.select('space_1')), 1)
                    self.assertTrue(t.ok)
                except tarantool.error.SslError:
                    self.assertFalse(t.ok)
                finally:
                    self.stop_srv(srv)
                    self.stop_con(con)

    @unittest.skipIf(sys.platform.startswith("win"),
                     'Pool tests on windows platform are not supported')
    def test_pool(self):
        servers = []
        cnt = 5
        pool = None
        try:
            addrs = []
            for i in range(cnt):
                srv = TarantoolServer(
                    transport='ssl',
                    ssl_key_file=self.key_file,
                    ssl_cert_file=self.cert_file,
                    ssl_ca_file=self.ca_file)
                srv.script = 'test/suites/box.lua'
                srv.start()
                srv.admin("""
                box.schema.user.create('test', { password = 'test' })
                """.replace('\n', ' '))
                srv.admin("""
                box.schema.user.grant('test', 'execute,read,write', 'universe')
                """.replace('\n', ' '))
                servers.append(srv)
                addrs.append({
                    'host': srv.host,
                    'port': srv.args['primary'],
                    'transport': 'ssl',
                    'ssl_key_file': self.key_file,
                    'ssl_cert_file': self.cert_file,
                    'ssl_ca_file': self.ca_file})

            pool = tarantool.ConnectionPool(
                addrs=addrs,
                user='test',
                password='test',
                connection_timeout=1,
                socket_timeout=1)
            self.assertSequenceEqual(
                pool.eval('return box.info().ro', mode=tarantool.Mode.RW),
                [False])
        finally:
            self.stop_pool(pool)

    def test_mesh(self):
        servers = []
        cnt = 5
        con = None
        try:
            addrs = []
            for i in range(cnt):
                srv = TarantoolServer(
                    transport='ssl',
                    ssl_key_file=self.key_file,
                    ssl_cert_file=self.cert_file,
                    ssl_ca_file=self.ca_file)
                srv.script = 'test/suites/box.lua'
                srv.start()
                srv.admin("""
                box.schema.user.create('test', { password = 'test' })
                """.replace('\n', ' '))
                srv.admin("""
                box.schema.user.grant('test', 'execute,read,write', 'universe')
                """.replace('\n', ' '))
                srv.admin("function srv_id() return %s end" % i)
                servers.append(srv)
                addrs.append({
                    'host': srv.host,
                    'port': srv.args['primary'],
                    'transport': 'ssl',
                    'ssl_key_file': self.key_file,
                    'ssl_cert_file': self.cert_file,
                    'ssl_ca_file': self.ca_file})

            mesh = tarantool.MeshConnection(
                addrs=addrs,
                user='test',
                password='test',
                connection_timeout=0.5,
                socket_timeout=0.5)
            for i in range(cnt):
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', NetworkWarning)
                    resp = mesh.call('srv_id')
                    self.assertEqual(resp.data and resp.data[0], i)
                    servers[i].stop()
        finally:
            self.stop_mesh(con)
