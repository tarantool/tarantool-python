import os
import sys
import unittest
import warnings
from tarantool.error import (
    NetworkWarning
)
from tarantool.const import (
    DEFAULT_TRANSPORT,
    SSL_TRANSPORT,
    AUTH_TYPE_CHAP_SHA1,
    AUTH_TYPE_PAP_SHA256,
)
import tarantool
from .lib.tarantool_server import TarantoolServer
from .lib.skip import (
    fetch_tarantool_version,
    skip_or_run_ssl_password_test_call,
    skip_or_run_auth_type_test_call,
)


def is_test_ssl():
    env = os.getenv("TEST_TNT_SSL")
    if env:
        env = env.upper()
        return env == "1" or env == "TRUE"
    return False


class SslTestCase:
    def __init__(self,
                 name="",
                 ok=False,
                 expected_error=tarantool.error.SslError,
                 server_transport=SSL_TRANSPORT,
                 server_key_file=None,
                 server_cert_file=None,
                 server_ca_file=None,
                 server_ciphers=None,
                 server_password=None,
                 server_password_file=None,
                 server_auth_type=None,
                 client_transport=SSL_TRANSPORT,
                 client_cert_file=None,
                 client_key_file=None,
                 client_ca_file=None,
                 client_ciphers=None,
                 client_password=None,
                 client_password_file=None,
                 client_auth_type=None):
        self.name = name
        self.ok = ok
        self.expected_error = expected_error
        self.server_transport = server_transport
        self.server_key_file = server_key_file
        self.server_cert_file = server_cert_file
        self.server_ca_file = server_ca_file
        self.server_ciphers = server_ciphers
        self.server_password = server_password
        self.server_password_file = server_password_file
        self.server_auth_type = server_auth_type
        self.client_transport = client_transport
        self.client_cert_file = client_cert_file
        self.client_key_file = client_key_file
        self.client_ca_file = client_ca_file
        self.client_ciphers = client_ciphers
        self.client_password = client_password
        self.client_password_file = client_password_file
        self.client_auth_type = client_auth_type

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
        self.key_enc_file = os.path.join(test_data_dir, "localhost.enc.key")
        self.ca_file = os.path.join(test_data_dir, "ca.crt")
        self.empty_file = os.path.join(test_data_dir, "empty")
        self.password = "mysslpassword"
        self.invalid_password = "notmysslpassword"
        self.password_file = os.path.join(test_data_dir, "passwords")
        self.invalid_password_file = os.path.join(test_data_dir, "invalidpasswords")
        self.invalid_file = "any_invalid_path"

        # Extract the version for skips.
        self.tnt_version = None
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()
        fetch_tarantool_version(self)
        self.srv.stop()
        self.srv.clean()
        self.srv = None

    def stop_srv(self, srv):
        if srv:
            srv.stop()
            srv.clean()

    def stop_servers(self, servers):
        for srv in servers:
            srv.stop()
            srv.clean()

    def stop_con(self, con):
        if con:
            con.close()

    def stop_mesh(self, mesh):
        if mesh:
            mesh.close()

    def stop_pool(self, pool):
        if pool:
            pool.close()

    def test_single(self):
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
            SslTestCase(
                name="pass_no_key_encrypt",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.password),
            SslTestCase(
                name="pass_file_no_key_encrypt",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password_file=self.password_file),
            SslTestCase(
                name="pass_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.password),
            SslTestCase(
                name="pass_file_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password_file=self.password_file),
            SslTestCase(
                name="pass_and_pass_file_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.password,
                client_password_file=self.password_file),
            SslTestCase(
                name="inv_pass_and_pass_file_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.invalid_password,
                client_password_file=self.password_file),
            SslTestCase(
                name="pass_and_inv_pass_file_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.password,
                client_password_file=self.invalid_password_file),
            SslTestCase(
                name="inv_pass_and_inv_pass_file_key_encrypt",
                ok=False,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.invalid_password,
                client_password_file=self.invalid_password_file),
            SslTestCase(
                name="no_pass_and_inv_pass_file_key_encrypt",
                ok=False,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file),
            SslTestCase(
                name="pass_key_invalidhost_crt_ca_server_and_key_crt_ca_client",
                # A Tarantool implementation does not check hostname. It's
                # the expected behavior. We don't do that too.
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.invalidhost_cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_password=self.password),
            SslTestCase(
                name="pass_file_invalid_path",
                ok=False,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password_file=self.invalid_file),
            SslTestCase(
                name="key_pass_crt_ca_ciphers_server_and_client",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_ciphers="ECDHE-RSA-AES256-GCM-SHA384",
                client_password=self.password),
            SslTestCase(
                name="pap-sha256_auth",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_auth_type=AUTH_TYPE_PAP_SHA256,
                client_auth_type=AUTH_TYPE_PAP_SHA256),
            SslTestCase(
                name="chap-sha1_auth",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_auth_type=AUTH_TYPE_CHAP_SHA1,
                client_auth_type=AUTH_TYPE_CHAP_SHA1),
            SslTestCase(
                name="pap-sha256_auth_no_ssl",
                ok=False,
                expected_error=tarantool.error.NetworkError,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_auth_type=AUTH_TYPE_CHAP_SHA1,
                client_transport=DEFAULT_TRANSPORT,
                client_auth_type=AUTH_TYPE_CHAP_SHA1),
            SslTestCase(
                name="auth_type_mismatch",
                ok=False,
                expected_error=tarantool.error.DatabaseError,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_auth_type=AUTH_TYPE_PAP_SHA256,
                client_auth_type=AUTH_TYPE_CHAP_SHA1),
            SslTestCase(
                name="auth_type_use_server_id",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_auth_type=AUTH_TYPE_PAP_SHA256),
        ]
        for t in testcases:
            with self.subTest(msg=t.name):
                if t.server_password is not None \
                or t.server_password_file is not None \
                or t.client_password is not None \
                or t.server_password_file is not None:
                    skip_or_run_ssl_password_test_call(self)

                if t.server_auth_type is not None \
                or t.client_auth_type is not None:
                    skip_or_run_auth_type_test_call(self)

                srv = TarantoolServer(
                    transport=t.server_transport,
                    ssl_key_file=t.server_key_file,
                    ssl_cert_file=t.server_cert_file,
                    ssl_ca_file=t.server_ca_file,
                    ssl_ciphers=t.server_ciphers,
                    ssl_password=t.server_password,
                    ssl_password_file=t.server_password_file,
                    auth_type=t.server_auth_type)
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

                con = None
                try:
                    con = tarantool.Connection(
                        srv.host, srv.args['primary'],
                        user="test",
                        password="test",
                        transport=t.client_transport,
                        ssl_key_file=t.client_key_file,
                        ssl_cert_file=t.client_cert_file,
                        ssl_ca_file=t.client_ca_file,
                        ssl_ciphers=t.client_ciphers,
                        ssl_password=t.client_password,
                        ssl_password_file=t.client_password_file,
                        connection_timeout=0.5,
                        socket_timeout=0.5,
                        auth_type=t.client_auth_type)

                    self.assertEqual(con.insert('space_1', [1])[0], [1])
                    self.assertEqual(len(con.select('space_1')), 1)
                    self.assertTrue(t.ok)
                except t.expected_error:
                    self.assertFalse(t.ok)
                finally:
                    self.stop_srv(srv)
                    self.stop_con(con)

    @unittest.skipIf(sys.platform.startswith("win"),
                     'Pool tests on windows platform are not supported')
    def test_pool(self):
        testcases = [
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
                name="pass_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.password),
            SslTestCase(
                name="pass_file_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password_file=self.password_file),
            SslTestCase(
                name="pap-sha256_auth",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_auth_type=AUTH_TYPE_PAP_SHA256,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_auth_type=AUTH_TYPE_PAP_SHA256),
            SslTestCase(
                name="auth_type_use_server_id",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_auth_type=AUTH_TYPE_PAP_SHA256),
        ]
        for t in testcases:
            cnt = 5
            with self.subTest(msg=t.name):
                if t.server_password is not None \
                or t.server_password_file is not None \
                or t.client_password is not None \
                or t.server_password_file is not None:
                    skip_or_run_ssl_password_test_call(self)

                if t.server_auth_type is not None \
                or t.client_auth_type is not None:
                    skip_or_run_auth_type_test_call(self)

                addrs = []
                servers = []
                for i in range(cnt):
                    srv = TarantoolServer(
                        transport=t.server_transport,
                        ssl_key_file=t.server_key_file,
                        ssl_cert_file=t.server_cert_file,
                        ssl_ca_file=t.server_ca_file,
                        ssl_ciphers=t.server_ciphers,
                        ssl_password=t.server_password,
                        ssl_password_file=t.server_password_file,
                        auth_type=t.server_auth_type)
                    srv.script = 'test/suites/box.lua'
                    srv.start()
                    srv.admin("""
                    box.schema.user.create('test', { password = 'test' })
                    """.replace('\n', ' '))
                    srv.admin("""
                    box.schema.user.grant('test', 'execute,read,write', 'universe')
                    """.replace('\n', ' '))
                    servers.append(srv)
                    addr = {
                        'host': srv.host,
                        'port': srv.args['primary'],
                        'transport': t.client_transport,
                    }
                    if t.client_key_file is not None:
                        addr['ssl_key_file'] = t.client_key_file
                    if t.client_cert_file is not None:
                        addr['ssl_cert_file'] = t.client_cert_file
                    if t.client_ca_file is not None:
                        addr['ssl_ca_file'] = t.client_ca_file
                    if t.client_ciphers is not None:
                        addr['ssl_ciphers'] = t.client_ciphers
                    if t.client_password is not None:
                        addr['ssl_password'] = t.client_password
                    if t.client_password_file is not None:
                        addr['ssl_password_file'] = t.client_password_file
                    if t.client_auth_type is not None:
                        addr['auth_type'] = t.client_auth_type
                    addrs.append(addr)

                pool = None
                try:
                    pool = tarantool.ConnectionPool(
                        addrs=addrs,
                        user='test',
                        password='test',
                        connection_timeout=1,
                        socket_timeout=1)
                    self.assertSequenceEqual(
                        pool.eval('return box.info().ro', mode=tarantool.Mode.RW),
                        [False])
                except t.expected_error:
                    self.assertFalse(t.ok)
                finally:
                    self.stop_pool(pool)
                    self.stop_servers(servers)

    def test_mesh(self):
        testcases = [
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
                name="pass_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password=self.password),
            SslTestCase(
                name="pass_file_key_encrypt",
                ok=True,
                server_key_file=self.key_enc_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_password=self.password,
                client_key_file=self.key_enc_file,
                client_cert_file=self.cert_file,
                client_password_file=self.password_file),
            SslTestCase(
                name="pap-sha256_auth",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_auth_type=AUTH_TYPE_PAP_SHA256,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file,
                client_auth_type=AUTH_TYPE_PAP_SHA256),
            SslTestCase(
                name="auth_type_use_server_id",
                ok=True,
                server_key_file=self.key_file,
                server_cert_file=self.cert_file,
                server_ca_file=self.ca_file,
                server_auth_type=AUTH_TYPE_PAP_SHA256,
                client_key_file=self.key_file,
                client_cert_file=self.cert_file,
                client_ca_file=self.ca_file),
        ]
        for t in testcases:
            cnt = 5
            with self.subTest(msg=t.name):
                if t.server_password is not None \
                or t.server_password_file is not None \
                or t.client_password is not None \
                or t.server_password_file is not None:
                    skip_or_run_ssl_password_test_call(self)

                if t.server_auth_type is not None \
                or t.client_auth_type is not None:
                    skip_or_run_auth_type_test_call(self)

                addrs = []
                servers = []
                for i in range(cnt):
                    srv = TarantoolServer(
                        transport=t.server_transport,
                        ssl_key_file=t.server_key_file,
                        ssl_cert_file=t.server_cert_file,
                        ssl_ca_file=t.server_ca_file,
                        ssl_ciphers=t.server_ciphers,
                        ssl_password=t.server_password,
                        ssl_password_file=t.server_password_file,
                        auth_type=t.server_auth_type)
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
                    addr = {
                        'host': srv.host,
                        'port': srv.args['primary'],
                        'transport': t.client_transport,
                    }
                    if t.client_key_file is not None:
                        addr['ssl_key_file'] = t.client_key_file
                    if t.client_cert_file is not None:
                        addr['ssl_cert_file'] = t.client_cert_file
                    if t.client_ca_file is not None:
                        addr['ssl_ca_file'] = t.client_ca_file
                    if t.client_ciphers is not None:
                        addr['ssl_ciphers'] = t.client_ciphers
                    if t.client_password is not None:
                        addr['ssl_password'] = t.client_password
                    if t.client_password_file is not None:
                        addr['ssl_password_file'] = t.client_password_file
                    if t.client_auth_type is not None:
                        addr['auth_type'] = t.client_auth_type
                    addrs.append(addr)

                mesh = None
                try:
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
                            servers[i].clean()
                    servers = []
                except t.expected_error:
                    self.assertFalse(t.ok)
                finally:
                    self.stop_mesh(mesh)
                    self.stop_servers(servers)
