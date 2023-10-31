"""
This module tests work with connection over socket fd.
"""
import os.path
# pylint: disable=missing-class-docstring,missing-function-docstring

import socket
import sys
import unittest

import tarantool
from .lib.skip import skip_or_run_box_session_new_tests
from .lib.tarantool_server import TarantoolServer, find_port
from .utils import assert_admin_success


def find_python():
    for _dir in os.environ["PATH"].split(os.pathsep):
        exe = os.path.join(_dir, "python")
        if os.access(exe, os.X_OK):
            return os.path.abspath(exe)
    raise RuntimeError("Can't find python executable in " + os.environ["PATH"])


class TestSuiteSocketFD(unittest.TestCase):
    EVAL_USER = "return box.session.user()"

    @classmethod
    def setUpClass(cls):
        print(' SOCKET FD '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

        cls.srv = TarantoolServer()
        cls.srv.script = 'test/suites/box.lua'
        cls.srv.start()
        cls.tcp_port = find_port()

        # Start tcp server to test work with blocking sockets.
        # pylint: disable=consider-using-f-string
        resp = cls.srv.admin("""
            local socket = require('socket')

            box.cfg{}
            box.schema.user.create('test', {password = 'test', if_not_exists = true})
            box.schema.user.grant('test', 'read,write,execute,create', 'universe',
                                   nil, {if_not_exists = true})
            box.schema.user.grant('guest', 'execute', 'universe',
                                   nil, {if_not_exists = true})

            socket.tcp_server('0.0.0.0', %d, function(s)
                if not s:nonblock(true) then
                    s:close()
                    return
                end
                box.session.new({
                    type = 'binary',
                    fd = s:fd(),
                    user = 'test',
                })
                s:detach()
            end)

            box.schema.create_space('test', {
                format = {{type='unsigned', name='id'}},
                if_not_exists = true,
            })
            box.space.test:create_index('primary')

            return true
        """ % cls.tcp_port)
        assert_admin_success(resp)

    @skip_or_run_box_session_new_tests
    def setUp(self):
        # Prevent a remote tarantool from clean our session.
        if self.srv.is_started():
            self.srv.touch_lock()

    def _get_tt_sock(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.srv.host, self.tcp_port))
        return sock

    def test_01_incorrect_params(self):
        cases = {
            "host and socket_fd": {
                "args": {"host": "123", "socket_fd": 3},
                "msg": "specifying both socket_fd and host/port is not allowed",
            },
            "port and socket_fd": {
                "args": {"port": 14, "socket_fd": 3},
                "msg": "specifying both socket_fd and host/port is not allowed",
            },
            "empty": {
                "args": {},
                "msg": r"host/port.* port.* or socket_fd",
            },
            "only host": {
                "args": {"host": "localhost"},
                "msg": "when specifying host, it is also necessary to specify port",
            },
        }

        for name, case in cases.items():
            with self.subTest(msg=name):
                with self.assertRaisesRegex(tarantool.Error, case["msg"]):
                    tarantool.Connection(**case["args"])

    def test_02_socket_fd_connect(self):
        sock = self._get_tt_sock()
        conn = tarantool.connect(None, None, socket_fd=sock.fileno())
        sock.detach()
        try:
            self.assertSequenceEqual(conn.eval(self.EVAL_USER), ["test"])
        finally:
            conn.close()

    def test_03_socket_fd_re_auth(self):
        sock = self._get_tt_sock()
        conn = tarantool.connect(None, None, socket_fd=sock.fileno(), user="guest")
        sock.detach()
        try:
            self.assertSequenceEqual(conn.eval(self.EVAL_USER), ["guest"])
        finally:
            conn.close()

    @unittest.skipIf(sys.platform.startswith("win"),
                     "Skip on Windows since it uses remote server")
    def test_04_tarantool_made_socket(self):
        python_exe = find_python()
        cwd = os.getcwd()
        side_script_path = os.path.join(cwd, "test", "suites", "sidecar.py")

        # pylint: disable=consider-using-f-string
        ret_code, err = self.srv.admin("""
            local socket = require('socket')
            local popen = require('popen')
            local os = require('os')
            local s1, s2 = socket.socketpair('AF_UNIX', 'SOCK_STREAM', 0)

            --[[ Tell sidecar which fd use to connect. --]]
            os.setenv('SOCKET_FD', tostring(s2:fd()))

            --[[ Tell sidecar where find `tarantool` module. --]]
            os.setenv('PYTHONPATH', (os.getenv('PYTHONPATH') or '') .. ':' .. '%s')

            box.session.new({
                type = 'binary',
                fd = s1:fd(),
                user = 'test',
            })
            s1:detach()

            local ph, err = popen.new({'%s', '%s'}, {
                stdout = popen.opts.PIPE,
                stderr = popen.opts.PIPE,
                inherit_fds = {s2:fd()},
            })

            if err ~= nil then
                return 1, err
            end

            ph:wait()

            local status_code = ph:info().status.exit_code
            local stderr = ph:read({stderr=true}):rstrip()
            return status_code, stderr
        """ % (cwd, python_exe, side_script_path))
        self.assertIsNone(err, err)
        self.assertEqual(ret_code, 0)

    def test_05_socket_fd_pool(self):
        sock = self._get_tt_sock()
        pool = tarantool.ConnectionPool(
            addrs=[{'host': None, 'port': None, 'socket_fd': sock.fileno()}]
        )
        sock.detach()
        try:
            self.assertSequenceEqual(pool.eval(self.EVAL_USER, mode=tarantool.Mode.ANY), ["test"])
        finally:
            pool.close()

    def test_06_socket_fd_mesh(self):
        sock = self._get_tt_sock()
        mesh = tarantool.MeshConnection(
            host=None,
            port=None,
            socket_fd=sock.fileno()
        )
        sock.detach()
        try:
            self.assertSequenceEqual(mesh.eval(self.EVAL_USER), ["test"])
        finally:
            mesh.close()

    @classmethod
    def tearDownClass(cls):
        cls.srv.stop()
        cls.srv.clean()
