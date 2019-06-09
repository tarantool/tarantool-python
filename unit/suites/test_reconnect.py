# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import unittest
import warnings
import tarantool
from .lib.tarantool_server import TarantoolServer


class TestSuite_Reconnect(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' RECONNECT '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'unit/suites/box.lua'

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

    def test_01_simple(self):
        # Create a connection, but don't connect it.
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                   connect_now=False)

        # Trigger a reconnection due to server unavailability.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.assertRaises(tarantool.error.NetworkError):
                con.ping()

        # Start a server and verify that the reconnection
        # succeeds.
        self.srv.start()
        self.assertIs(con.ping(notime=True), "Success")

        # Close the connection and stop the server.
        con.close()
        self.srv.stop()

    def test_02_wrong_auth(self):
        # Create a connection with wrong credentials, but don't
        # connect it.
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                   connect_now=False, user='not_exist')

        # Start a server.
        self.srv.start()

        # Trigger a reconnection due to wrong credentials.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.assertRaises(tarantool.error.DatabaseError):
                con.ping()

        # Set right credentials and verify that the reconnection
        # succeeds.
        con.user = None
        self.assertIs(con.ping(notime=True), "Success")

        # Close the connection and stop the server.
        con.close()
        self.srv.stop()

    def test_03_connect_after_close(self):
        # Start a server and connect to it.
        self.srv.start()
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'])
        con.ping()

        # Close the connection and connect again.
        con.close()
        con.connect()

        # Verify that the connection is alive.
        con.ping()

        # Close the connection and stop the server.
        con.close()
        self.srv.stop()

    @classmethod
    def tearDownClass(self):
        self.srv.clean()
