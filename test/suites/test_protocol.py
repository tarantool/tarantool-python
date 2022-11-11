import sys
import pkg_resources
import unittest
import uuid

import tarantool
from tarantool.utils import greeting_decode, version_id

from .lib.tarantool_server import TarantoolServer

from tarantool.const import (
    IPROTO_FEATURE_STREAMS,
    IPROTO_FEATURE_TRANSACTIONS,
    IPROTO_FEATURE_ERROR_EXTENSION,
    IPROTO_FEATURE_WATCHERS,
)

class TestSuite_Protocol(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' PROTOCOL '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()
        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'])
        self.adm = self.srv.admin

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

    def test_00_greeting_1_6(self):
        buf = "Tarantool 1.6.6                                                \n" + \
              "AtQnb9SAIaKazZZy9lJKvK3urtbjCEJndhRVbslSPGc=                   \n";
        greeting = greeting_decode(buf.encode())
        self.assertEqual(greeting.version_id, version_id(1, 6, 6))
        self.assertEqual(greeting.protocol, "Binary")
        self.assertIsNone(greeting.uuid)
        self.assertIsNotNone(greeting.salt)

    def test_01_greeting_1_6_with_tag(self):
        buf = "Tarantool 1.6.6-232-gcf47324                                   \n" + \
              "AtQnb9SAIaKazZZy9lJKvK3urtbjCEJndhRVbslSPGc=                   \n";
        greeting = greeting_decode(buf.encode())
        self.assertEqual(greeting.version_id, version_id(1, 6, 6))
        self.assertEqual(greeting.protocol, "Binary")
        self.assertIsNone(greeting.uuid)
        self.assertIsNotNone(greeting.salt)

    def test_02_greeting_1_6_console(self):
        buf = "Tarantool 1.6.6-132-g82f5424 (Lua console)                     \n" + \
              "type 'help' for interactive help                               \n";
        greeting = greeting_decode(buf.encode())
        self.assertEqual(greeting.version_id, version_id(1, 6, 6))
        self.assertEqual(greeting.protocol, "Lua console")
        self.assertIsNone(greeting.uuid)
        self.assertIsNone(greeting.salt)

    def test_03_greeting_1_6_7(self):
        buf = "Tarantool 1.6.7 (Binary) 52dc2837-8001-48fe-bdce-c493c04599ce  \n" + \
              "Z+2F+VRlyK1nKT82xQtxqEggMtkTK5RtPYf27JryRas=                   \n";
        greeting = greeting_decode(buf.encode())
        self.assertEqual(greeting.version_id, version_id(1, 6, 7))
        self.assertEqual(greeting.protocol, "Binary")
        self.assertEqual(greeting.uuid,
                         uuid.UUID('52dc2837-8001-48fe-bdce-c493c04599ce'))
        self.assertIsNotNone(greeting.salt)

    def test_04_protocol(self):
        # First Tarantool protocol version (1) was introduced between
        # 2.10.0-beta1 and 2.10.0-beta2. Versions 2 and 3 were also
        # introduced between 2.10.0-beta1 and 2.10.0-beta2. Version 4
        # was introduced between 2.10.0-beta2 and 2.10.0-rc1 and reverted
        # back to version 3 in the same version interval.
        # Tarantool 2.10.3 still has version 3.
        if self.adm.tnt_version >= pkg_resources.parse_version('2.10.0'):
            self.assertTrue(self.con._protocol_version >= 3)
            self.assertEqual(self.con._features[IPROTO_FEATURE_ERROR_EXTENSION], True)
        else:
            self.assertIsNone(self.con._protocol_version)
            self.assertEqual(self.con._features[IPROTO_FEATURE_ERROR_EXTENSION], False)

        self.assertEqual(self.con._features[IPROTO_FEATURE_STREAMS], False)
        self.assertEqual(self.con._features[IPROTO_FEATURE_TRANSACTIONS], False)
        self.assertEqual(self.con._features[IPROTO_FEATURE_WATCHERS], False)

    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()

