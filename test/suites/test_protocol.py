# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import unittest
from tarantool.utils import greeting_decode, version_id
import uuid

class TestSuite_Protocol(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' PROTOCOL '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

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
