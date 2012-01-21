# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Tests for tarantool.response module
'''
import binascii
import sys
import unittest

py3 = sys.version_info.major >= 3

import tarantool.response


class field(unittest.TestCase):

    def test__init_from_unicode(self):

        # Word "Test" in cyrillic utf-8 encoded
        if py3:
            value = str(b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82", "utf-8")
        else:
            value = unicode(b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82", "utf-8")

        self.assertEqual(
            tarantool.response.field(value),
            b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82",
            "Instantiate field from unicode string"
        )


    def test__init_from_bytes(self):

        # Word "Test" in cyrillic utf-8 encoded
        value = b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82"

        self.assertEqual(
            tarantool.response.field(value),
            b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82",
            "Instantiate field from bytes"
        )


    def test__init_from_int(self):

        self.assertEqual(
            tarantool.response.field(0),
            b"\x00\x00\x00\x00",
            "Instantiate field from 32 bit integer value 0"
        )

        self.assertEqual(
            tarantool.response.field(0x11223344),
            b"\x44\x33\x22\x11",
            "Instantiate field from 32 bit integer value 0x11223344"
        )

        self.assertEqual(
            tarantool.response.field(0x7fffffff),
            b"\xff\xff\xff\x7f",
            "Instantiate field from 32 bit integer value 0x7fffffff"
        )

        self.assertEqual(
            tarantool.response.field(0xffffffff),
            b"\xff\xff\xff\xff",
            "Instantiate field from 32 bit integer value 0xffffffff"
        )

        self.assertEqual(
            tarantool.response.field(0xffffffffffffffff),
            b"\xff\xff\xff\xff\xff\xff\xff\xff",
            "Instantiate field from 64 bit integer value 0xffffffffffffffff"
        )

        self.assertEqual(
            tarantool.response.field(0x0100000000000000),
            b"\x00\x00\x00\x00\x00\x00\x00\x01",
            "Instantiate field from 64 bit integer value 0x0100000000000000"
        )

        self.assertEqual(
            tarantool.response.field(0x1122334455667788),
            b"\x88\x77\x66\x55\x44\x33\x22\x11",
            "Instantiate field from 64 bit integer value 0x1122334455667788"
        )

        # Out of range
        with self.assertRaises(ValueError):
            tarantool.response.field(0xffffffffffffffff+1)
            tarantool.response.field(-1)

        # Unsupported argument type
        with self.assertRaises(TypeError):
                tarantool.response.field(None)
                tarantool.response.field([1,2,3])


    def test__cast_to_int(self):
        '''
        Test type casting from field to int
        '''
        for i in (0, 0x11, 0x1122, 0x112233, 0xffffffff, 0xffffffffffffffff):
            f = tarantool.response.field(i)
            self.assertEqual(
                int(f),
                i,
                "Cast field instance to int, value = %d"%i
            )

        # Can't cast string value to int
        f = tarantool.response.field(b"not an int value")
        with self.assertRaises(ValueError):
            int(f)


    def test__cast_to_str(self):
        '''
        Test type casting from field to str or unicode
        '''
        # Word "Test" in cyrillic utf-8 encoded
        if py3:
            self.assertEqual(
                str(tarantool.response.field(b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82")),
                str(b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82", "utf-8"),
                "Cast field instance to unicode")
        else:
            self.assertEqual(
                unicode(tarantool.response.field(b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82")),
                unicode(b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82", "utf-8"),
                "Cast field instance to unicode")



