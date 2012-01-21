# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Tests for tarantool.response module
'''
import binascii
import sys
import unittest

py3 = sys.version_info.major >= 3
from_hex = lambda x: binascii.unhexlify(''.join(x.split()))
to_hex = lambda x: binascii.hexlify(x)


import tarantool.response


class field(unittest.TestCase):
    '''
    Tests for response.field class
    '''

    def test__init_from_unicode(self):
        '''
        Test field instantiation from str or unicode value
        '''
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
        '''
        Test field instantiation from raw bytes value
        '''
        # Word "Test" in cyrillic utf-8 encoded
        value = b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82"

        self.assertEqual(
            tarantool.response.field(value),
            b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82",
            "Instantiate field from bytes"
        )


    def test__init_from_int(self):
        '''
        Test field instantiation from integer value
        '''
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



class Response(unittest.TestCase):
    '''
    Tests for response.Response
    '''

    def test__init_single(self):
        '''
        Test Response instance creation: unpack single record
        '''
        header = from_hex(
            "0d000000" # request_type = 0x0d ("insert")
            "1b000000" # body_length = 27
            "00000000" # request_id
        )

        body = from_hex(
            "00000000"    # return_code = 0
            "01000000"    # count = 1
            "0b000000"    # tuple_size = 11
            "02000000"    # cardinality = 2
                          # tuple = (1, "JKLMN")
            "04 01000000" + "05 4a4b4c4d4e")

        self.assertEqual(
            tarantool.response.Response(header, body),
            [(b"\x01\x00\x00\x00", b"JKLMN")],
            "Create Response instance: single record"
        )

    def test__init_multiple(self):
        '''
        Test Response instance creation: unpack multiple records
        '''
        header = from_hex(
            "11000000" # request_type = 0x11 ("select")
            "51000000" # body_length = 32
            "00000000" # request_id
        )
        body = from_hex(
            "00000000" # return_code = 0
            "03000000" # count = 3
            "10000000" # tuple_size = 16 (0x10)
            "02000000" # cardinality = 2
                       # tuple = (1, "1111111111")
            "04 01000000" + "0a 31313131313131313131"
            "10000000" # tuple_size = 16 (0x10)
            "02000000" # cardinality = 2
                       # tuple = (2, "2222222222")
            "04 02000000" + "0a 32323232323232323232"
            "11000000" # tuple_size = 17 (0x11)
            "04000000" # cardinality = 4
                       # tuple = (3, "LLL", "MMM", "NNN")
            "04 03000000" + "03 4c4c4c" + "03 4d4d4d" + "03 4e4e4e"
        )

        self.assertEqual(
            tarantool.response.Response(header, body),
            [(b"\x01\x00\x00\x00", b"1111111111"),
            (b"\x02\x00\x00\x00", b"2222222222"),
            (b"\x03\x00\x00\x00", b"LLL", b"MMM", b"NNN")],
            "Create Response instance - multiple records with multiple fields"
        )

    def test__init_attrs(self):

        # Check instanse attributes

        header = from_hex("0d00000014000000 11223344")
        body = from_hex("00000000010000000400000002000000014b015a")
        r = tarantool.response.Response(header, body)

        self.assertEqual(r.return_code, 0, "Check return_code property")
        self.assertIsNone(r.return_message, "Check return_message property")
        self.assertEqual(r.return_code, 0, "Check completion_status property")
        self.assertEqual(r.rowcount, 1, "Check rowcount property")
        self.assertEqual(r._body_length, 20, "Check _body_length attribute")
        self.assertEqual(r._request_id, 0x44332211, "Check _request_id attribute")
