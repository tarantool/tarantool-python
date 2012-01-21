# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Tests for tarantool.request module
'''

import binascii
import unittest


import tarantool.request


class RequestInsert(unittest.TestCase):

    def test__cast_to_bytes(self):
        '''
        Test binary INSERT request representation
        '''
        self.assertEqual(
            bytes(tarantool.request.RequestInsert(1, (1, 2000, 30000), False)),
            binascii.unhexlify("0d0000001b00000000000000010000000000000003000000040100000004d00700000430750000")
        )

        self.assertEqual(
            bytes(tarantool.request.RequestInsert(1, (b"AAA", b"BBBB", b"CCCCCC"), False)),
            binascii.unhexlify("0d0000001c0000000000000001000000000000000300000003414141044242424206434343434343")
        )


class RequestDelete(unittest.TestCase):

    def test__cast_to_bytes(self):
        '''
        Test binary DELETE request representation
        '''
        self.assertEqual(
            bytes(tarantool.request.RequestDelete(1, 1, False)),
            binascii.unhexlify("1500000011000000000000000100000000000000010000000401000000")
        )

        self.assertEqual(
            bytes(tarantool.request.RequestDelete(1, b"AAA", False)),
            binascii.unhexlify("15000000100000000000000001000000000000000100000003414141")
        )

        # Raises a TypeError exception because the primary key must be a scalar value (int or str)
        with self.assertRaises(TypeError):
            tarantool.request.RequestDelete(1, [1,2], False)


class RequestSelect(unittest.TestCase):

    def test__cast_to_bytes(self):
        '''
        Test binary SELECT request representation
        '''
        # select * from t1 where k0 = 1
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(1, 0, [(1,)], 0, 0xffff)),
            binascii.unhexlify("110000001d00000000000000010000000000000000000000ffff000001000000010000000401000000")
        )

        # select * from t1 where k0 = "AAA"
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(1, 0, [(b"AAA",)], 0, 0xffff)),
            binascii.unhexlify("110000001c00000000000000010000000000000000000000ffff0000010000000100000003414141")
        )

        # select * from t1 where k0 in (1, 2, 3)
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(1, 0, [(1,), (2,), (3,)], 0, 0xffff)),
            binascii.unhexlify("110000002f00000000000000010000000000000000000000ffff000003000000010000000401000000010000000402000000010000000403000000")
        )
