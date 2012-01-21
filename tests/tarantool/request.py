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
            binascii.unhexlify("110000001d00000000000000010000000000000000000000ffff000001000000010000000401000000"),
            "Select using integer key"
        )

        # select * from t1 where k0 = "AAA"
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(1, 0, [(b"AAA",)], 0, 0xffff)),
            binascii.unhexlify("110000001c00000000000000010000000000000000000000ffff0000010000000100000003414141"),
            "Select using string key"
        )

        # select * from t1 where k0 in (1, 2, 3)
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(1, 0, [(1,), (2,), (3,)], 0, 0xffff)),
            binascii.unhexlify("110000002f00000000000000010000000000000000000000ffff000003000000010000000401000000010000000402000000010000000403000000"),
            "Select multiple keys"
        )

        # select * from t1 where k0 = (1, 2)
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(1, 0, [(1, 2)], 0, 0xffff)),
            binascii.unhexlify("110000002200000000000000010000000000000000000000ffff0000010000000200000004010000000402000000"),
            "Select using composite index"
        )

        # select * from t1 where k0 = (1, 2) or k0 = (3, 4)
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(1, 0, [(1, 2), (3, 4)], 0, 0xffff)),
            binascii.unhexlify("110000003000000000000000010000000000000000000000ffff00000200000002000000040100000004020000000200000004030000000404000000"),
            "Select multiple keys using composite index"
        )


class RequestUpdate(unittest.TestCase):

    def test__cast_to_bytes(self):
        '''
        Test binary UPDATE request representation
        '''

        # ------------------------------------------------------------
        # Update operation "ASSIGN" ('='), op_code = 0

        # update t17 set k51 = 0x11223344 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33, '=', 0x11223344)], False)),
            binascii.unhexlify("130000001f0000000000000011000000000000000100000004220000000100000033000000000444332211"),
            "Update: assign single integer value using an integer key"
        )

        # update t17 set k51 = 0x11223344 where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, b"ZZZZZZ", [(0x33, '=', 0x11223344)], False)),
            binascii.unhexlify("130000002100000000000000110000000000000001000000065a5a5a5a5a5a0100000033000000000444332211"),
            "Update: assign single integer value using a string key"
        )

        # update t17 set k51 = "NNN" where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33, '=', b"NNN")], False)),
            binascii.unhexlify("130000001e000000000000001100000000000000010000000422000000010000003300000000034e4e4e"),
            "Update: assign single string value using an integer key"
        )

        # update t17 set k51 = "NNN" where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, b"ZZZZZZ", [(0x33, '=', b"NNN")], False)),
            binascii.unhexlify("130000002000000000000000110000000000000001000000065a5a5a5a5a5a010000003300000000034e4e4e"),
            "Update: assign single string value using a string key"
        )

        # update t17 set k51 = 0x3333, k68 = 0x4444, k85 = 0x5555  where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33, '=', 0x3333), (0x44, '=', 0x4444), (0x55, '=', 0x5555)], False)),
            binascii.unhexlify("130000003300000000000000110000000000000001000000042200000003000000330000000004333300004400000000044444000055000000000455550000"),
            "Update: assign multiple integer values using an integer key"
        )

        # update t17 set k51 = 0x3333, k68 = 0x4444, k85 = 0x5555  where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, "ZZZZZZ", [(0x33, '=', 0x3333), (0x44, '=', 0x4444), (0x55, '=', 0x5555)], False)),
            binascii.unhexlify("130000003500000000000000110000000000000001000000065a5a5a5a5a5a03000000330000000004333300004400000000044444000055000000000455550000"),
            "Update: assign multiple integer values using a string key"
        )

        # update t17 set k51 = "KKK", k68 = "LLL", k85 = "MMM"  where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33,'=', b"KKK"), (0x44,'=', b"LLL"), (0x55,'=', b"MMM")], False)),
            binascii.unhexlify("1300000030000000000000001100000000000000010000000422000000030000003300000000034b4b4b4400000000034c4c4c5500000000034d4d4d"),
            "Update: assign multiple string values using an integer key"
        )

        # update t17 set k51 = "KKK", k68 = "LLL", k85 = "MMM"  where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, b"ZZZZZZ", [(0x33,'=', b"KKK"), (0x44,'=', b"LLL"), (0x55,'=', b"MMM")], False)),
            binascii.unhexlify("130000003200000000000000110000000000000001000000065a5a5a5a5a5a030000003300000000034b4b4b4400000000034c4c4c5500000000034d4d4d"),
            "Update: assign multiple string values using a string key"
        )

        # ------------------------------------------------------------
        # Update operation "ADD" ('+'), op_code = 1

        # update t17 set k51 = k51 + 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33, '+', 0x55)], False)),
            binascii.unhexlify("130000001f00000000000000"
                               +"11000000" # space_no
                               +"00000000" # flags
                               +"01000000"    # key cardinality
                               + "0422000000" # key value
                               +"01000000"    # count (number of operations)
                                              # --- operation triplets ---
                               + "33000000"   # field_no = 0x33
                               + "01"         # op_code = add ('+')
                               + "0455000000" # field = 0x55
                               ),
            #
            "Update: ADD single integer value using an integer key"
        )

        # ------------------------------------------------------------
        # Update operation bitwise "AND" ('&'), op_code = 2

        # update t17 set k51 = k51 & 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33, '&', 0x55)], False)),
            binascii.unhexlify("130000001f00000000000000" # 12 byte header
                               + "11000000"    # space_no
                               + "00000000"    # flags
                               + "01000000"    # key cardinality
                               + "04220000000" # key value
                               + "1000000"     # count (number of operations)
                                               # --- operation triplets ---
                               + "33000000"    # field_no = 0x33
                               + "02"          # op_code = AND ('&')
                               + "0455000000"  # field = 0x55
                               ),
            #
            "Update: ADD single integer value using an integer key"
        )


        # ------------------------------------------------------------
        # Update operation bitwise "XOR" ('^'), op_code = 3

        # update t17 set k51 = k51 | 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33, '^', 0x55)], False)),
            binascii.unhexlify("130000001f00000000000000" # 12 byte header
                               + "11000000"    # space_no
                               + "00000000"    # flags
                               + "01000000"    # key cardinality
                               + "04220000000" # key value
                               + "1000000"     # count (number of operations)
                                               # --- operation triplets ---
                               + "33000000"    # field_no = 0x33
                               + "03"          # op_code = XOR ('^')
                               + "0455000000"  # field = 0x55
                               ),
            #
            "Update: OR single integer value using an integer key"
        )


        # ------------------------------------------------------------
        # Update operation bitwise "OR" ('|'), op_code = 4

        # update t17 set k51 = k51 | 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(0x11, 0x22, [(0x33, '|', 0x55)], False)),
            binascii.unhexlify("130000001f00000000000000" # 12 byte header
                               + "11000000"    # space_no
                               + "00000000"    # flags
                               + "01000000"    # key cardinality
                               + "04220000000" # key value
                               + "1000000"     # count (number of operations)
                                               # --- operation triplets ---
                               + "33000000"    # field_no = 0x33
                               + "04"          # op_code = OR ('|')
                               + "0455000000"  # field = 0x55
                               ),
            #
            "Update: OR single integer value using an integer key"
        )

