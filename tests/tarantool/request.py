# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
Tests for tarantool.request module
'''
import binascii
import unittest

import tarantool.request
from tarantool.schema import Schema

class ConnectionMock(object):
    def __init__(self, default_type):
        self.schema = Schema({
            1: {
                'name': 'users',
                'default_type': default_type,
                'fields': {
                    0: ('f1', default_type),
                    1: ('f2', default_type),
                    2: ('f3', default_type),
                },
                'indexes': {
                    0: ('pk', [0, 0]),
                },
            }
        })

class RequestInsert(unittest.TestCase):
    def setUp(self):
        self.conn1 = ConnectionMock(tarantool.NUM)
        self.conn2 = ConnectionMock(tarantool.STR)

    def test__cast_to_bytes(self):
        '''
        Test binary INSERT request representation
        '''
        self.assertEqual(
            bytes(tarantool.request.RequestInsert(self.conn1, 1, (1, 2000, 30000), False)),
            binascii.unhexlify("0d000000270000000000000001000000000000000300000008010000000000000008d007000000000000083075000000000000")
        )

        self.assertEqual(
            bytes(tarantool.request.RequestInsert(self.conn2, 1, (b"AAA", b"BBBB", b"CCCCCC"), False)),
            binascii.unhexlify("0d0000001c0000000000000001000000000000000300000003414141044242424206434343434343")
        )

class RequestDelete(unittest.TestCase):
    def setUp(self):
        self.conn1 = ConnectionMock(tarantool.NUM)
        self.conn2 = ConnectionMock(tarantool.STR)

    def test__cast_to_bytes(self):
        '''
        Test binary DELETE request representation
        '''
        self.assertEqual(
            bytes(tarantool.request.RequestDelete(self.conn1, 1, 1, False)),
            binascii.unhexlify("150000001500000000000000010000000000000001000000080100000000000000")
        )

        self.assertEqual(
            bytes(tarantool.request.RequestDelete(self.conn2, 1, b"AAA", False)),
            binascii.unhexlify("15000000100000000000000001000000000000000100000003414141")
        )

        # Raises a TypeError exception because the primary key must be a scalar value (int or str)
        with self.assertRaises(TypeError):
            tarantool.request.RequestDelete(1, [1,2], False)


class RequestSelect(unittest.TestCase):
    def setUp(self):
        self.conn1 = ConnectionMock(tarantool.NUM)
        self.conn2 = ConnectionMock(tarantool.STR)

    def test__cast_to_bytes(self):
        '''
        Test binary SELECT request representation
        '''
        # select * from t1 where k0 = 1
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(self.conn1, 1, 0, [(1,)], 0, 0xffff)),
            binascii.unhexlify("110000002100000000000000010000000000000000000000ffff00000100000001000000080100000000000000"),
            "Select using integer key"
        )

        # select * from t1 where k0 = "AAA"
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(self.conn2, 1, 0, [(b"AAA",)], 0, 0xffff)),
            binascii.unhexlify("110000001c00000000000000010000000000000000000000ffff0000010000000100000003414141"),
            "Select using string key"
        )

        # select * from t1 where k0 in (1, 2, 3)
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(self.conn1, 1, 0, [(1,), (2,), (3,)], 0, 0xffff)),
            binascii.unhexlify("110000003b00000000000000010000000000000000000000ffff000003000000010000000801000000000000000100000008020000000000000001000000080300000000000000"),
            "Select multiple keys"
        )

        # select * from t1 where k0 = (1, 2)
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(self.conn1, 1, 0, [(1, 2)], 0, 0xffff)),
            binascii.unhexlify("110000002a00000000000000010000000000000000000000ffff00000100000002000000080100000000000000080200000000000000"),
            "Select using composite index"
        )

        # select * from t1 where k0 = (1, 2) or k0 = (3, 4)
        self.assertEqual(
            bytes(tarantool.request.RequestSelect(self.conn1, 1, 0, [(1, 2), (3, 4)], 0, 0xffff)),
            binascii.unhexlify("110000004000000000000000010000000000000000000000ffff0000020000000200000008010000000000000008020000000000000002000000080300000000000000080400000000000000"),
            "Select multiple keys using composite index"
        )


class RequestUpdate(unittest.TestCase):
    def setUp(self):
        self.conn = ConnectionMock(tarantool.NUM)

    def test__cast_to_bytes(self):
        '''
        Test binary UPDATE request representation
        '''

        # ------------------------------------------------------------
        # Update operation "ASSIGN" ('='), op_code = 0

        # update t17 set k51 = 0x11223344 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33, '=', 0x11223344)], False)),
            binascii.unhexlify("130000002700000000000000110000000000000001000000082200000000000000010000003300000000084433221100000000"),
            "Update: assign single integer value using an integer key"
        )

        # update t17 set k51 = 0x11223344 where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, b"ZZZZZZ", [(0x33, '=', 0x11223344)], False)),
            binascii.unhexlify("130000002500000000000000110000000000000001000000065a5a5a5a5a5a010000003300000000084433221100000000"),
            "Update: assign single integer value using a string key"
        )

        # update t17 set k51 = "NNN" where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33, '=', b"NNN")], False)),
            binascii.unhexlify("130000002200000000000000110000000000000001000000082200000000000000010000003300000000034e4e4e"),
            "Update: assign single string value using an integer key"
        )

        # update t17 set k51 = "NNN" where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, b"ZZZZZZ", [(0x33, '=', b"NNN")], False)),
            binascii.unhexlify("130000002000000000000000110000000000000001000000065a5a5a5a5a5a010000003300000000034e4e4e"),
            "Update: assign single string value using a string key"
        )

        # update t17 set k51 = 0x3333, k68 = 0x4444, k85 = 0x5555  where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33, '=', 0x3333), (0x44, '=', 0x4444), (0x55, '=', 0x5555)], False)),
            binascii.unhexlify("13000000430000000000000011000000000000000100000008220000000000000003000000330000000008333300000000000044000000000844440000000000005500000000085555000000000000"),
            "Update: assign multiple integer values using an integer key"
        )

        # update t17 set k51 = 0x3333, k68 = 0x4444, k85 = 0x5555  where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, "ZZZZZZ", [(0x33, '=', 0x3333), (0x44, '=', 0x4444), (0x55, '=', 0x5555)], False)),
            binascii.unhexlify("130000004100000000000000110000000000000001000000065a5a5a5a5a5a03000000330000000008333300000000000044000000000844440000000000005500000000085555000000000000"),
            "Update: assign multiple integer values using a string key"
        )

        # update t17 set k51 = "KKK", k68 = "LLL", k85 = "MMM"  where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33,'=', b"KKK"), (0x44,'=', b"LLL"), (0x55,'=', b"MMM")], False)),
            binascii.unhexlify("130000003400000000000000110000000000000001000000082200000000000000030000003300000000034b4b4b4400000000034c4c4c5500000000034d4d4d"),
            "Update: assign multiple string values using an integer key"
        )

        # update t17 set k51 = "KKK", k68 = "LLL", k85 = "MMM"  where k0 = "ZZZZZZ"
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, b"ZZZZZZ", [(0x33,'=', b"KKK"), (0x44,'=', b"LLL"), (0x55,'=', b"MMM")], False)),
            binascii.unhexlify("130000003200000000000000110000000000000001000000065a5a5a5a5a5a030000003300000000034b4b4b4400000000034c4c4c5500000000034d4d4d"),
            "Update: assign multiple string values using a string key"
        )

        # ------------------------------------------------------------
        # Update operation "ADD" ('+'), op_code = 1

        # update t17 set k51 = k51 + 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33, '+', 0x55)], False)),
            binascii.unhexlify("130000002700000000000000"
                               +"11000000" # space_no
                               +"00000000" # flags
                               +"01000000"    # key cardinality
                               + "082200000000000000" # key value
                               +"01000000"    # count (number of operations)
                                              # --- operation triplets ---
                               + "33000000"   # field_no = 0x33
                               + "01"         # op_code = add ('+')
                               + "085500000000000000" # field = 0x55
                               ),
            #
            "Update: ADD single integer value using an integer key"
        )

        # ------------------------------------------------------------
        # Update operation bitwise "AND" ('&'), op_code = 2

        # update t17 set k51 = k51 & 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33, '&', 0x55)], False)),
            binascii.unhexlify("130000002700000000000000" # 12 byte header
                               + "11000000"    # space_no
                               + "00000000"    # flags
                               + "01000000"    # key cardinality
                               + "082200000000000000" # key value
                               + "01000000"     # count (number of operations)
                                               # --- operation triplets ---
                               + "33000000"    # field_no = 0x33
                               + "02"          # op_code = AND ('&')
                               + "085500000000000000"  # field = 0x55
                               ),
            #
            "Update: ADD single integer value using an integer key"
        )


        # ------------------------------------------------------------
        # Update operation bitwise "XOR" ('^'), op_code = 3

        # update t17 set k51 = k51 | 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33, '^', 0x55)], False)),
            binascii.unhexlify("130000002700000000000000" # 12 byte header
                               + "11000000"    # space_no
                               + "00000000"    # flags
                               + "01000000"    # key cardinality
                               + "082200000000000000" # key value
                               + "01000000"     # count (number of operations)
                                               # --- operation triplets ---
                               + "33000000"    # field_no = 0x33
                               + "03"          # op_code = XOR ('^')
                               + "085500000000000000"  # field = 0x55
                               ),
            #
            "Update: OR single integer value using an integer key"
        )


        # ------------------------------------------------------------
        # Update operation bitwise "OR" ('|'), op_code = 4

        # update t17 set k51 = k51 | 0x55 where k0 = 0x22
        self.assertEqual(
            bytes(tarantool.request.RequestUpdate(self.conn, 0x11, 0x22, [(0x33, '|', 0x55)], False)),
            binascii.unhexlify("130000002700000000000000" # 12 byte header
                               + "11000000"    # space_no
                               + "00000000"    # flags
                               + "01000000"    # key cardinality
                               + "082200000000000000" # key value
                               + "01000000"     # count (number of operations)
                                               # --- operation triplets ---
                               + "33000000"    # field_no = 0x33
                               + "04"          # op_code = OR ('|')
                               + "085500000000000000"  # field = 0x55
                               ),
            #
            "Update: OR single integer value using an integer key"
        )
