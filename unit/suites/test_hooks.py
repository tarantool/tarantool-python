# -*- coding: utf-8 -*-

import sys
import unittest
from datetime import datetime

import tarantool
from .lib.tarantool_server import TarantoolServer


def object_unpack(obj):
    if obj.get("__type__") == "datetime":
        return datetime.fromtimestamp(obj['obj'])
    return obj


def object_pack(obj):
    if isinstance(obj, datetime):
        return {"__type__": "datetime", "obj": obj.timestamp()}
    return obj


def list_unpack(lst):
    if lst and lst[0] == "_my_datetime":
        return datetime.fromtimestamp(lst[1])
    return lst


def list_pack(obj):
    if isinstance(obj, datetime):
        return ["_my_datetime", obj.timestamp()]
    return obj


class TestSuite_DefaultAndObjectHook(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' PACK/UNPACK HOOKs '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'unit/suites/box.lua'
        self.srv.start()
        self.srv.admin.execute("simple_return = function(a) return a end")
        self.srv.admin.execute(
            "box.schema.user.grant('guest','execute','universe')")

    def test_00_not_set(self):
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'])

        ret = con.call("simple_return",
            {"__type__": "datetime", "obj": 1546300800})
        self.assertDictEqual(ret._data[0],
            {"__type__": "datetime", "obj": 1546300800})

        with self.assertRaises(TypeError):
            con.call("simple_return", datetime.fromtimestamp(1546300800))

    def test_01_set_default(self):
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
            pack_default=object_pack)

        ret = con.call("simple_return", datetime.fromtimestamp(1546300800))
        self.assertDictEqual(ret._data[0],
            {"__type__": "datetime", "obj": 1546300800})

    def test_02_set_object_hook(self):
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
            unpack_object_hook=object_unpack)

        ret = con.call("simple_return",
            {"__type__": "datetime", "obj": 1546300800})
        self.assertEqual(ret._data[0], datetime.fromtimestamp(1546300800))

        ret = con.call("simple_return",
            {"__type__": "1datetime", "obj": 1546300800})
        self.assertEqual(ret._data[0],
            {"__type__": "1datetime", "obj": 1546300800})

    def test_03_set_object_hook_and_default(self):
        dt = datetime.fromtimestamp(1546300800)
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
            unpack_object_hook=object_unpack, pack_default=object_pack)
        ret = con.call("simple_return", dt)
        self.assertEqual(ret._data[0], dt)


    def test_04_set_list_hook_and_default(self):
        dt = datetime.fromtimestamp(1546300800)
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
            unpack_list_hook=list_unpack, pack_default=list_pack)
        ret = con.call("simple_return", dt)
        self.assertEqual(ret._data[0], dt)
