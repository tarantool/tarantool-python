from tarantool.msgpack_ext.types.datetime import Datetime

EXT_ID = 4

def encode(obj):
    return obj.msgpack_encode()

def decode(data):
    return Datetime(data)
