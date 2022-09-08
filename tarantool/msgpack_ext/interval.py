from tarantool.msgpack_ext.types.interval import Interval

EXT_ID = 6

def encode(obj):
    return obj.msgpack_encode()

def decode(data):
    return Interval(data)
