from uuid import UUID

# https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-uuid-type
#
# The UUID MessagePack representation looks like this:
# +--------+------------+-----------------+
# | MP_EXT | MP_UUID    | UuidValue       |
# | = d8   | = 2        | = 16-byte value |
# +--------+------------+-----------------+

EXT_ID = 2

def encode(obj):
    return obj.bytes

def decode(data):
    return UUID(bytes=data)
