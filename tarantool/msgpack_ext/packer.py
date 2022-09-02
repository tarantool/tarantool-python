from decimal import Decimal
from uuid import UUID
from msgpack import ExtType

import tarantool.msgpack_ext.decimal as ext_decimal
import tarantool.msgpack_ext.uuid as ext_uuid

encoders = [
    {'type': Decimal, 'ext': ext_decimal},
    {'type': UUID,    'ext': ext_uuid   },
]

def default(obj):
    for encoder in encoders:
        if isinstance(obj, encoder['type']):
            return ExtType(encoder['ext'].EXT_ID, encoder['ext'].encode(obj))
    raise TypeError("Unknown type: %r" % (obj,))
