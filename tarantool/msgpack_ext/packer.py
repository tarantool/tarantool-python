from decimal import Decimal
from uuid import UUID
from msgpack import ExtType

from tarantool.msgpack_ext.types.datetime import Datetime
from tarantool.msgpack_ext.types.interval import Interval

import tarantool.msgpack_ext.decimal as ext_decimal
import tarantool.msgpack_ext.uuid as ext_uuid
import tarantool.msgpack_ext.datetime as ext_datetime
import tarantool.msgpack_ext.interval as ext_interval

encoders = [
    {'type': Decimal,  'ext': ext_decimal },
    {'type': UUID,     'ext': ext_uuid    },
    {'type': Datetime, 'ext': ext_datetime},
    {'type': Interval, 'ext': ext_interval},
]

def default(obj):
    for encoder in encoders:
        if isinstance(obj, encoder['type']):
            return ExtType(encoder['ext'].EXT_ID, encoder['ext'].encode(obj))
    raise TypeError("Unknown type: %r" % (obj,))
