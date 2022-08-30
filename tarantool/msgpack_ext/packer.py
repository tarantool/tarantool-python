from decimal import Decimal
from msgpack import ExtType

import tarantool.msgpack_ext.decimal as ext_decimal

def default(obj):
    if isinstance(obj, Decimal):
        return ExtType(ext_decimal.EXT_ID, ext_decimal.encode(obj))
    raise TypeError("Unknown type: %r" % (obj,))
