"""
Tarantool `extension`_ types encoding support.

.. _extension: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/
"""

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
    """
    :class:`msgpack.Packer` encoder.

    :param obj: Object to encode.
    :type obj: :class:`decimal.Decimal` or :class:`uuid.UUID` or
        :class:`tarantool.Datetime` or :class:`tarantool.Interval`

    :return: Encoded value.
    :rtype: :class:`msgpack.ExtType`

    :raise: :exc:`~TypeError`
    """

    for encoder in encoders:
        if isinstance(obj, encoder['type']):
            return ExtType(encoder['ext'].EXT_ID, encoder['ext'].encode(obj))
    raise TypeError("Unknown type: %r" % (obj,))
