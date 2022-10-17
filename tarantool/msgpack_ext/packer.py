"""
Tarantool `extension`_ types encoding support.

.. _extension: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/
"""

from decimal import Decimal
from uuid import UUID
from msgpack import ExtType

from tarantool.types import BoxError
from tarantool.msgpack_ext.types.datetime import Datetime
from tarantool.msgpack_ext.types.interval import Interval

import tarantool.msgpack_ext.decimal as ext_decimal
import tarantool.msgpack_ext.uuid as ext_uuid
import tarantool.msgpack_ext.error as ext_error
import tarantool.msgpack_ext.datetime as ext_datetime
import tarantool.msgpack_ext.interval as ext_interval

encoders = [
    {'type': Decimal,  'ext': ext_decimal },
    {'type': UUID,     'ext': ext_uuid    },
    {'type': BoxError, 'ext': ext_error   },
    {'type': Datetime, 'ext': ext_datetime},
    {'type': Interval, 'ext': ext_interval},
]

def default(obj, packer=None):
    """
    :class:`msgpack.Packer` encoder.

    :param obj: Object to encode.
    :type obj: :class:`decimal.Decimal` or :class:`uuid.UUID` or
         or :class:`tarantool.BoxError` or :class:`tarantool.Datetime`
         or :class:`tarantool.Interval`

    :param packer: msgpack packer to work with common types
        (like dictionary in extended error payload)
    :type packer: :class:`msgpack.Packer`, optional

    :return: Encoded value.
    :rtype: :class:`msgpack.ExtType`

    :raise: :exc:`~TypeError`
    """

    for encoder in encoders:
        if isinstance(obj, encoder['type']):
            return ExtType(encoder['ext'].EXT_ID, encoder['ext'].encode(obj, packer))
    raise TypeError("Unknown type: %r" % (obj,))
