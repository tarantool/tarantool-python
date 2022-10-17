"""
Tarantool `extension`_ types decoding support.

.. _extension: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/
"""

import tarantool.msgpack_ext.decimal as ext_decimal
import tarantool.msgpack_ext.uuid as ext_uuid
import tarantool.msgpack_ext.error as ext_error
import tarantool.msgpack_ext.datetime as ext_datetime
import tarantool.msgpack_ext.interval as ext_interval

decoders = {
    ext_decimal.EXT_ID : ext_decimal.decode ,
    ext_uuid.EXT_ID    : ext_uuid.decode    ,
    ext_error.EXT_ID   : ext_error.decode   ,
    ext_datetime.EXT_ID: ext_datetime.decode,
    ext_interval.EXT_ID: ext_interval.decode,
}

def ext_hook(code, data, unpacker=None):
    """
    :class:`msgpack.Unpacker` decoder.

    :param code: MessagePack extension type code.
    :type code: :obj:`int`

    :param data: MessagePack extension type data.
    :type data: :obj:`bytes`

    :param unpacker: msgpack unpacker to work with common types
        (like dictionary in extended error payload)
    :type unpacker: :class:`msgpack.Unpacker`, optional

    :return: Decoded value.
    :rtype: :class:`decimal.Decimal` or :class:`uuid.UUID` or
         or :class:`tarantool.BoxError` or :class:`tarantool.Datetime`
         or :class:`tarantool.Interval`

    :raise: :exc:`NotImplementedError`
    """

    if code in decoders:
        return decoders[code](data, unpacker)
    raise NotImplementedError("Unknown msgpack extension type code %d" % (code,))
