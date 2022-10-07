"""
Tarantool `datetime`_ extension type support module.

Refer to :mod:`~tarantool.msgpack_ext.types.datetime`.

.. _datetime: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-datetime-type
"""

from tarantool.msgpack_ext.types.datetime import Datetime

EXT_ID = 4
"""
`datetime`_ type id.
"""

def encode(obj):
    """
    Encode a datetime object.

    :param obj: Datetime to encode.
    :type: :obj: :class:`tarantool.Datetime`

    :return: Encoded datetime.
    :rtype: :obj:`bytes`

    :raise: :exc:`tarantool.Datetime.msgpack_encode` exceptions
    """

    return obj.msgpack_encode()

def decode(data):
    """
    Decode a datetime object.

    :param obj: Datetime to decode.
    :type obj: :obj:`bytes`

    :return: Decoded datetime.
    :rtype: :class:`tarantool.Datetime`

    :raise: :exc:`tarantool.Datetime` exceptions
    """

    return Datetime(data)
