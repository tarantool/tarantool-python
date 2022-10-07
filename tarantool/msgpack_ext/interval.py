"""
Tarantool `datetime.interval`_ extension type support module.

Refer to :mod:`~tarantool.msgpack_ext.types.interval`.

.. _datetime.interval: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-interval-type
"""

from tarantool.msgpack_ext.types.interval import Interval

EXT_ID = 6
"""
`datetime.interval`_ type id.
"""

def encode(obj):
    """
    Encode an interval object.

    :param obj: Interval to encode.
    :type: :obj: :class:`tarantool.Interval`

    :return: Encoded interval.
    :rtype: :obj:`bytes`

    :raise: :exc:`tarantool.Interval.msgpack_encode` exceptions
    """

    return obj.msgpack_encode()

def decode(data):
    """
    Decode an interval object.

    :param obj: Interval to decode.
    :type obj: :obj:`bytes`

    :return: Decoded interval.
    :rtype: :class:`tarantool.Interval`

    :raise: :exc:`tarantool.Interval` exceptions
    """

    return Interval(data)
