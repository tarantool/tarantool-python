"""
Tarantool `datetime`_ extension type support module.

The datetime MessagePack representation looks like this:

.. code-block:: text

    +---------+----------------+==========+-----------------+
    | MP_EXT  | MP_DATETIME    | seconds  | nsec; tzoffset; |
    | = d7/d8 | = 4            |          | tzindex;        |
    +---------+----------------+==========+-----------------+

MessagePack data contains:

* Seconds (8 bytes) as an unencoded 64-bit signed integer stored in the
  little-endian order.
* The optional fields (8 bytes), if any of them have a non-zero value.
  The fields include nsec (4 bytes), tzoffset (2 bytes), and
  tzindex (2 bytes) packed in the little-endian order.

``seconds`` is seconds since Epoch, where the epoch is the point where
the time starts, and is platform dependent. For Unix, the epoch is
January 1, 1970, 00:00:00 (UTC). Tarantool uses a ``double`` type, see a
structure definition in src/lib/core/datetime.h and reasons in
`datetime RFC`_.

``nsec`` is nanoseconds, fractional part of seconds. Tarantool uses
``int32_t``, see a definition in src/lib/core/datetime.h.

``tzoffset`` is timezone offset in minutes from UTC. Tarantool uses
``int16_t`` type, see a structure definition in src/lib/core/datetime.h.

``tzindex`` is Olson timezone id. Tarantool uses ``int16_t`` type, see
a structure definition in src/lib/core/datetime.h. If both
``tzoffset`` and ``tzindex`` are specified, ``tzindex`` has the
preference and the ``tzoffset`` value is ignored.

.. _datetime RFC: https://github.com/tarantool/tarantool/wiki/Datetime-internals#intervals-in-c
"""

from tarantool.msgpack_ext.types.datetime import (
    NSEC_IN_SEC,
    SEC_IN_MIN,
    Datetime,
)
import tarantool.msgpack_ext.types.timezones as tt_timezones

from tarantool.error import MsgpackError

EXT_ID = 4
"""
`datetime`_ type id.
"""

BYTEORDER = 'little'

SECONDS_SIZE_BYTES  = 8
NSEC_SIZE_BYTES     = 4
TZOFFSET_SIZE_BYTES = 2
TZINDEX_SIZE_BYTES  = 2


def get_int_as_bytes(data, size):
    """
    Get binary representation of integer value.

    :param data: Integer value.
    :type data: :obj:`int`

    :param size: Integer size, in bytes.
    :type size: :obj:`int`

    :return: Encoded integer.
    :rtype: :obj:`bytes`

    :meta private:
    """

    return data.to_bytes(size, byteorder=BYTEORDER, signed=True)

def encode(obj, _):
    """
    Encode a datetime object.

    :param obj: Datetime to encode.
    :type: :obj: :class:`tarantool.Datetime`

    :return: Encoded datetime.
    :rtype: :obj:`bytes`

    :raise: :exc:`tarantool.Datetime.msgpack_encode` exceptions
    """

    seconds = obj.value // NSEC_IN_SEC
    nsec = obj.nsec
    tzoffset = obj.tzoffset

    tz = obj.tz
    if tz != '':
        tzindex = tt_timezones.timezoneToIndex[tz]
    else:
        tzindex = 0

    buf = get_int_as_bytes(seconds, SECONDS_SIZE_BYTES)

    if (nsec != 0) or (tzoffset != 0) or (tzindex != 0):
        buf = buf + get_int_as_bytes(nsec, NSEC_SIZE_BYTES)
        buf = buf + get_int_as_bytes(tzoffset, TZOFFSET_SIZE_BYTES)
        buf = buf + get_int_as_bytes(tzindex, TZINDEX_SIZE_BYTES)

    return buf


def get_bytes_as_int(data, cursor, size):
    """
    Get integer value from binary data.

    :param data: MessagePack binary data.
    :type data: :obj:`bytes`

    :param cursor: Index after last parsed byte.
    :type cursor: :obj:`int`

    :param size: Integer size, in bytes.
    :type size: :obj:`int`

    :return: First value: parsed integer, second value: new cursor
        position.
    :rtype: first value: :obj:`int`, second value: :obj:`int`

    :meta private:
    """

    part = data[cursor:cursor + size]
    return int.from_bytes(part, BYTEORDER, signed=True), cursor + size

def decode(data, _):
    """
    Decode a datetime object.

    :param obj: Datetime to decode.
    :type obj: :obj:`bytes`

    :return: Decoded datetime.
    :rtype: :class:`tarantool.Datetime`

    :raise: :exc:`~tarantool.error.MsgpackError`,
        :exc:`tarantool.Datetime` exceptions
    """

    cursor = 0
    seconds, cursor = get_bytes_as_int(data, cursor, SECONDS_SIZE_BYTES)

    data_len = len(data)
    if data_len == (SECONDS_SIZE_BYTES + NSEC_SIZE_BYTES + \
                    TZOFFSET_SIZE_BYTES + TZINDEX_SIZE_BYTES):
        nsec, cursor     = get_bytes_as_int(data, cursor, NSEC_SIZE_BYTES)
        tzoffset, cursor = get_bytes_as_int(data, cursor, TZOFFSET_SIZE_BYTES)
        tzindex, cursor  = get_bytes_as_int(data, cursor, TZINDEX_SIZE_BYTES)
    elif data_len == SECONDS_SIZE_BYTES:
        nsec = 0
        tzoffset = 0
        tzindex = 0
    else:
        raise MsgpackError(f'Unexpected datetime payload length {data_len}')

    if tzindex != 0:
        if tzindex not in tt_timezones.indexToTimezone:
            raise MsgpackError(f'Failed to decode datetime with unknown tzindex "{tzindex}"')
        tz = tt_timezones.indexToTimezone[tzindex]
        return Datetime(timestamp=seconds, nsec=nsec, tz=tz,
                        timestamp_since_utc_epoch=True)
    elif tzoffset != 0:
        return Datetime(timestamp=seconds, nsec=nsec, tzoffset=tzoffset,
                        timestamp_since_utc_epoch=True)
    else:
        return Datetime(timestamp=seconds, nsec=nsec,
                        timestamp_since_utc_epoch=True)
