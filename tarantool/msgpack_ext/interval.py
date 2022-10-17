"""
Tarantool `datetime.interval`_ extension type support module.

The interval MessagePack representation looks like this:

.. code-block:: text

    +--------+-------------------------+-------------+----------------+
    | MP_EXT | Size of packed interval | MP_INTERVAL | PackedInterval |
    +--------+-------------------------+-------------+----------------+

Packed interval consists of:

* Packed number of non-zero fields.
* Packed non-null fields.

Each packed field has the following structure:

.. code-block:: text

    +----------+=====================+
    | field ID |     field value     |
    +----------+=====================+

The number of defined (non-null) fields can be zero. In this case,
the packed interval will be encoded as integer 0.

List of the field IDs:

* 0 – year
* 1 – month
* 2 – week
* 3 – day
* 4 – hour
* 5 – minute
* 6 – second
* 7 – nanosecond
* 8 – adjust

.. _datetime.interval: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-interval-type
"""

import msgpack

from tarantool.error import MsgpackError

from tarantool.msgpack_ext.types.interval import Interval, Adjust, id_map

EXT_ID = 6
"""
`datetime.interval`_ type id.
"""

def encode(obj, _):
    """
    Encode an interval object.

    :param obj: Interval to encode.
    :type: :obj: :class:`tarantool.Interval`

    :return: Encoded interval.
    :rtype: :obj:`bytes`
    """

    buf = bytes()

    count = 0
    for field_id in id_map.keys():
        field_name = id_map[field_id]
        value = getattr(obj, field_name)

        if field_name == 'adjust':
            value = value.value

        if value != 0:
            buf = buf + msgpack.packb(field_id) + msgpack.packb(value)
            count = count + 1

    buf = msgpack.packb(count) + buf

    return buf

def decode(data, unpacker):
    """
    Decode an interval object.

    :param obj: Interval to decode.
    :type obj: :obj:`bytes`

    :param unpacker: msgpack unpacker to decode fields.
    :type unpacker: :class:`msgpack.Unpacker`

    :return: Decoded interval.
    :rtype: :class:`tarantool.Interval`

    :raise: :exc:`MsgpackError`
    """

    # If MessagePack data does not contain a field value, it is zero.
    # If built not from MessagePack data, set argument values later.
    kwargs = {
        'year': 0,
        'month': 0,
        'week': 0,
        'day': 0,
        'hour': 0,
        'minute': 0,
        'sec': 0,
        'nsec': 0,
        'adjust': Adjust(0),
    }

    if len(data) != 0:
        # Unpacker object is the only way to parse
        # a sequence of values in Python msgpack module.
        unpacker.feed(data)
        field_count = unpacker.unpack()
        for _ in range(field_count):
            field_id = unpacker.unpack()
            value = unpacker.unpack()

            if field_id not in id_map:
                raise MsgpackError(f'Unknown interval field id {field_id}')

            field_name = id_map[field_id]

            if field_name == 'adjust':
                try:
                    value = Adjust(value)
                except ValueError as e:
                    raise MsgpackError(e)

            kwargs[id_map[field_id]] = value

    return Interval(**kwargs)
