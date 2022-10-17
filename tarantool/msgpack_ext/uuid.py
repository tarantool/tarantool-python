"""
Tarantool `uuid`_ extension type support module.

The UUID MessagePack representation looks like this:

.. code-block:: text

    +--------+------------+-----------------+
    | MP_EXT | MP_UUID    | UuidValue       |
    | = d8   | = 2        | = 16-byte value |
    +--------+------------+-----------------+

.. _uuid: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-uuid-type
"""

from uuid import UUID

EXT_ID = 2
"""
`uuid`_ type id.
"""

def encode(obj, _):
    """
    Encode an UUID object.

    :param obj: UUID to encode.
    :type obj: :obj:`uuid.UUID`

    :return: Encoded UUID.
    :rtype: :obj:`bytes`
    """

    return obj.bytes

def decode(data, _):
    """
    Decode an UUID object.

    :param data: UUID to decode.
    :type data: :obj:`bytes`

    :return: Decoded UUID.
    :rtype: :obj:`uuid.UUID`
    """

    return UUID(bytes=data)
