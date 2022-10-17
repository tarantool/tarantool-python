"""
Tarantool `error`_ extension type support module.

Refer to :mod:`~tarantool.msgpack_ext.types.error`.

.. _error: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-error-type
"""

from tarantool.types import (
    encode_box_error,
    decode_box_error,
)

EXT_ID = 3
"""
`error`_ type id.
"""

def encode(obj, packer):
    """
    Encode an error object.

    :param obj: Error to encode.
    :type obj: :class:`tarantool.BoxError`

    :param packer: msgpack packer to encode error dictionary.
    :type packer: :class:`msgpack.Packer`

    :return: Encoded error.
    :rtype: :obj:`bytes`
    """

    err_map = encode_box_error(obj)
    return packer.pack(err_map)

def decode(data, unpacker):
    """
    Decode an error object.

    :param obj: Error to decode.
    :type obj: :obj:`bytes`

    :param unpacker: msgpack unpacker to decode error dictionary.
    :type unpacker: :class:`msgpack.Unpacker`

    :return: Decoded error.
    :rtype: :class:`tarantool.BoxError`
    """

    unpacker.feed(data)
    err_map = unpacker.unpack()
    return decode_box_error(err_map)
