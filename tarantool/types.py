"""
Additional Tarantool type definitions.
"""

import typing
from dataclasses import dataclass

@dataclass
class BoxError():
    """
    Type representing Tarantool `box.error`_ object: a single
    MP_ERROR_STACK object with a link to the previous stack error.

    .. _box.error: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_error/error/
    """

    type: typing.Union[str, bytes]
    """
    Type that implies source, for example ``"ClientError"``.

    Value type depends on :class:`~tarantool.Connection`
    :paramref:`~tarantool.Connection.params.encoding`.
    """

    file: typing.Union[str, bytes]
    """
    Source code file where error was caught.

    Value type depends on :class:`~tarantool.Connection`
    :paramref:`~tarantool.Connection.params.encoding`.
    """

    line: int
    """
    Line number in source code file.
    """

    message: typing.Union[str, bytes]
    """
    Text of reason.

    Value type depends on :class:`~tarantool.Connection`
    :paramref:`~tarantool.Connection.params.encoding`.
    """

    errno: int
    """
    Ordinal number of the error.
    """

    errcode: int
    """
    Number of the error as defined in ``errcode.h``.
    """

    fields: typing.Optional[dict] = None
    """
    Additional fields depending on error type. For example, if
    :attr:`~tarantool.BoxError.type` is ``"AccessDeniedError"``,
    then it will include ``"object_type"``, ``"object_name"``,
    ``"access_type"``.
    """

    prev: typing.Optional[typing.List['BoxError']] = None
    """
    Previous error in stack.
    """


MP_ERROR_STACK = 0x00
MP_ERROR_TYPE = 0x00
MP_ERROR_FILE = 0x01
MP_ERROR_LINE = 0x02
MP_ERROR_MESSAGE = 0x03
MP_ERROR_ERRNO = 0x04
MP_ERROR_ERRCODE = 0x05
MP_ERROR_FIELDS = 0x06

def decode_box_error(err_map):
    """
    Decode MessagePack map received from Tarantool to `box.error`_
    object representation.

    :param err_map: Error MessagePack map received from Tarantool.
    :type err_map: :obj:`dict`

    :rtype: :class:`~tarantool.BoxError`

    :raises: :exc:`KeyError`
    """

    encoded_stack = err_map[MP_ERROR_STACK]

    prev = None
    for item in encoded_stack[::-1]:
        err = BoxError(
            type=item[MP_ERROR_TYPE],
            file=item[MP_ERROR_FILE],
            line=item[MP_ERROR_LINE],
            message=item[MP_ERROR_MESSAGE],
            errno=item[MP_ERROR_ERRNO],
            errcode=item[MP_ERROR_ERRCODE],
            fields=item.get(MP_ERROR_FIELDS), # omitted if empty
            prev=prev,
        )
        prev = err

    return prev

def encode_box_error(err):
    """
    Encode Python `box.error`_ representation to MessagePack map.

    :param err: Error to encode
    :type err: :obj:`tarantool.BoxError`

    :rtype: :obj:`dict`

    :raises: :exc:`KeyError`
    """

    stack = []

    while err is not None:
        dict_item = {
            MP_ERROR_TYPE: err.type,
            MP_ERROR_FILE: err.file,
            MP_ERROR_LINE: err.line,
            MP_ERROR_MESSAGE: err.message,
            MP_ERROR_ERRNO: err.errno,
            MP_ERROR_ERRCODE: err.errcode,
        }

        if err.fields is not None: # omitted if empty
            dict_item[MP_ERROR_FIELDS] = err.fields

        stack.append(dict_item)

        err = err.prev

    return {MP_ERROR_STACK: stack}
