import sys
import uuid

ENCODING_DEFAULT = "utf-8"

from base64 import decodebytes as base64_decode

def strxor(rhs, lhs):
    """
    XOR two strings.

    :param rhs: String to XOR.
    :type rhs: :obj:`str` or :obj:`bytes`

    :param lhs: Another string to XOR.
    :type lhs: :obj:`str` or :obj:`bytes`

    :rtype: :obj:`bytes`
    """

    return bytes([x ^ y for x, y in zip(rhs, lhs)])

def wrap_key(*args, first=True, select=False):
    """
    Wrap request key in list, if needed.

    :param args: Method args.
    :type args: :obj:`tuple`

    :param first: ``True`` if this is the first recursion iteration.
    :type first: :obj:`bool`

    :param select: ``True`` if wrapping SELECT request key.
    :type select: :obj:`bool`

    :rtype: :obj:`list`
    """

    if len(args) == 0 and select:
        return []
    if len(args) == 1:
        if isinstance(args[0], (list, tuple)) and first:
            return wrap_key(*args[0], first=False, select=select)
        elif args[0] is None and select:
            return []

    return list(args)


def version_id(major, minor, patch):
    """
    :param major: Version major number.
    :type major: :obj:`int`

    :param minor: Version minor number.
    :type minor: :obj:`int`

    :param patch: Version patch number.
    :type patch: :obj:`int`

    :return: Unique version identificator for 8-bytes major, minor,
        patch numbers.
    :rtype: :obj:`int`
    """

    return (((major << 8) | minor) << 8) | patch

def greeting_decode(greeting_buf):
    """
    Decode Tarantool server greeting.

    :param greeting_buf: Binary greetings data.
    :type greeting_buf: :obj:`bytes`

    :rtype: ``Greeting`` dataclass with ``version_id``, ``protocol``,
        ``uuid``, ``salt`` fields

    :raise: :exc:`~Exception`
    """

    class Greeting:
        version_id = 0
        protocol = None
        uuid = None
        salt = None

    # Tarantool 1.6.6
    # Tarantool 1.6.6-102-g4e9bde2
    # Tarantool 1.6.8 (Binary) 3b151c25-4c4a-4b5d-8042-0f1b3a6f61c3
    # Tarantool 1.6.8-132-g82f5424 (Lua console)
    result = Greeting()
    try:
        (product, _, tail) = greeting_buf[0:63].decode().partition(' ')
        if product.startswith("Tarantool "):
            raise Exception()
        # Parse a version string - 1.6.6-83-gc6b2129 or 1.6.7
        (version, _, tail) = tail.partition(' ')
        version = version.split('-')[0].split('.')
        result.version_id = version_id(int(version[0]), int(version[1]),
                                       int(version[2]))
        if len(tail) > 0 and tail[0] == '(':
            (protocol, _, tail) = tail[1:].partition(') ')
            # Extract protocol name - a string between (parentheses)
            result.protocol = protocol
            if result.protocol != "Binary":
                return result
            # Parse UUID for binary protocol
            (uuid_buf, _, tail) = tail.partition(' ')
            if result.version_id >= version_id(1, 6, 7):
                result.uuid = uuid.UUID(uuid_buf.strip())
        elif result.version_id < version_id(1, 6, 7):
            # Tarantool < 1.6.7 doesn't add "(Binary)" to greeting
            result.protocol = "Binary"
        elif len(tail.strip()) != 0:
            raise Exception("x")  # Unsupported greeting
        result.salt = base64_decode(greeting_buf[64:])[:20]
        return result
    except Exception as e:
        print('exx', e)
        raise ValueError("Invalid greeting: " + str(greeting_buf))
