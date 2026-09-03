"""
This module provides utility functions for the package.
"""

from base64 import decodebytes as base64_decode
from dataclasses import dataclass
import re
import typing
import uuid
import socket

from tarantool.error import ConfigurationError

ENCODING_DEFAULT = "utf-8"

DSN_DEFAULT_HOST = "127.0.0.1"
"""
Host to use if a DSN consists of a port only.
"""

DSN_UNIX_PREFIX = "unix/:"
"""
Prefix of a Unix socket address in a DSN.
"""

DSN_SCHEME_RE = re.compile(r'[A-Za-z][A-Za-z0-9+.\-]*://')
"""
Scheme of a DSN. Only a scheme at the very beginning of a DSN is
recognized as such, the same way Tarantool does it.

:meta private:
"""

DSN_OPTIONS: typing.Tuple[str, ...] = (
    'transport',
    'ssl_key_file',
    'ssl_cert_file',
    'ssl_ca_file',
    'ssl_ciphers',
    'ssl_password',
    'ssl_password_file',
    'auth_type',
)
"""
Tarantool URI parameters allowed in a DSN query.
"""


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
        if args[0] is None and select:
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


@dataclass
class Greeting():
    """
    Connection greeting info.
    """

    version_id: typing.Optional = 0
    """
    :type: :obj:`tuple` or :obj:`list`
    """

    protocol: typing.Optional[str] = None
    """
    :type: :obj:`str`, optional
    """

    uuid: typing.Optional[str] = None
    """
    :type: :obj:`str`, optional
    """

    salt: typing.Optional[str] = None
    """
    :type: :obj:`str`, optional
    """


def greeting_decode(greeting_buf):
    """
    Decode Tarantool server greeting.

    :param greeting_buf: Binary greetings data.
    :type greeting_buf: :obj:`bytes`

    :rtype: ``Greeting`` dataclass with ``version_id``, ``protocol``,
        ``uuid``, ``salt`` fields

    :raise: :exc:`~Exception`
    """

    # Tarantool 1.6.6
    # Tarantool 1.6.6-102-g4e9bde2
    # Tarantool 1.6.8 (Binary) 3b151c25-4c4a-4b5d-8042-0f1b3a6f61c3
    # Tarantool 1.6.8-132-g82f5424 (Lua console)
    result = Greeting()
    try:
        (product, _, tail) = greeting_buf[0:63].decode().partition(' ')
        if product.startswith("Tarantool "):
            raise ValueError()
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
            raise ValueError("x")  # Unsupported greeting
        result.salt = base64_decode(greeting_buf[64:])[:20]
        return result
    except ValueError as exc:
        print('exx', exc)
        raise ValueError("Invalid greeting: " + str(greeting_buf)) from exc


def _dsn_error(dsn: str, msg: str) -> ConfigurationError:
    """
    Build a DSN parse error.

    :param dsn: Source DSN.
    :type dsn: :obj:`str`

    :param msg: Error description.
    :type msg: :obj:`str`

    :rtype: :exc:`~tarantool.error.ConfigurationError`

    :meta private:
    """

    return ConfigurationError(f'DSN "{dsn}": {msg}')


def _is_dsn_port(port_str: str) -> bool:
    """
    Check whether a DSN substring is a port.

    :param port_str: Port substring.
    :type port_str: :obj:`str`

    :rtype: :obj:`bool`

    :meta private:
    """

    return port_str.isascii() and port_str.isdigit()


def _is_dsn_userinfo(userinfo: str) -> bool:
    """
    Check whether a DSN substring before an ``@`` is a user name with
    a password rather than a part of a Unix socket path. Tarantool
    allows neither ``/`` nor more than one ``:`` in a user name and a
    password, so ``/tmp/tt@1.sock`` is a socket path, not a user name.

    :param userinfo: Substring before the first ``@``.
    :type userinfo: :obj:`str`

    :rtype: :obj:`bool`

    :meta private:
    """

    return '/' not in userinfo


def _parse_dsn_port(dsn: str, port_str: str) -> int:
    """
    Parse the port of a DSN.

    :param dsn: Source DSN.
    :type dsn: :obj:`str`

    :param port_str: Port substring.
    :type port_str: :obj:`str`

    :rtype: :obj:`int`

    :raise: :exc:`~tarantool.error.ConfigurationError`

    :meta private:
    """

    if not _is_dsn_port(port_str):
        raise _dsn_error(dsn, f'port "{port_str}" is not a number')
    port = int(port_str)
    if port < 1 or port > 65535:
        raise _dsn_error(dsn, 'port must be in range [1, 65535]')
    return port


def _parse_dsn_address(
        dsn: str,
        address: str) -> typing.Tuple[typing.Optional[str],
                                      typing.Union[int, str]]:
    """
    Parse the address of a DSN. For a Unix socket address, host is
    ``None`` and port is a socket path.

    :param dsn: Source DSN.
    :type dsn: :obj:`str`

    :param address: Address substring.
    :type address: :obj:`str`

    :return: `(host, port)` pair.
    :rtype: :obj:`tuple`

    :raise: :exc:`~tarantool.error.ConfigurationError`

    :meta private:
    """
    # pylint: disable=too-many-return-statements,too-many-branches

    if address.startswith(DSN_UNIX_PREFIX):
        path = address[len(DSN_UNIX_PREFIX):]
        if not path:
            raise _dsn_error(dsn, 'Unix socket path is empty')
        if not path.startswith(('/', './')):
            raise _dsn_error(dsn, f'Unix socket path "{path}" is neither '
                                  'absolute nor started with "./"')
        return None, path

    if address.startswith(('/', './')):
        return None, address

    if '/' in address:
        raise _dsn_error(dsn, f'address "{address}" is neither a host with '
                              'a port nor a Unix socket path')

    if address.startswith('['):
        delim = address.find(']')
        if delim == -1:
            raise _dsn_error(dsn, 'IPv6 address is not closed with "]"')
        host, tail = address[1:delim], address[delim + 1:]
        try:
            socket.inet_pton(socket.AF_INET6, host)
        except (OSError, ValueError):
            raise _dsn_error(dsn, f'"{host}" is not an IPv6 address') from None
        if not tail.startswith(':'):
            raise _dsn_error(dsn, 'port is not specified')
        return host, _parse_dsn_port(dsn, tail[1:])

    if ':' in address:
        host, port_str = address.rsplit(':', 1)
        if not host:
            raise _dsn_error(dsn, 'host value is empty')
        if ':' in host:
            raise _dsn_error(dsn, 'IPv6 address must be enclosed in "[]"')
        return host, _parse_dsn_port(dsn, port_str)

    if _is_dsn_port(address):
        return DSN_DEFAULT_HOST, _parse_dsn_port(dsn, address)

    raise _dsn_error(dsn, 'port is not specified')


def _parse_dsn_options(dsn: str, query: str) -> typing.Dict[str, str]:
    """
    Parse the query of a DSN into connection options.

    :param dsn: Source DSN.
    :type dsn: :obj:`str`

    :param query: Query substring.
    :type query: :obj:`str`

    :rtype: :obj:`dict`

    :raise: :exc:`~tarantool.error.ConfigurationError`

    :meta private:
    """

    options: typing.Dict[str, str] = {}
    for option_str in query.split('&'):
        if not option_str:
            continue
        name, delim, value = option_str.partition('=')
        if not delim:
            raise _dsn_error(dsn, f'option "{name}" has no value')
        if name not in DSN_OPTIONS:
            raise _dsn_error(dsn, f'unknown option "{name}"')
        options[name] = value
    return options


def parse_dsn(dsn: str) -> typing.Dict[str, typing.Any]:
    """
    Parse a Tarantool DSN string into :class:`~tarantool.Connection`
    parameters.

    Expected format is
    ``[scheme://][user[:password]@]host:port[?option=value&...]``.
    A Unix socket address is either ``unix/:path`` or a path itself,
    absolute or started with ``./``; an IPv6 address must be enclosed
    in ``[]``. The scheme, if any, is ignored, as Tarantool itself
    ignores it, but it is recognized only at the very beginning of a
    DSN. Values are not percent-decoded, so neither ``@`` nor ``/``
    is allowed in a user name or a password. Whitespace is not
    allowed anywhere. Allowed options are the Tarantool URI parameters
    listed in :data:`~tarantool.utils.DSN_OPTIONS`.

    :param dsn: Tarantool server DSN.
    :type dsn: :obj:`str`

    :return: Keyword arguments for :class:`~tarantool.Connection`.
        Only the parameters explicitly set in the DSN are present.
    :rtype: :obj:`dict`

    :raise: :exc:`~tarantool.error.ConfigurationError`
    """

    if not isinstance(dsn, str):
        raise ConfigurationError('DSN should be of a string type')

    source = dsn
    if not dsn:
        raise ConfigurationError('DSN should not be an empty string')

    if any(char.isspace() for char in dsn):
        raise _dsn_error(source, 'whitespace is not allowed')

    params: typing.Dict[str, typing.Any] = {}

    scheme = DSN_SCHEME_RE.match(dsn)
    if scheme:
        dsn = dsn[scheme.end():]

    query = ''
    if '?' in dsn:
        dsn, query = dsn.split('?', 1)

    userinfo, delim, tail = dsn.partition('@')
    if delim and _is_dsn_userinfo(userinfo):
        dsn = tail
        if '@' in dsn:
            raise _dsn_error(source, '"@" is not allowed in a user name '
                                     'or a password')
        user, delim, password = userinfo.partition(':')
        if not user:
            raise _dsn_error(source, 'user value is empty')
        if ':' in password:
            raise _dsn_error(source, '":" is not allowed in a password')
        params['user'] = user
        if delim:
            params['password'] = password

    if not dsn:
        raise _dsn_error(source, 'address is not specified')

    params['host'], params['port'] = _parse_dsn_address(source, dsn)
    params.update(_parse_dsn_options(source, query))

    return params
