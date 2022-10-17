"""
Tarantool `decimal`_ extension type support module.

The decimal MessagePack representation looks like this:

.. code-block:: text

    +--------+-------------------+------------+===============+
    | MP_EXT | length (optional) | MP_DECIMAL | PackedDecimal |
    +--------+-------------------+------------+===============+

``PackedDecimal`` has the following structure:

.. code-block:: text

     <--- length bytes -->
    +-------+=============+
    | scale |     BCD     |
    +-------+=============+

Here the scale is either `mp_int`_ or `mp_uint`_. Scale is the number
of digits after the decimal point

BCD is a sequence of bytes representing decimal digits of the encoded
number (each byte has two decimal digits each encoded using 4-bit
nibbles), so ``byte >> 4`` is the first digit and ``byte & 0x0f`` is
the second digit. The leftmost digit in the array is the most
significant. The rightmost digit in the array is the least significant.

The first byte of the ``BCD`` array contains the first digit of the number,
represented as follows:

.. code-block:: text

    |  4 bits           |  4 bits           |
       = 0x                = the 1st digit

(The first nibble contains ``0`` if the decimal number has an even
number of digits.) The last byte of the ``BCD`` array contains the last
digit of the number and the final nibble, represented as follows:

.. code-block:: text

    |  4 bits           |  4 bits           |
       = the last digit    = nibble

The final nibble represents the number’s sign:

* ``0x0a``, ``0x0c``, ``0x0e``, ``0x0f`` stand for plus,
* ``0x0b`` and ``0x0d`` stand for minus.

.. _decimal: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-decimal-type
.. _mp_int:
.. _mp_uint: https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
"""

from decimal import Decimal

from tarantool.error import MsgpackError, MsgpackWarning, warn

EXT_ID = 1
"""
`decimal`_ type id.
"""

TARANTOOL_DECIMAL_MAX_DIGITS = 38

def get_mp_sign(sign):
    """
    Parse decimal sign to a nibble.

    :param sign: ``'+`` or ``'-'`` symbol.
    :type sign: :obj:`str`

    :return: Decimal sigh nibble.
    :rtype: :obj:`int`

    :raise: :exc:`RuntimeError`

    :meta private:
    """

    if sign == '+':
        return 0x0c

    if sign == '-':
        return 0x0d

    raise RuntimeError

def add_mp_digit(digit, bytes_reverted, digit_count):
    """
    Append decimal digit to a binary data array.

    :param digit: Digit to add.
    :type digit: :obj:`int`

    :param bytes_reverted: Reverted array with binary data.
    :type bytes_reverted: :obj:`bytearray`

    :param digit_count: Current digit count.
    :type digit_count: :obj:`int`

    :meta private:
    """

    if digit_count % 2 == 0:
        bytes_reverted[-1] = bytes_reverted[-1] | (digit << 4)
    else:
        bytes_reverted.append(digit)

def check_valid_tarantool_decimal(str_repr, scale, first_digit_ind):
    """
    Decimal numbers have 38 digits of precision, that is, the total
    number of digits before and after the decimal point can be 38. If
    there are more digits arter the decimal point, the precision is
    lost. If there are more digits before the decimal point, error is
    thrown (Tarantool 2.10.1-0-g482d91c66).

    .. code-block:: lua

        tarantool> decimal.new('10000000000000000000000000000000000000')
        ---
        - 10000000000000000000000000000000000000
        ...

        tarantool> decimal.new('100000000000000000000000000000000000000')
        ---
        - error: incorrect value to convert to decimal as 1 argument
        ...

        tarantool> decimal.new('1.0000000000000000000000000000000000001')
        ---
        - 1.0000000000000000000000000000000000001
        ...

        tarantool> decimal.new('1.00000000000000000000000000000000000001')
        ---
        - 1.0000000000000000000000000000000000000
        ...

    In fact, there is also an exceptional case: if decimal starts with
    ``0.``, 38 digits after the decimal point are supported without the
    loss of precision.

    .. code-block:: lua

        tarantool> decimal.new('0.00000000000000000000000000000000000001')
        ---
        - 0.00000000000000000000000000000000000001
        ...

        tarantool> decimal.new('0.000000000000000000000000000000000000001')
        ---
        - 0.00000000000000000000000000000000000000
        ...

    :param str_repr: Decimal string representation.
    :type str_repr: :obj:`str`

    :param scale: Decimal scale.
    :type scale: :obj:`int`

    :param first_digit_ind: Index of the first digit in decimal string
        representation.
    :type first_digit_ind: :obj:`int`

    :return: ``True``, if decimal can be encoded to Tarantool decimal
        without precision loss. ``False`` otherwise.
    :rtype: :obj:`bool`

    :raise: :exc:`~tarantool.error.MsgpackError`

    :meta private:
    """

    if scale > 0:
        digit_count = len(str_repr) - 1 - first_digit_ind
    else:
        digit_count = len(str_repr) - first_digit_ind

    if digit_count <= TARANTOOL_DECIMAL_MAX_DIGITS:
        return True

    if (digit_count - scale) > TARANTOOL_DECIMAL_MAX_DIGITS:
        raise MsgpackError('Decimal cannot be encoded: Tarantool decimal ' + \
                           'supports a maximum of 38 digits.')

    starts_with_zero = str_repr[first_digit_ind] == '0'

    if (    (digit_count > TARANTOOL_DECIMAL_MAX_DIGITS + 1) or \
            (digit_count == TARANTOOL_DECIMAL_MAX_DIGITS + 1 \
            and not starts_with_zero)):
        warn('Decimal encoded with loss of precision: ' + \
             'Tarantool decimal supports a maximum of 38 digits.',
             MsgpackWarning)
        return False

    return True

def strip_decimal_str(str_repr, scale, first_digit_ind):
    """
    Strip decimal digits after the decimal point if decimal cannot be
    represented as Tarantool decimal without precision loss.

    :param str_repr: Decimal string representation.
    :type str_repr: :obj:`str`

    :param scale: Decimal scale.
    :type scale: :obj:`int`

    :param first_digit_ind: Index of the first digit in decimal string
        representation.
    :type first_digit_ind: :obj:`int`

    :meta private:
    """

    assert scale > 0
    # Strip extra bytes
    str_repr = str_repr[:TARANTOOL_DECIMAL_MAX_DIGITS + 1 + first_digit_ind]

    str_repr = str_repr.rstrip('0')
    str_repr = str_repr.rstrip('.')
    # Do not strips zeroes before the decimal point
    return str_repr

def encode(obj, _):
    """
    Encode a decimal object.

    :param obj: Decimal to encode.
    :type obj: :obj:`decimal.Decimal`

    :return: Encoded decimal.
    :rtype: :obj:`bytes`

    :raise: :exc:`~tarantool.error.MsgpackError`
    """

    # Non-scientific string with trailing zeroes removed
    str_repr = format(obj, 'f')

    bytes_reverted = bytearray()

    scale = 0
    for i in range(len(str_repr)):
        str_digit = str_repr[i]
        if str_digit == '.':
            scale = len(str_repr) - i - 1
            break

    if str_repr[0] == '-':
        sign = '-'
        first_digit_ind = 1
    else:
        sign = '+'
        first_digit_ind = 0

    if not check_valid_tarantool_decimal(str_repr, scale, first_digit_ind):
        str_repr = strip_decimal_str(str_repr, scale, first_digit_ind)

    bytes_reverted.append(get_mp_sign(sign))

    digit_count = 0
    # We need to update the scale after possible strip_decimal_str() 
    scale = 0

    for i in range(len(str_repr) - 1, first_digit_ind - 1, -1):
        str_digit = str_repr[i]
        if str_digit == '.':
            scale = len(str_repr) - i - 1
            continue

        add_mp_digit(int(str_digit), bytes_reverted, digit_count)
        digit_count = digit_count + 1

    # Remove leading zeroes since they already covered by scale
    for i in range(len(bytes_reverted) - 1, 0, -1):
        if bytes_reverted[i] != 0:
            break
        bytes_reverted.pop()

    bytes_reverted.append(scale)

    return bytes(bytes_reverted[::-1])


def get_str_sign(nibble):
    """
    Parse decimal sign nibble to a symbol.

    :param nibble: Decimal sign nibble.
    :type nibble: :obj:`int`

    :return: ``'+`` or ``'-'`` symbol.
    :rtype: :obj:`str`

    :raise: :exc:`MsgpackError`

    :meta private:
    """

    if nibble == 0x0a or nibble == 0x0c or nibble == 0x0e or nibble == 0x0f:
        return '+'

    if nibble == 0x0b or nibble == 0x0d:
        return '-'

    raise MsgpackError('Unexpected MP_DECIMAL sign nibble')

def add_str_digit(digit, digits_reverted, scale):
    """
    Append decimal digit to a binary data array.

    :param digit: Digit to add.
    :type digit: :obj:`int`

    :param digits_reverted: Reverted decimal string.
    :type digits_reverted: :obj:`str`

    :param scale: Decimal scale.
    :type scale: :obj:`int`

    :raise: :exc:`~tarantool.error.MsgpackError`

    :meta private:
    """

    if not (0 <= digit <= 9):
        raise MsgpackError('Unexpected MP_DECIMAL digit nibble')

    if len(digits_reverted) == scale:
        digits_reverted.append('.')

    digits_reverted.append(str(digit))

def decode(data, _):
    """
    Decode a decimal object.

    :param obj: Decimal to decode.
    :type obj: :obj:`bytes`

    :return: Decoded decimal.
    :rtype: :obj:`decimal.Decimal`

    :raise: :exc:`~tarantool.error.MsgpackError`
    """

    scale = data[0] 

    sign = get_str_sign(data[-1] & 0x0f)

    # Parse from tail since scale is counted from the tail.
    digits_reverted = []

    add_str_digit(data[-1] >> 4, digits_reverted, scale)

    for i in range(len(data) - 2, 0, -1):
        add_str_digit(data[i] & 0x0f, digits_reverted, scale)
        add_str_digit(data[i] >> 4, digits_reverted, scale)

    # Add leading zeroes in case of 0.000... number
    for i in range(len(digits_reverted), scale + 1):
        add_str_digit(0, digits_reverted, scale)

    digits_reverted.append(sign)

    str_repr = ''.join(digits_reverted[::-1])

    return Decimal(str_repr)
