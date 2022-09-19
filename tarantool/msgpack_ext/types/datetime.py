from copy import deepcopy

import pandas
import pytz

# https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-datetime-type
#
# The datetime MessagePack representation looks like this:
# +---------+----------------+==========+-----------------+
# | MP_EXT  | MP_DATETIME    | seconds  | nsec; tzoffset; |
# | = d7/d8 | = 4            |          | tzindex;        |
# +---------+----------------+==========+-----------------+
# MessagePack data contains:
#
# * Seconds (8 bytes) as an unencoded 64-bit signed integer stored in the
#   little-endian order.
# * The optional fields (8 bytes), if any of them have a non-zero value.
#   The fields include nsec (4 bytes), tzoffset (2 bytes), and
#   tzindex (2 bytes) packed in the little-endian order.
#
# seconds is seconds since Epoch, where the epoch is the point where the time
# starts, and is platform dependent. For Unix, the epoch is January 1,
# 1970, 00:00:00 (UTC). Tarantool uses a double type, see a structure
# definition in src/lib/core/datetime.h and reasons in
# https://github.com/tarantool/tarantool/wiki/Datetime-internals#intervals-in-c
#
# nsec is nanoseconds, fractional part of seconds. Tarantool uses int32_t, see
# a definition in src/lib/core/datetime.h.
#
# tzoffset is timezone offset in minutes from UTC. Tarantool uses a int16_t type,
# see a structure definition in src/lib/core/datetime.h.
#
# tzindex is Olson timezone id. Tarantool uses a int16_t type, see a structure
# definition in src/lib/core/datetime.h. If both tzoffset and tzindex are
# specified, tzindex has the preference and the tzoffset value is ignored.

SECONDS_SIZE_BYTES  = 8
NSEC_SIZE_BYTES     = 4
TZOFFSET_SIZE_BYTES = 2
TZINDEX_SIZE_BYTES  = 2

BYTEORDER = 'little'

NSEC_IN_SEC = 1000000000
NSEC_IN_MKSEC = 1000
SEC_IN_MIN = 60

def get_bytes_as_int(data, cursor, size):
    part = data[cursor:cursor + size]
    return int.from_bytes(part, BYTEORDER, signed=True), cursor + size

def get_int_as_bytes(data, size):
    return data.to_bytes(size, byteorder=BYTEORDER, signed=True)

def compute_offset(timestamp):
    utc_offset = timestamp.tzinfo.utcoffset(timestamp)

    # `None` offset is a valid utcoffset implementation,
    # but it seems that pytz timezones never return `None`:
    # https://github.com/pandas-dev/pandas/issues/15986
    assert utc_offset is not None

    # There is no precision loss since offset is in minutes
    return int(utc_offset.total_seconds()) // SEC_IN_MIN

def msgpack_decode(data):
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

    total_nsec = seconds * NSEC_IN_SEC + nsec
    datetime = pandas.to_datetime(total_nsec, unit='ns')

    if tzindex != 0:
        raise NotImplementedError
    elif tzoffset != 0:
        tzinfo = pytz.FixedOffset(tzoffset)
        return datetime.replace(tzinfo=pytz.UTC).tz_convert(tzinfo)
    else:
        return datetime

class Datetime():
    def __init__(self, data=None, *, timestamp=None, year=None, month=None,
                 day=None, hour=None, minute=None, sec=None, nsec=None,
                 tzoffset=0):
        if data is not None:
            if not isinstance(data, bytes):
                raise ValueError('data argument (first positional argument) ' +
                                 'expected to be a "bytes" instance')

            self._datetime = msgpack_decode(data)
            return

        # The logic is same as in Tarantool, refer to datetime API.
        # https://www.tarantool.io/en/doc/latest/reference/reference_lua/datetime/new/
        if timestamp is not None:
            if ((year is not None) or (month is not None) or \
                    (day is not None) or (hour is not None) or \
                    (minute is not None) or (sec is not None)):
                raise ValueError('Cannot provide both timestamp and year, month, ' +
                                 'day, hour, minute, sec')

            if nsec is not None:
                if not isinstance(timestamp, int):
                    raise ValueError('timestamp must be int if nsec provided')

                total_nsec = timestamp * NSEC_IN_SEC + nsec
                datetime = pandas.to_datetime(total_nsec, unit='ns')
            else:
                datetime = pandas.to_datetime(timestamp, unit='s')
        else:
            if nsec is not None:
                microsecond = nsec // NSEC_IN_MKSEC
                nanosecond = nsec % NSEC_IN_MKSEC
            else:
                microsecond = 0
                nanosecond = 0

            datetime = pandas.Timestamp(year=year, month=month, day=day,
                                        hour=hour, minute=minute, second=sec,
                                        microsecond=microsecond,
                                        nanosecond=nanosecond)

        if tzoffset != 0:
            tzinfo = pytz.FixedOffset(tzoffset)
            datetime = datetime.replace(tzinfo=tzinfo)

        self._datetime = datetime

    def __eq__(self, other):
        if isinstance(other, Datetime):
            return self._datetime == other._datetime
        elif isinstance(other, pandas.Timestamp):
            return self._datetime == other
        else:
            return False

    def __str__(self):
        return self._datetime.__str__()

    def __repr__(self):
        return f'datetime: {self._datetime.__repr__()}'

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, deepcopy(v, memo))
        return result

    @property
    def year(self):
        return self._datetime.year

    @property
    def month(self):
        return self._datetime.month

    @property
    def day(self):
        return self._datetime.day

    @property
    def hour(self):
        return self._datetime.hour

    @property
    def minute(self):
        return self._datetime.minute

    @property
    def sec(self):
        return self._datetime.second

    @property
    def nsec(self):
        # microseconds + nanoseconds
        return self._datetime.value % NSEC_IN_SEC

    @property
    def timestamp(self):
        return self._datetime.timestamp()

    @property
    def tzoffset(self):
        if self._datetime.tzinfo is not None:
            return compute_offset(self._datetime)
        return 0

    @property
    def value(self):
        return self._datetime.value

    def msgpack_encode(self):
        seconds = self.value // NSEC_IN_SEC
        nsec = self.nsec
        tzoffset = self.tzoffset
        tzindex = 0

        buf = get_int_as_bytes(seconds, SECONDS_SIZE_BYTES)

        if (nsec != 0) or (tzoffset != 0) or (tzindex != 0):
            buf = buf + get_int_as_bytes(nsec, NSEC_SIZE_BYTES)
            buf = buf + get_int_as_bytes(tzoffset, TZOFFSET_SIZE_BYTES)
            buf = buf + get_int_as_bytes(tzindex, TZINDEX_SIZE_BYTES)

        return buf
