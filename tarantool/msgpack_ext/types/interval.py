import msgpack
from enum import Enum

from tarantool.error import MsgpackError

# https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-interval-type
#
# The interval MessagePack representation looks like this:
# +--------+-------------------------+-------------+----------------+
# | MP_EXT | Size of packed interval | MP_INTERVAL | PackedInterval |
# +--------+-------------------------+-------------+----------------+
# Packed interval consists of:
# - Packed number of non-zero fields.
# - Packed non-null fields.
#
# Each packed field has the following structure:
# +----------+=====================+
# | field ID |     field value     |
# +----------+=====================+
#
# The number of defined (non-null) fields can be zero. In this case,
# the packed interval will be encoded as integer 0.
#
# List of the field IDs:
# - 0 – year
# - 1 – month
# - 2 – week
# - 3 – day
# - 4 – hour
# - 5 – minute
# - 6 – second
# - 7 – nanosecond
# - 8 – adjust

id_map = {
    0: 'year',
    1: 'month',
    2: 'week',
    3: 'day',
    4: 'hour',
    5: 'minute',
    6: 'sec',
    7: 'nsec',
    8: 'adjust',
}

# https://github.com/tarantool/c-dt/blob/cec6acebb54d9e73ea0b99c63898732abd7683a6/dt_arithmetic.h#L34
class Adjust(Enum):
    EXCESS = 0 # DT_EXCESS in c-dt, "excess" in Tarantool
    NONE = 1 # DT_LIMIT in c-dt, "none" in Tarantool
    LAST = 2 # DT_SNAP in c-dt, "last" in Tarantool

class Interval():
    def __init__(self, data=None, *, year=0, month=0, week=0,
                 day=0, hour=0, minute=0, sec=0,
                 nsec=0, adjust=Adjust.NONE):
        # If msgpack data does not contain a field value, it is zero.
        # If built not from msgpack data, set argument values later. 
        self.year = 0
        self.month = 0
        self.week = 0
        self.day = 0
        self.hour = 0
        self.minute = 0
        self.sec = 0
        self.nsec = 0
        self.adjust = Adjust(0)

        if data is not None:
            if not isinstance(data, bytes):
                raise ValueError('data argument (first positional argument) ' +
                                 'expected to be a "bytes" instance')

            if len(data) == 0:
                return

            # To create an unpacker is the only way to parse
            # a sequence of values in Python msgpack module.
            unpacker = msgpack.Unpacker()
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

                setattr(self, id_map[field_id], value)
        else:
            self.year = year
            self.month = month
            self.week = week
            self.day = day
            self.hour = hour
            self.minute = minute
            self.sec = sec
            self.nsec = nsec
            self.adjust = adjust

    def __add__(self, other):
        if not isinstance(other, Interval):
            raise TypeError(f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'")

        # Tarantool saves adjust of the first argument
        #
        # Tarantool 2.10.1-0-g482d91c66
        #
        # tarantool> dt1 = datetime.interval.new{year = 2, adjust='last'}
        # ---
        # ...
        #
        # tarantool> dt2 = datetime.interval.new{year = 1, adjust='excess'}
        # ---
        # ...
        #
        # tarantool> (dt1 + dt2).adjust
        # ---
        # - 'cdata<enum 112>: 2'
        # ...

        return Interval(
            year = self.year + other.year,
            month = self.month + other.month,
            day = self.day + other.day,
            hour = self.hour + other.hour,
            minute = self.minute + other.minute,
            sec = self.sec + other.sec,
            nsec = self.nsec + other.nsec,
            adjust = self.adjust,
        )

    def __sub__(self, other):
        if not isinstance(other, Interval):
            raise TypeError(f"unsupported operand type(s) for -: '{type(self)}' and '{type(other)}'")

        # Tarantool saves adjust of the first argument
        #
        # Tarantool 2.10.1-0-g482d91c66
        #
        # tarantool> dt1 = datetime.interval.new{year = 2, adjust='last'}
        # ---
        # ...
        #
        # tarantool> dt2 = datetime.interval.new{year = 1, adjust='excess'}
        # ---
        # ...
        #
        # tarantool> (dt1 - dt2).adjust
        # ---
        # - 'cdata<enum 112>: 2'
        # ...

        return Interval(
            year = self.year - other.year,
            month = self.month - other.month,
            day = self.day - other.day,
            hour = self.hour - other.hour,
            minute = self.minute - other.minute,
            sec = self.sec - other.sec,
            nsec = self.nsec - other.nsec,
            adjust = self.adjust,
        )

    def __eq__(self, other):
        if not isinstance(other, Interval):
            return False

        # Tarantool interval compare is naive too
        #
        # Tarantool 2.10.1-0-g482d91c66
        #
        # tarantool> datetime.interval.new{hour=1} == datetime.interval.new{min=60}
        # ---
        # - false
        # ...

        for field_id in id_map.keys():
            field_name = id_map[field_id]
            if getattr(self, field_name) != getattr(other, field_name):
                return False

        return True

    def __repr__(self):
        return f'tarantool.Interval(year={self.year}, month={self.month}, day={self.day}, ' + \
               f'hour={self.hour}, minute={self.minute}, sec={self.sec}, ' + \
               f'nsec={self.nsec}, adjust={self.adjust})'

    __str__ = __repr__

    def msgpack_encode(self):
        buf = bytes()

        count = 0
        for field_id in id_map.keys():
            field_name = id_map[field_id]
            value = getattr(self, field_name)

            if field_name == 'adjust':
                value = value.value

            if value != 0:
                buf = buf + msgpack.packb(field_id) + msgpack.packb(value)
                count = count + 1

        buf = msgpack.packb(count) + buf

        return buf
