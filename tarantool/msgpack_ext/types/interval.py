"""
Tarantool `datetime.interval`_ extension type implementation module.
"""

from enum import Enum

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
    """
    Interval adjustment mode for year and month arithmetic. Refer to
    :meth:`~tarantool.Datetime.__add__`.
    """

    EXCESS = 0
    """
    Overflow mode.
    """

    NONE = 1
    """
    Only truncation toward the end of month is performed.
    """

    LAST = 2
    """
    Mode when day snaps to the end of month, if it happens.
    """

class Interval():
    """
    Class representing Tarantool `datetime.interval`_ info.

    You can create :class:`~tarantool.Interval` objects either
    from MessagePack data or by using the same API as in Tarantool:

    .. code-block:: python

        di = tarantool.Interval(year=-1, month=2, day=3,
                                hour=4, minute=-5, sec=6,
                                nsec=308543321,
                                adjust=tarantool.IntervalAdjust.NONE)

    Its attributes (same as in init API) are exposed, so you can
    use them if needed.

    .. _datetime.interval: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-interval-type
    """

    def __init__(self, *, year=0, month=0, week=0,
                 day=0, hour=0, minute=0, sec=0,
                 nsec=0, adjust=Adjust.NONE):
        """
        :param year: Interval year value.
        :type year: :obj:`int`, optional

        :param month: Interval month value.
        :type month: :obj:`int`, optional

        :param week: Interval week value.
        :type week: :obj:`int`, optional

        :param day: Interval day value.
        :type day: :obj:`int`, optional

        :param hour: Interval hour value.
        :type hour: :obj:`int`, optional

        :param minute: Interval minute value.
        :type minute: :obj:`int`, optional

        :param sec: Interval seconds value.
        :type sec: :obj:`int`, optional

        :param nsec: Interval nanoseconds value.
        :type nsec: :obj:`int`, optional

        :param adjust: Interval adjustment rule. Refer to
            :meth:`~tarantool.Datetime.__add__`.
        :type adjust: :class:`~tarantool.IntervalAdjust`, optional
        """
        
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
        """
        Valid operations:

        * :class:`~tarantool.Interval` + :class:`~tarantool.Interval`
          = :class:`~tarantool.Interval`

        Adjust of the first operand is used in result.

        :param other: Second operand.
        :type other: :class:`~tarantool.Interval`

        :rtype: :class:`~tarantool.Interval`

        :raise: :exc:`TypeError`
        """

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
        """
        Valid operations:

        * :class:`~tarantool.Interval` - :class:`~tarantool.Interval`
          = :class:`~tarantool.Interval`

        Adjust of the first operand is used in result.

        :param other: Second operand.
        :type other: :class:`~tarantool.Interval`

        :rtype: :class:`~tarantool.Interval`

        :raise: :exc:`TypeError`
        """

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
        """
        Compare equality of each field, no casts.

        :param other: Second operand.
        :type other: :class:`~tarantool.Interval`

        :rtype: :obj:`bool`
        """

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
