"""
Tarantool `datetime`_ extension type implementation module.
"""

from copy import deepcopy

import pandas
import pytz

import tarantool.msgpack_ext.types.timezones as tt_timezones

from tarantool.msgpack_ext.types.interval import Interval, Adjust

NSEC_IN_SEC = 1000000000
NSEC_IN_MKSEC = 1000
SEC_IN_MIN = 60
MONTH_IN_YEAR = 12

def compute_offset(timestamp):
    """
    Compute timezone offset. Offset is computed each time and not stored
    since it could depend on current datetime value. It is expected that
    timestamp offset is not ``None``.

    :param timestamp: Timestamp data.
    :type timestamp: :class:`pandas.Timestamp`

    :return: Timezone offset, in minutes.
    :rtype: :obj:`int`

    :meta private:
    """

    utc_offset = timestamp.tzinfo.utcoffset(timestamp)

    # `None` offset is a valid utcoffset implementation,
    # but it seems that pytz timezones never return `None`:
    # https://github.com/pandas-dev/pandas/issues/15986
    assert utc_offset is not None

    # There is no precision loss since offset is in minutes
    return int(utc_offset.total_seconds()) // SEC_IN_MIN

def get_python_tzinfo(tz):
    """
    All non-abbreviated Tarantool timezones are represented as pytz
    timezones (from :func:`pytz.timezone`). All non-ambiguous
    abbreviated Tarantool timezones are represented as
    :class:`pytz.FixedOffset` timezones. Attempt to build timezone
    info for ambiguous timezone results in raising the exception, same
    as in Tarantool.

    :param tz: Tarantool timezone name.
    :type tz: :obj:`str`

    :return: Timezone object.
    :rtype: :func:`pytz.timezone` result or :class:`pytz.FixedOffset`

    :raise: :exc:`~tarantool.msgpack_ext.types.datetime.get_python_tzinfo.params.error_class`

    :meta private:
    """

    if tz in pytz.all_timezones:
        return pytz.timezone(tz)

    # Checked with timezones/validate_timezones.py
    tt_tzinfo = tt_timezones.timezoneAbbrevInfo[tz]
    if (tt_tzinfo['category'] & tt_timezones.TZ_AMBIGUOUS) != 0:
        raise ValueError(f'Failed to create datetime with ambiguous timezone "{tz}"')

    return pytz.FixedOffset(tt_tzinfo['offset'])

class Datetime():
    """
    Class representing Tarantool `datetime`_ info. Internals are based
    on :class:`pandas.Timestamp`.

    You can create :class:`~tarantool.Datetime` objects by using the
    same API as in Tarantool:

    .. code-block:: python

        dt1 = tarantool.Datetime(year=2022, month=8, day=31,
                                 hour=18, minute=7, sec=54,
                                 nsec=308543321)

        dt2 = tarantool.Datetime(timestamp=1661969274)

        dt3 = tarantool.Datetime(timestamp=1661969274, nsec=308543321)

    :class:`~tarantool.Datetime` exposes
    :attr:`~tarantool.Datetime.year`,
    :attr:`~tarantool.Datetime.month`,
    :attr:`~tarantool.Datetime.day`,
    :attr:`~tarantool.Datetime.hour`,
    :attr:`~tarantool.Datetime.minute`,
    :attr:`~tarantool.Datetime.sec`,
    :attr:`~tarantool.Datetime.nsec`,
    :attr:`~tarantool.Datetime.timestamp` and
    :attr:`~tarantool.Datetime.value` (integer epoch time with
    nanoseconds precision) properties if you need to convert
    :class:`~tarantool.Datetime` to any other kind of datetime object:

    .. code-block:: python

        pdt = pandas.Timestamp(year=dt.year, month=dt.month, day=dt.day,
                               hour=dt.hour, minute=dt.minute, second=dt.sec,
                               microsecond=(dt.nsec // 1000),
                               nanosecond=(dt.nsec % 1000))

    Use :paramref:`~tarantool.Datetime.params.tzoffset` parameter to set
    up offset timezone:

    .. code-block:: python

        dt = tarantool.Datetime(year=2022, month=8, day=31,
                                hour=18, minute=7, sec=54,
                                nsec=308543321, tzoffset=180)

    You may use the :attr:`~tarantool.Datetime.tzoffset` property to
    get the timezone offset of a datetime object.

    Use :paramref:`~tarantool.Datetime.params.tz` parameter to set up
    timezone name:

    .. code-block:: python

        dt = tarantool.Datetime(year=2022, month=8, day=31,
                                hour=18, minute=7, sec=54,
                                nsec=308543321, tz='Europe/Moscow')

    If both :paramref:`~tarantool.Datetime.params.tz` and
    :paramref:`~tarantool.Datetime.params.tzoffset` are specified,
    :paramref:`~tarantool.Datetime.params.tz` is used.

    You may use the :attr:`~tarantool.Datetime.tz` property to get
    the timezone name of a datetime object.

    .. _datetime: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-datetime-type
    """

    def __init__(self, *, timestamp=None, year=None, month=None,
                 day=None, hour=None, minute=None, sec=None, nsec=None,
                 tzoffset=0, tz='', timestamp_since_utc_epoch=False):
        """
        :param timestamp: Timestamp since epoch. Cannot be provided
            together with
            :paramref:`~tarantool.Datetime.params.year`,
            :paramref:`~tarantool.Datetime.params.month`,
            :paramref:`~tarantool.Datetime.params.day`,
            :paramref:`~tarantool.Datetime.params.hour`,
            :paramref:`~tarantool.Datetime.params.minute`,
            :paramref:`~tarantool.Datetime.params.sec`.
            If :paramref:`~tarantool.Datetime.params.nsec` is provided,
            it must be :obj:`int`. Refer to
            :paramref:`~tarantool.Datetime.params.timestamp_since_utc_epoch`
            to clarify how timezone-aware datetime is computed from
            the timestamp.
        :type timestamp: :obj:`float` or :obj:`int`, optional

        :param year: Datetime year value. Must be a valid
            :class:`pandas.Timestamp` ``year`` parameter.
            Must be provided unless the object is built with
            :paramref:`~tarantool.Datetime.params.data` or
            :paramref:`~tarantool.Datetime.params.timestamp`.
        :type year: :obj:`int`, optional

        :param month: Datetime month value. Must be a valid
            :class:`pandas.Timestamp` ``month`` parameter.
            Must be provided unless the object is built with
            :paramref:`~tarantool.Datetime.params.data` or
            :paramref:`~tarantool.Datetime.params.timestamp`.
        :type month: :obj:`int`, optional

        :param day: Datetime day value. Must be a valid
            :class:`pandas.Timestamp` ``day`` parameter.
            Must be provided unless the object is built with
            :paramref:`~tarantool.Datetime.params.data` or
            :paramref:`~tarantool.Datetime.params.timestamp`.
        :type day: :obj:`int`, optional

        :param hour: Datetime hour value. Must be a valid
            :class:`pandas.Timestamp` ``hour`` parameter.
        :type hour: :obj:`int`, optional

        :param minute: Datetime minute value. Must be a valid
            :class:`pandas.Timestamp` ``minute`` parameter.
        :type minute: :obj:`int`, optional

        :param sec: Datetime seconds value. Must be a valid
            :class:`pandas.Timestamp` ``second`` parameter.
        :type sec: :obj:`int`, optional

        :param nsec: Datetime nanoseconds value. Quotient of a division
            by 1000 (nanoseconds in microseconds) must be a valid
            :class:`pandas.Timestamp` ``microsecond`` parameter,
            remainder of a division by 1000 must be a valid
            :class:`pandas.Timestamp` ``nanosecond`` parameter.
        :type sec: :obj:`int`, optional

        :param tzoffset: Timezone offset. Ignored, if provided together
            with :paramref:`~tarantool.Datetime.params.tz`.
        :type tzoffset: :obj:`int`, optional

        :param tz: Timezone name from Olson timezone database.
        :type tz: :obj:`str`, optional

        :param timestamp_since_utc_epoch: Parameter to set timestamp
            convertion behavior for timezone-aware datetimes.

            If ``False`` (default), behaves similar to Tarantool
            `datetime.new()`_:

            .. code-block:: python

                >>> dt = tarantool.Datetime(timestamp=1640995200, timestamp_since_utc_epoch=False)
                >>> dt
                datetime: Timestamp('2022-01-01 00:00:00'), tz: ""
                >>> dt.timestamp
                1640995200.0
                >>> dt = tarantool.Datetime(timestamp=1640995200, tz='Europe/Moscow',
                ...                         timestamp_since_utc_epoch=False)
                >>> dt
                datetime: Timestamp('2022-01-01 00:00:00+0300', tz='Europe/Moscow'), tz: "Europe/Moscow"
                >>> dt.timestamp
                1640984400.0

            Thus, if ``False``, datetime is computed from timestamp
            since epoch and then timezone is applied without any
            convertion. In that case,
            :attr:`~tarantool.Datetime.timestamp` won't be equal to
            initialization
            :paramref:`~tarantool.Datetime.params.timestamp` for all
            timezones with non-zero offset.

            If ``True``, behaves similar to :class:`pandas.Timestamp`:

            .. code-block:: python

                >>> dt = tarantool.Datetime(timestamp=1640995200, timestamp_since_utc_epoch=True)
                >>> dt
                datetime: Timestamp('2022-01-01 00:00:00'), tz: ""
                >>> dt.timestamp
                1640995200.0
                >>> dt = tarantool.Datetime(timestamp=1640995200, tz='Europe/Moscow',
                ...                         timestamp_since_utc_epoch=True)
                >>> dt
                datetime: Timestamp('2022-01-01 03:00:00+0300', tz='Europe/Moscow'), tz: "Europe/Moscow"
                >>> dt.timestamp
                1640995200.0

            Thus, if ``True``, datetime is computed in a way that
            :attr:`~tarantool.Datetime.timestamp` will always be equal
            to initialization
            :paramref:`~tarantool.Datetime.params.timestamp`.
        :type timestamp_since_utc_epoch: :obj:`bool`, optional

        :raise: :exc:`ValueError`, :exc:`~tarantool.error.MsgpackError`,
            :class:`pandas.Timestamp` exceptions

        .. _datetime.new(): https://www.tarantool.io/en/doc/latest/reference/reference_lua/datetime/new/
        """

        tzinfo = None
        if tz != '':
            if tz not in tt_timezones.timezoneToIndex:
                raise ValueError(f'Unknown Tarantool timezone "{tz}"')

            tzinfo = get_python_tzinfo(tz)
        elif tzoffset != 0:
            tzinfo = pytz.FixedOffset(tzoffset)
        self._tz = tz

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

            if not timestamp_since_utc_epoch:
                self._datetime = datetime.tz_localize(tzinfo)
            else:
                self._datetime = datetime.tz_localize(pytz.UTC).tz_convert(tzinfo)
        else:
            if nsec is not None:
                microsecond = nsec // NSEC_IN_MKSEC
                nanosecond = nsec % NSEC_IN_MKSEC
            else:
                microsecond = 0
                nanosecond = 0

            self._datetime = pandas.Timestamp(
                year=year, month=month, day=day,
                hour=hour, minute=minute, second=sec,
                microsecond=microsecond,
                nanosecond=nanosecond).tz_localize(tzinfo)

    def _interval_operation(self, other, sign=1):
        """
        Implementation of :class:`~tarantool.Interval` addition and
        subtraction.

        :param other: Interval to add or subtract.
        :type other: :class:`~tarantool.Interval`

        :param sign: Right operand multiplier: ``1`` for addition,
            ``-1`` for subtractiom.
        :type sign: :obj:`int`

        :rtype: :class:`~tarantool.Datetime`

        :meta private:
        """

        self_dt = self._datetime

        # https://github.com/tarantool/tarantool/wiki/Datetime-Internals#date-adjustions-and-leap-years
        months = other.year * MONTH_IN_YEAR + other.month

        res = self_dt + pandas.DateOffset(months = sign * months)

        # pandas.DateOffset works exactly like Adjust.NONE
        if other.adjust == Adjust.EXCESS:
            if self_dt.day > res.day:
                res = res + pandas.DateOffset(days = self_dt.day - res.day)
        elif other.adjust == Adjust.LAST:
            if self_dt.is_month_end:
                # day replaces days
                res = res.replace(day = res.days_in_month)

        res = res + pandas.Timedelta(weeks = sign * other.week,
                                     days = sign * other.day,
                                     hours = sign * other.hour,
                                     minutes = sign * other.minute,
                                     seconds = sign * other.sec,
                                     microseconds = sign * (other.nsec // NSEC_IN_MKSEC),
                                     nanoseconds = sign * (other.nsec % NSEC_IN_MKSEC))

        if res.tzinfo is not None:
            tzoffset = compute_offset(res)
        else:
            tzoffset = 0
        return Datetime(year=res.year, month=res.month, day=res.day,
                        hour=res.hour, minute=res.minute, sec=res.second,
                        nsec=res.nanosecond + res.microsecond * NSEC_IN_MKSEC,
                        tzoffset=tzoffset, tz=self.tz)

    def __add__(self, other):
        """
        Valid operations:

        * :class:`~tarantool.Datetime` + :class:`~tarantool.Interval`
          = :class:`~tarantool.Datetime`

        Since :class:`~tarantool.Interval` could contain
        :paramref:`~tarantool.Interval.params.month` and
        :paramref:`~tarantool.Interval.params.year` fields and such
        operations could be ambiguous, you can use the
        :paramref:`~tarantool.Interval.params.adjust` field to tune the
        logic. The behavior is the same as in Tarantool, see
        `Interval arithmetic RFC`_.

        * :attr:`tarantool.IntervalAdjust.NONE <tarantool.msgpack_ext.types.interval.Adjust.NONE>`
          -- only truncation toward the end of month is performed (default
          mode).

          .. code-block:: python

              >>> dt = tarantool.Datetime(year=2022, month=3, day=31)
              datetime: Timestamp('2022-03-31 00:00:00'), tz: ""
              >>> di = tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.NONE)
              >>> dt + di
              datetime: Timestamp('2022-04-30 00:00:00'), tz: ""

        * :attr:`tarantool.IntervalAdjust.EXCESS <tarantool.msgpack_ext.types.interval.Adjust.EXCESS>`
          -- overflow mode,
          without any snap or truncation to the end of month, straight
          addition of days in month, stopping over month boundaries if
          there is less number of days.

          .. code-block:: python

              >>> dt = tarantool.Datetime(year=2022, month=1, day=31)
              datetime: Timestamp('2022-01-31 00:00:00'), tz: ""
              >>> di = tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.EXCESS)
              >>> dt + di
              datetime: Timestamp('2022-03-02 00:00:00'), tz: ""

        * :attr:`tarantool.IntervalAdjust.LAST <tarantool.msgpack_ext.types.interval.Adjust.LAST>`
          -- mode when day snaps to the end of month, if it happens.

          .. code-block:: python

              >>> dt = tarantool.Datetime(year=2022, month=2, day=28)
              datetime: Timestamp('2022-02-28 00:00:00'), tz: ""
              >>> di = tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.LAST)
              >>> dt + di
              datetime: Timestamp('2022-03-31 00:00:00'), tz: ""

        :param other: Second operand.
        :type other: :class:`~tarantool.Interval`

        :rtype: :class:`~tarantool.Datetime`

        :raise: :exc:`TypeError`

        .. _Interval arithmetic RFC: https://github.com/tarantool/tarantool/wiki/Datetime-Internals#interval-arithmetic
        """

        if not isinstance(other, Interval):
            raise TypeError(f"unsupported operand type(s) for +: '{type(self)}' and '{type(other)}'")

        return self._interval_operation(other, sign=1)

    def __sub__(self, other):
        """
        Valid operations:

        * :class:`~tarantool.Datetime` - :class:`~tarantool.Interval`
          = :class:`~tarantool.Datetime`
        * :class:`~tarantool.Datetime` - :class:`~tarantool.Datetime`
          = :class:`~tarantool.Interval`

        Refer to :meth:`~tarantool.Datetime.__add__` for interval
        adjustment rules.

        :param other: Second operand.
        :type other: :class:`~tarantool.Interval` or
            :class:`~tarantool.Datetime`

        :rtype: :class:`~tarantool.Datetime` or
            :class:`~tarantool.Interval`

        :raise: :exc:`TypeError`
        """

        if isinstance(other, Datetime):
            self_dt = self._datetime
            other_dt = other._datetime

            # Tarantool datetime subtraction ignores timezone info, but it is a bug:
            #
            # Tarantool 2.10.1-0-g482d91c66
            #
            # tarantool> datetime.new{tz='MSK'} - datetime.new{tz='UTC'}
            # ---
            # - +0 seconds
            # ...
            #
            # Refer to https://github.com/tarantool/tarantool/issues/7698
            # for possible updates.

            if self_dt.tzinfo != other_dt.tzinfo:
                other_dt = other_dt.tz_convert(self_dt.tzinfo)

            self_nsec = self_dt.microsecond * NSEC_IN_MKSEC + self_dt.nanosecond
            other_nsec = other_dt.microsecond * NSEC_IN_MKSEC + other_dt.nanosecond

            return Interval(
                year = self_dt.year - other_dt.year,
                month = self_dt.month - other_dt.month,
                day = self_dt.day - other_dt.day,
                hour = self_dt.hour - other_dt.hour,
                minute = self_dt.minute - other_dt.minute,
                sec = self_dt.second - other_dt.second,
                nsec = self_nsec - other_nsec,
            )
        elif isinstance(other, Interval):
            return self._interval_operation(other, sign=-1)
        else:
            raise TypeError(f"unsupported operand type(s) for -: '{type(self)}' and '{type(other)}'")

    def __eq__(self, other):
        """
        Datetimes are equal when underlying datetime infos are equal.

        :param other: Second operand.
        :type other: :class:`~tarantool.Datetime` or
            :class:`~pandas.Timestamp`

        :rtype: :obj:`bool`
        """

        if isinstance(other, Datetime):
            return self._datetime == other._datetime
        elif isinstance(other, pandas.Timestamp):
            return self._datetime == other
        else:
            return False

    def __str__(self):
        return self._datetime.__str__()

    def __repr__(self):
        return f'datetime: {self._datetime.__repr__()}, tz: "{self.tz}"'

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
        """
        Datetime year.

        :rtype: :obj:`int`
        """

        return self._datetime.year

    @property
    def month(self):
        """
        Datetime month.

        :rtype: :obj:`int`
        """

        return self._datetime.month

    @property
    def day(self):
        """
        Datetime day.

        :rtype: :obj:`int`
        """

        return self._datetime.day

    @property
    def hour(self):
        """
        Datetime day.

        :rtype: :obj:`int`
        """

        return self._datetime.hour

    @property
    def minute(self):
        """
        Datetime minute.

        :rtype: :obj:`int`
        """

        return self._datetime.minute

    @property
    def sec(self):
        """
        Datetime seconds.

        :rtype: :obj:`int`
        """

        return self._datetime.second

    @property
    def nsec(self):
        """
        Datetime nanoseconds (everything less than seconds is included).

        :rtype: :obj:`int`
        """

        return self._datetime.value % NSEC_IN_SEC

    @property
    def timestamp(self):
        """
        Datetime time since epoch, in seconds.

        :rtype: :obj:`float`
        """

        return self._datetime.timestamp()

    @property
    def tzoffset(self):
        """
        Datetime current timezone offset.

        :rtype: :obj:`int`
        """

        if self._datetime.tzinfo is not None:
            return compute_offset(self._datetime)
        return 0

    @property
    def tz(self):
        """
        Datetime timezone name.

        :rtype: :obj:`str`
        """

        return self._tz

    @property
    def value(self):
        """
        Datetime time since epoch, in nanoseconds.

        :rtype: :obj:`int`
        """

        return self._datetime.value
