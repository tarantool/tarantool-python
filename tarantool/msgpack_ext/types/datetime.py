"""
Tarantool `datetime`_ extension type implementation module.
"""
# pylint: disable=line-too-long

from calendar import monthrange
from copy import deepcopy
from datetime import datetime, timedelta
import sys

import pytz

import tarantool.msgpack_ext.types.timezones as tt_timezones

from tarantool.msgpack_ext.types.interval import Interval, Adjust

NSEC_IN_SEC = 1000000000
NSEC_IN_MKSEC = 1000
SEC_IN_MIN = 60
MONTH_IN_YEAR = 12
_EPOCH = datetime(1970, 1, 1, tzinfo=pytz.utc)


def compute_offset(_datetime):
    """
    Compute timezone offset. Offset is computed each time and not stored
    since it could depend on current datetime value. It is expected that
    timestamp offset is not ``None``.

    :param _datetime: Datetime date.
    :type _datetime: :class:`datetime.datetime`

    :return: Timezone offset, in minutes.
    :rtype: :obj:`int`

    :meta private:
    """

    utc_offset = _datetime.tzinfo.utcoffset(_datetime)

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


def month_last_day(year, month):
    """
    Get the number of the last day in month.

    :param year: Calendar year.
    :type year: :obj:`int`

    :param month: Calendar month.
    :type month: :obj:`int`

    :rtype: :obj:`int`

    :meta private:
    """

    return monthrange(year, month)[1]


class Datetime():
    """
    Class representing Tarantool `datetime`_ info. Internals are based
    on :class:`datetime.datetime`.

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
            :class:`datetime.datetime` ``year`` parameter.
            Must be provided unless the object is built with
            :paramref:`~tarantool.Datetime.params.data` or
            :paramref:`~tarantool.Datetime.params.timestamp`.
        :type year: :obj:`int`, optional

        :param month: Datetime month value. Must be a valid
            :class:`datetime.datetime` ``month`` parameter.
            Must be provided unless the object is built with
            :paramref:`~tarantool.Datetime.params.data` or
            :paramref:`~tarantool.Datetime.params.timestamp`.
        :type month: :obj:`int`, optional

        :param day: Datetime day value. Must be a valid
            :class:`datetime.datetime` ``day`` parameter.
            Must be provided unless the object is built with
            :paramref:`~tarantool.Datetime.params.data` or
            :paramref:`~tarantool.Datetime.params.timestamp`.
        :type day: :obj:`int`, optional

        :param hour: Datetime hour value. Must be a valid
            :class:`datetime.datetime` ``hour`` parameter.
        :type hour: :obj:`int`, optional

        :param minute: Datetime minute value. Must be a valid
            :class:`datetime.datetime` ``minute`` parameter.
        :type minute: :obj:`int`, optional

        :param sec: Datetime seconds value. Must be a valid
            :class:`datetime.datetime` ``second`` parameter.
        :type sec: :obj:`int`, optional

        :param nsec: Datetime nanoseconds value.
        :type sec: :obj:`int`, optional

        :param tzoffset: Timezone offset. Ignored, if provided together
            with :paramref:`~tarantool.Datetime.params.tz`.
        :type tzoffset: :obj:`int`, optional

        :param tz: Timezone name from Olson timezone database.
        :type tz: :obj:`str`, optional

        :param timestamp_since_utc_epoch: Parameter to set timestamp
            conversion behavior for timezone-aware datetimes.

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
            conversion. In that case,
            :attr:`~tarantool.Datetime.timestamp` won't be equal to
            initialization
            :paramref:`~tarantool.Datetime.params.timestamp` for all
            timezones with non-zero offset.

            If ``True``, behaves similar to :class:`datetime.datetime`:

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
            :class:`datetime.datetime` exceptions

        .. _datetime.new(): https://www.tarantool.io/en/doc/latest/reference/reference_lua/datetime/new/
        """
        # pylint: disable=too-many-branches,too-many-locals,too-many-statements

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
            # pylint: disable=too-many-boolean-expressions
            if ((year is not None) or (month is not None)
                    or (day is not None) or (hour is not None)
                    or (minute is not None) or (sec is not None)):
                raise ValueError('Cannot provide both timestamp and year, month, '
                                 'day, hour, minute, sec')

            if nsec is not None:
                if not isinstance(timestamp, int):
                    raise ValueError('timestamp must be int if nsec provided')

                # Tarantool may send negative nanoseconds or nanoseconds bigger
                # than 999999999. datetime.datetime doesn't process overflows.
                if (nsec >= NSEC_IN_SEC) or (nsec < 0):
                    timestamp += nsec // NSEC_IN_SEC
                    nsec = nsec % NSEC_IN_SEC

            if (sys.platform.startswith("win")) and (timestamp < 0):
                # Fails to create a datetime from negative timestamp on Windows.
                _datetime = _EPOCH + timedelta(seconds=timestamp)
            else:
                # Timezone-naive datetime objects are treated by many datetime methods
                # as local times, so we represent time in UTC explicitly if not provided.
                _datetime = datetime.fromtimestamp(timestamp, pytz.UTC)

            if nsec is not None:
                _datetime = _datetime.replace(microsecond=nsec // NSEC_IN_MKSEC)
                _datetime_nsec = nsec % NSEC_IN_MKSEC
            else:
                _datetime_nsec = 0

            if tzinfo is not None:
                if not timestamp_since_utc_epoch:
                    # It seems that there is no way to get expected behavior without
                    # this hack. Localizing a timezone-naive datetime built
                    # from the timestamp fails since it uses local timezone to mess up
                    # the underlying timestamp. On the other hand, you cannot localize
                    # a timezone-aware datetime, even UTC one. Replaces don't work since
                    # they are broken for pytz + datetime, see
                    # https://pythonhosted.org/pytz/
                    _datetime = datetime.combine(_datetime.date(), _datetime.time())
                    _datetime = tzinfo.localize(_datetime)
                else:
                    _datetime = _datetime.astimezone(tzinfo)

            self._datetime = _datetime
            self._datetime_nsec = _datetime_nsec
        else:
            # datetime does not support None as defaults,
            # we support them for backward compatibility.
            if hour is None:
                hour = 0

            if minute is None:
                minute = 0

            if sec is None:
                sec = 0

            overflow = None
            if nsec is None:
                nsec = 0
            else:
                # Tarantool may send negative nanoseconds or nanoseconds bigger
                # than 999999999. datetime.datetime doesn't process overflows.
                if (nsec >= NSEC_IN_SEC) or (nsec < 0):
                    overflow = timedelta(seconds=nsec // NSEC_IN_SEC)
                    nsec = nsec % NSEC_IN_SEC

            _datetime = datetime(year=year, month=month, day=day,
                                 hour=hour, minute=minute, second=sec,
                                 microsecond=nsec // NSEC_IN_MKSEC)
            if overflow is not None:
                _datetime = _datetime + overflow
            # tzinfo as argument on the datetime not works as expected, see
            # https://pythonhosted.org/pytz/
            # Timezone-naive datetime objects are treated by many datetime methods
            # as local times, so we represent time in UTC explicitly if not provided.
            if tzinfo is None:
                tzinfo = pytz.UTC
            self._datetime = tzinfo.localize(_datetime)
            self._datetime_nsec = nsec % NSEC_IN_MKSEC

    def _interval_operation(self, interval, sign=1):
        """
        Implementation of :class:`~tarantool.Interval` addition and
        subtraction.

        :param interval: Interval to add or subtract.
        :type interval: :class:`~tarantool.Interval`

        :param sign: Right operand multiplier: ``1`` for addition,
            ``-1`` for subtractiom.
        :type sign: :obj:`int`

        :rtype: :class:`~tarantool.Datetime`

        :meta private:
        """

        old_dt = self._datetime
        new_dt = old_dt

        new_year = old_dt.year + sign * interval.year
        new_month = old_dt.month + sign * interval.month
        if (new_month < 1) or (new_month - 1 > MONTH_IN_YEAR):
            new_year += (new_month - 1) // MONTH_IN_YEAR
            new_month = (new_month - 1) % MONTH_IN_YEAR + 1

        new_month_last_day = month_last_day(new_year, new_month)
        old_month_last_day = month_last_day(old_dt.year, old_dt.month)

        # https://github.com/tarantool/tarantool/wiki/Datetime-Internals#date-adjustions-and-leap-years
        if (interval.adjust == Adjust.NONE) and (new_month_last_day < new_dt.day):
            new_dt = new_dt.replace(year=new_year, month=new_month, day=new_month_last_day)
        elif (interval.adjust == Adjust.EXCESS) and (new_month_last_day < new_dt.day):
            new_dt = new_dt.replace(year=new_year, month=new_month, day=new_month_last_day) + \
                timedelta(days=new_dt.day - new_month_last_day)
        elif (interval.adjust == Adjust.LAST) and (old_dt.day == old_month_last_day):
            new_dt = new_dt.replace(year=new_year, month=new_month, day=new_month_last_day)
        else:
            new_dt = new_dt.replace(year=new_year, month=new_month)

        nsec = self._datetime_nsec + sign * interval.nsec
        new_dt = new_dt + timedelta(weeks=sign * interval.week,
                                    days=sign * interval.day,
                                    hours=sign * interval.hour,
                                    minutes=sign * interval.minute,
                                    seconds=sign * interval.sec,
                                    microseconds=nsec // NSEC_IN_MKSEC)
        new_nsec = nsec % NSEC_IN_MKSEC

        if new_dt.tzinfo is not None:
            tzoffset = compute_offset(new_dt)
        else:
            tzoffset = 0
        return Datetime(year=new_dt.year, month=new_dt.month, day=new_dt.day,
                        hour=new_dt.hour, minute=new_dt.minute, sec=new_dt.second,
                        nsec=new_nsec + new_dt.microsecond * NSEC_IN_MKSEC,
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
            raise TypeError("unsupported operand type(s) for +: "
                            f"'{type(self)}' and '{type(other)}'")

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

            if self_dt.tzinfo != other_dt.tzinfo:
                other_dt = other_dt.astimezone(self_dt.tzinfo)

            self_nsec = self_dt.microsecond * NSEC_IN_MKSEC + self._datetime_nsec
            other_nsec = other_dt.microsecond * NSEC_IN_MKSEC + other._datetime_nsec

            return Interval(
                year=self_dt.year - other_dt.year,
                month=self_dt.month - other_dt.month,
                day=self_dt.day - other_dt.day,
                hour=self_dt.hour - other_dt.hour,
                minute=self_dt.minute - other_dt.minute,
                sec=self_dt.second - other_dt.second,
                nsec=self_nsec - other_nsec,
            )
        if isinstance(other, Interval):
            return self._interval_operation(other, sign=-1)

        raise TypeError(f"unsupported operand type(s) for -: '{type(self)}' and '{type(other)}'")

    def __eq__(self, other):
        """
        Datetimes are equal when underlying datetime infos are equal.

        :param other: Second operand.
        :type other: :class:`~tarantool.Datetime`

        :rtype: :obj:`bool`
        """

        if isinstance(other, Datetime):
            return self.value == other.value
        return False

    def __str__(self):
        # Based on pandas.Timestamp isofomat for backward compatibility.
        # https://github.com/pandas-dev/pandas/blob/249d93e4abc59639983eb3e8fccac8382592d457/pandas/_libs/tslibs/timestamps.pyx#L1015-L1034
        base = self._datetime.isoformat(sep='T', timespec='auto')

        # Preserve explicit UTC and implicit UTC difference for backward compatibility.
        implicit_utc = False
        if (self._datetime.tzinfo == pytz.UTC) and (self._tz == ''):
            implicit_utc = True
            base = base[:-6]

        if self._datetime_nsec == 0:
            return base

        if implicit_utc:
            base1, base2 = base, ""
        else:
            base1, base2 = base[:-6], base[-6:]

        if self._datetime.microsecond:
            base1 += f"{self._datetime_nsec:03d}"
        else:
            base1 += f".{self._datetime_nsec:09d}"

        return base1 + base2

    def __repr__(self):
        return f'datetime: {self._datetime.__repr__()}, nsec: {self._datetime_nsec}, ' + \
               f'tz: "{self.tz}"'

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for key, val in self.__dict__.items():
            setattr(result, key, deepcopy(val, memo))
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

        return self._datetime.microsecond * NSEC_IN_MKSEC + self._datetime_nsec

    @property
    def timestamp(self):
        """
        Datetime time since epoch, in seconds.

        :rtype: :obj:`float`
        """

        return self._datetime.timestamp() + self._datetime_nsec / NSEC_IN_SEC

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

        # Python sources way to get ineteger time since epoch.
        # https://github.com/python/cpython/blob/a6f95941a3d686707fb38e0f37758e666f25e180/Lib/datetime.py#L1879
        seconds = (self._datetime - _EPOCH) // timedelta(0, 1)
        return seconds * NSEC_IN_SEC + self.nsec
