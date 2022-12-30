# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Fixed

## 0.11.0 - 2022-12-31

### Added
- Support custom packer and unpacker factories (#191).
- Support [crud module](https://github.com/tarantool/crud) native API (#205).
- Support `ssl_password` and `ssl_password_file` options
  to decrypt private SSL key file (#224).
- Support specifying authentication method with `auth_type`
  and Tarantool EE `pap-sha256` authentication method (#269).

## 0.10.0 - 2022-11-09

### Added
- Decimal type support (#203).
- UUID type support (#202).
- Datetime type support and tarantool.Datetime type (#204).

  Tarantool datetime objects are decoded to `tarantool.Datetime`
  type. `tarantool.Datetime` may be encoded to Tarantool datetime
  objects.

  You can create `tarantool.Datetime` objects either from
  MessagePack data or by using the same API as in Tarantool:

  ```python
  dt1 = tarantool.Datetime(year=2022, month=8, day=31,
                           hour=18, minute=7, sec=54,
                           nsec=308543321)

  dt2 = tarantool.Datetime(timestamp=1661969274)

  dt3 = tarantool.Datetime(timestamp=1661969274, nsec=308543321)
  ```

  `tarantool.Datetime` exposes `year`, `month`, `day`, `hour`,
  `minute`, `sec`, `nsec`, `timestamp` and `value` (integer epoch time
  with nanoseconds precision) properties if you need to convert
  `tarantool.Datetime` to any other kind of datetime object:

  ```python
  pdt = pandas.Timestamp(year=dt.year, month=dt.month, day=dt.day,
                         hour=dt.hour, minute=dt.minute, second=dt.sec,
                         microsecond=(dt.nsec // 1000),
                         nanosecond=(dt.nsec % 1000))
  ```

- Offset in datetime type support (#204).

  Use `tzoffset` parameter to set up offset timezone:

  ```python
  dt = tarantool.Datetime(year=2022, month=8, day=31,
                          hour=18, minute=7, sec=54,
                          nsec=308543321, tzoffset=180)
  ```

  You may use `tzoffset` property to get timezone offset of a datetime
  object.

- Timezone in datetime type support (#204).

  Use `tz` parameter to set up timezone name:

  ```python
  dt = tarantool.Datetime(year=2022, month=8, day=31,
                          hour=18, minute=7, sec=54,
                          nsec=308543321, tz='Europe/Moscow')
  ```

  If both `tz` and `tzoffset` is specified, `tz` is used.

  You may use `tz` property to get timezone name of a datetime object.

- Datetime interval type support and tarantool.Interval type (#229).

  Tarantool datetime interval objects are decoded to `tarantool.Interval`
  type. `tarantool.Interval` may be encoded to Tarantool interval
  objects.

  You can create `tarantool.Interval` objects either from
  MessagePack data or by using the same API as in Tarantool:

  ```python
  di = tarantool.Interval(year=-1, month=2, day=3,
                          hour=4, minute=-5, sec=6,
                          nsec=308543321,
                          adjust=tarantool.IntervalAdjust.NONE)
  ```

  Its attributes (same as in init API) are exposed, so you can
  use them if needed.

- Datetime interval arithmetic support (#229).

  Valid operations:
  - `tarantool.Datetime` + `tarantool.Interval` = `tarantool.Datetime`
  - `tarantool.Datetime` - `tarantool.Interval` = `tarantool.Datetime`
  - `tarantool.Datetime` - `tarantool.Datetime` = `tarantool.Interval`
  - `tarantool.Interval` + `tarantool.Interval` = `tarantool.Interval`
  - `tarantool.Interval` - `tarantool.Interval` = `tarantool.Interval`

  Since `tarantool.Interval` could contain `month` and `year` fields
  and such operations could be ambiguous, you can use `adjust` field
  to tune the logic. The behavior is the same as in Tarantool, see
  [Interval arithmetic RFC](https://github.com/tarantool/tarantool/wiki/Datetime-Internals#interval-arithmetic).

  - `tarantool.IntervalAdjust.NONE` -- only truncation toward the end of
    month performed (default mode).

    ```python
    >>> dt = tarantool.Datetime(year=2022, month=3, day=31)
    datetime: Timestamp('2022-03-31 00:00:00'), tz: ""
    >>> di = tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.NONE)
    >>> dt + di
    datetime: Timestamp('2022-04-30 00:00:00'), tz: ""
    ```

  - `tarantool.IntervalAdjust.EXCESS` -- overflow mode, without any snap
    or truncation to the end of month, straight addition of days in month,
    stopping over month boundaries if there is less number of days.

    ```python
    >>> dt = tarantool.Datetime(year=2022, month=1, day=31)
    datetime: Timestamp('2022-01-31 00:00:00'), tz: ""
    >>> di = tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.EXCESS)
    >>> dt + di
    datetime: Timestamp('2022-03-02 00:00:00'), tz: ""
    ```

  - `tarantool.IntervalAdjust.LAST` -- mode when day snaps to the end of month,
    if happens.

    ```python
    >>> dt = tarantool.Datetime(year=2022, month=2, day=28)
    datetime: Timestamp('2022-02-28 00:00:00'), tz: ""
    >>> di = tarantool.Interval(month=1, adjust=tarantool.IntervalAdjust.LAST)
    >>> dt + di
    datetime: Timestamp('2022-03-31 00:00:00'), tz: ""
    ```

- Support iproto feature discovery (#206).
- Backport ConnectionPool support for Python 3.6.
- Support extra information for iproto errors (#232).
- Error extension type support (#232).

- Support pandas way to build datetime from timestamp (PR #252).

  `timestamp_since_utc_epoch` is a parameter to set timestamp
  convertion behavior for timezone-aware datetimes.

  If ``False`` (default), behaves similar to Tarantool `datetime.new()`:

  ```python
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
  ```

  Thus, if ``False``, datetime is computed from timestamp
  since epoch and then timezone is applied without any
  convertion. In that case, `dt.timestamp` won't be equal to
  initialization `timestamp` for all timezones with non-zero offset.

  If ``True``, behaves similar to `pandas.Timestamp`:

  ```python
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
  ```

  Thus, if ``True``, datetime is computed in a way that `dt.timestamp` will
  always be equal to initialization `timestamp`.

- Support iproto feature push (#201).
- Pack pip package with GitHub Actions (#198).
- Publish pip package with GitHub Actions (#198).
- Pack RPM package with GitHub Actions (#164, #198).
- Publish RPM package with GitHub Actions (#164, #198).
- Pack deb package with GitHub Actions (#198).
- Publish deb package with GitHub Actions (#198).

### Changed
- Bump msgpack requirement to 1.0.4 (PR #223).
  The only reason of this bump is various vulnerability fixes,
  msgpack>=0.4.0 and msgpack-python==0.4.0 are still supported.
- Change documentation HTML theme (#67).
- Update API documentation strings (#67).
- Update documentation index, quick start and guide pages (#67).
- Use git version to set package version (#238).
- Extract tarantool.Datetime encode and decode to external
  functions (PR #252).
- Extract tarantool.Interval encode and decode to external
  functions (PR #252).
- Do not enforce msgpack version (#198).

### Fixed
- Package build (#238).
- Allow any MessagePack supported type as a request key (#240).
- Putting test files in pip package (#238).
- Make connection close idempotent (#250).
- readthedocs version (#255).
- timezone offset with old pytz and pandas (#198).

## 0.9.0 - 2022-06-20

### Added
- SSL support (PR #220, #217).
- Tarantool Enterprise testing workflow on GitHub actions (PR #220).

## 0.8.0 - 2022-04-29

### Added
- Reusable testing workflow for integration with tarantool artifacts
  (PR #192).
- Connection pool with master discovery (PR #207, #196).

  ConnectionPool is supported only for Python 3.7 or newer.
  Authenticated user must be able to call `box.info` on instances.
  For example, to give grants to `'guest'` user, evaluate
  ```lua
  box.schema.func.create('box.info')
  box.schema.user.grant('guest', 'execute', 'function', 'box.info')
  ```
  on Tarantool instances.

  ConnectionPool updates information about each server state (RO/RW)
  on initial connect and then asynchronously in separate threads.
  Application retries must be written considering the asynchronous nature
  of cluster state refresh. The user does not need to use any synchronization
  mechanisms in requests, it's all handled with ConnectionPool methods.

  ConnectionPool API is the same as the plain Connection API.
  On each request, a connection is chosen to execute the request.
  A connection is chosen based on the request mode:
  * `Mode.ANY` chooses any instance.
  * `Mode.RW` chooses an RW instance.
  * `Mode.RO` chooses an RO instance.
  * `Mode.PREFER_RW` chooses an RW instance, if possible, an RO instance
    otherwise.
  * `Mode.PREFER_RO` chooses an RO instance, if possible, an RW instance
    otherwise.
  All requests that guarantee to write data (insert, replace, delete,
  upsert, update) use the RW mode by default.
  The select request uses `ANY` by default. You
  can set the mode explicitly. The call, eval, execute, and ping requests
  require to set the mode explicitly.

  Example:
  ```python
  pool = tarantool.ConnectionPool(
      addrs=[
          {'host': '108.177.16.0', 'port': 3301},
          {'host': '108.177.16.0', 'port': 3302},
      ],
      user='test',
      password='test',)

  pool.call('some_write_procedure', arg, mode=tarantool.Mode.RW)
  ```

### Changed
- **Breaking change**: Python 2 support dropped (PR #207).
- **Breaking change**: `encode`/`decode` binary types for Python 3 changed
  to support working with `varbinary` (PR #211, #105).
  With Python 2, the behavior of the connector remains the same.

  Before this patch:

  * `encoding="utf-8"` (default)

    | Python 3 | -> | Tarantool          | -> | Python 3 |
    |----------|----|--------------------|----|----------|
    | str      | -> | mp_str (string)    | -> | str      |
    | bytes    | -> | mp_str (string)    | -> | str      |
    |          |    | mp_bin (varbinary) | -> | bytes    |

  * `encoding=None`

    | Python 3 | -> | Tarantool          | -> | Python 3 |
    |----------|----|--------------------|----|----------|
    | bytes    | -> | mp_str (string)    | -> | bytes    |
    | str      | -> | mp_str (string)    | -> | bytes    |
    |          |    | mp_bin (varbinary) | -> | bytes    |

  Several method (delete, update, select) did not support
  using `bytes` as key.

  After this patch:

  * `encoding="utf-8"` (default)

    | Python 3 | -> | Tarantool          | -> | Python 3 |
    |----------|----|--------------------|----|----------|
    | str      | -> | mp_str (string)    | -> | str      |
    | bytes    | -> | mp_bin (varbinary) | -> | bytes    |

  * encoding=None

    | Python 3 | -> | Tarantool          | -> | Python 3 |
    |----------|----|--------------------|----|----------|
    | bytes    | -> | mp_str (string)    | -> | bytes    |
    | str      | -> | mp_str (string)    | -> | bytes    |
    |          |    | mp_bin (varbinary) | -> | bytes    |

  All methods now support using `bytes` as key.

  Thus, an `encoding="utf-8"` connection may be used to work with
  UTF-8 strings and `varbinary`, and an `encoding=None` connection
  may be used to work with non-UTF-8 strings.

- Clarify the license of the project (BSD-2-Clause) (PR #210, #197).
- Migrate CI to GitHub Actions (PR #213, PR #216, #182).
- Various improvements and fixes in README (PR #210, PR #215).

### Fixed
- json.dumps compatibility with Python 2 (PR #186). 
- Unix socket support in mesh_connection (PR #189, #111).
- Various fixes in tests (PR #189, #111, PR #195, #194).


## 0.7.1 - 2020-12-28

### Fixed
- msgpack library dependency (PR #185).


## 0.7.0 - 2020-12-28

Caution: Use tarantool-python 0.7.1 instead of 0.7.0. It fixes
the dependency on the msgpack library.

### Added
- Support msgpack 1.0.0 (#155, PR #173).
- SQL support (the method `<connection>.execute()`) (#159, PR #161).
- Allow receiving a Tarantool tuple as a Python tuple, not a list, with
    the `use_list=False` connection option (#166, PR #161).
- Support the Database API (PEP-0249) (PR #161).

### Changed
- Various improvements in README (PR #147, PR #151, PR #180).

### Fixed
- Support `encoding=None` connections (PR #172).
- Various improvements and fixes in tests (8ff9a3f, bd37703, PR #165,
  #178, PR #179, PR #181).
