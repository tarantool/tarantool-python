# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- Reusable testing workflow for integration with tarantool artifacts
  (PR #192).

### Changed
- **Breaking**: drop Python 2 support (PR #207).
- **Breaking**: change binary types encode/decode for Python 3
  to support working with varbinary (PR #211, #105).
  With Python 2 the behavior of the connector remains the same.

  Before this patch:

  * encoding="utf-8" (default)

    | Python 3 | -> | Tarantool          | -> | Python 3 |
    |----------|----|--------------------|----|----------|
    | str      | -> | mp_str (string)    | -> | str      |
    | bytes    | -> | mp_str (string)    | -> | str      |
    |          |    | mp_bin (varbinary) | -> | bytes    |

  * encoding=None

    | Python 3 | -> | Tarantool          | -> | Python 3 |
    |----------|----|--------------------|----|----------|
    | bytes    | -> | mp_str (string)    | -> | bytes    |
    | str      | -> | mp_str (string)    | -> | bytes    |
    |          |    | mp_bin (varbinary) | -> | bytes    |

  Using bytes as key was not supported by several methods (delete,
  update, select).

  After this patch:

  * encoding="utf-8" (default)

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

  Using bytes as key are now supported by all methods.

  Thus, encoding="utf-8" connection may be used to work with
  utf-8 strings and varbinary and encodine=None connection
  may be used to work with non-utf-8 strings.

- Clarify license of the project (BSD-2-Clause) (PR #210, #197).
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
- SQL support (<connection>.execute() method) (#159, PR #161).
- Allow to receive a Tarantool tuple as a Python tuple, not a list, with
    use_list=False connection option (#166, PR #161).
- Support the Database API (PEP-0249) (PR #161).

### Changed
- Various improvements in README (PR #147, PR #151, PR #180).

### Fixed
- Support encoding=None connections (PR #172).
- Various improvements and fixes in tests (8ff9a3f, bd37703, PR #165,
  #178, PR #179, PR #181).