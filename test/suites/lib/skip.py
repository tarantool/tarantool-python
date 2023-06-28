"""
This module provides helpers to skip specific tests.
"""

import functools
import sys

import pkg_resources


def fetch_tarantool_version(self):
    """
    Helper to fetch current Tarantool version.
    """
    if not hasattr(self, 'tnt_version') or self.tnt_version is None:
        srv = None

        if hasattr(self, 'servers') and self.servers is not None:
            srv = self.servers[0]

        if hasattr(self, 'srv') and self.srv is not None:
            srv = self.srv

        assert srv is not None

        try:
            self.tnt_version = srv.admin.tnt_version
        except AttributeError:
            self.__class__.tnt_version = srv.admin.tnt_version


def skip_or_run_test_tarantool_impl(self, required_tt_version, msg):
    """
    Helper to skip or run tests depending on the Tarantool
    version.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """
    fetch_tarantool_version(self)

    support_version = pkg_resources.parse_version(required_tt_version)

    if self.tnt_version < support_version:
        self.skipTest(f'Tarantool {self.tnt_version} {msg}')


def skip_or_run_test_tarantool(func, required_tt_version, msg):
    """
    Decorator to skip or run tests depending on the tarantool
    version.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ == 'setUp':
            func(self, *args, **kwargs)

        skip_or_run_test_tarantool_impl(self, required_tt_version, msg)

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper


def skip_or_run_test_tarantool_call(self, required_tt_version, msg):
    """
    Function to skip or run tests depending on the tarantool
    version. Useful in cases when in is inconvenient to work
    with decorators.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    skip_or_run_test_tarantool_impl(self, required_tt_version, msg)


def skip_or_run_test_pcall_require(func, required_tt_module, msg):
    """
    Decorator to skip or run tests depending on tarantool
    module require success or fail.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ == 'setUp':
            func(self, *args, **kwargs)

        srv = None

        if hasattr(self, 'servers') and self.servers:
            srv = self.servers[0]

        if hasattr(self, 'srv') and self.srv:
            srv = self.srv

        assert srv is not None

        resp = srv.admin(f"pcall(require, '{required_tt_module}')")
        if not resp[0]:
            self.skipTest(f'Tarantool {msg}')

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper


def skip_or_run_test_python(func, required_python_version, msg):
    """
    Decorator to skip or run tests depending on the Python version.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ == 'setUp':
            func(self, *args, **kwargs)

        ver = sys.version_info
        python_version_str = f'{ver.major}.{ver.minor}'
        python_version = pkg_resources.parse_version(python_version_str)
        support_version = pkg_resources.parse_version(required_python_version)
        if python_version < support_version:
            self.skipTest(f'Python {python_version} connector {msg}')

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper


def skip_or_run_sql_test(func):
    """
    Decorator to skip or run SQL-related tests depending on the
    tarantool version.

    Tarantool supports SQL-related stuff only since 2.0.0 version.
    So this decorator should wrap every SQL-related test to skip it if
    the tarantool version < 2.0.0 is used for testing.
    """

    return skip_or_run_test_tarantool(func, '2.0.0', 'does not support SQL')


def skip_or_run_varbinary_test(func):
    """
    Decorator to skip or run VARBINARY-related tests depending on
    the tarantool version.

    Tarantool supports VARBINARY type only since 2.2.1 version.
    See https://github.com/tarantool/tarantool/issues/4201
    """

    return skip_or_run_test_tarantool(func, '2.2.1',
                                      'does not support VARBINARY type')


def skip_or_run_decimal_test(func):
    """
    Decorator to skip or run decimal-related tests depending on
    the tarantool version.

    Tarantool supports decimal type only since 2.2.1 version.
    See https://github.com/tarantool/tarantool/issues/692
    """

    return skip_or_run_test_pcall_require(func, 'decimal',
                                          'does not support decimal type')


def skip_or_run_uuid_test(func):
    """
    Decorator to skip or run UUID-related tests depending on
    the tarantool version.

    Tarantool supports UUID type only since 2.4.1 version.
    See https://github.com/tarantool/tarantool/issues/4268
    """

    return skip_or_run_test_tarantool(func, '2.4.1',
                                      'does not support UUID type')


def skip_or_run_datetime_test(func):
    """
    Decorator to skip or run datetime-related tests depending on
    the tarantool version.

    Tarantool supports datetime type only since 2.10.0 version.
    See https://github.com/tarantool/tarantool/issues/5941
    """

    return skip_or_run_test_pcall_require(func, 'datetime',
                                          'does not support datetime type')


def skip_or_run_datetime_2_11_test(func):
    """
    Decorator to skip or run tests related to datetime module with
    fixes introduced in 2.11 release.

    See https://github.com/tarantool/tarantool/issues/7698 and
    https://github.com/tarantool/tarantool/issues/7700
    """

    return skip_or_run_test_tarantool(func, '2.11.0',
                                      'does not provide required datetime fixes')


def skip_or_run_error_extra_info_test(func):
    """
    Decorator to skip or run tests related to extra error info
    provided over iproto depending on the tarantool version.

    Tarantool provides extra error info only since 2.4.1 version.
    See https://github.com/tarantool/tarantool/issues/4398
    """

    return skip_or_run_test_tarantool(func, '2.4.1',
                                      'does not provide extra error info')


def skip_or_run_error_ext_type_test(func):
    """
    Decorator to skip or run tests related to error extension
    type depending on the tarantool version.

    Tarantool supports error extension type only since 2.4.1 version,
    yet encoding was introduced only in 2.10.0.
    See https://github.com/tarantool/tarantool/issues/4398,
    https://github.com/tarantool/tarantool/issues/6433
    """

    return skip_or_run_test_tarantool(func, '2.10.0',
                                      'does not support error extension type')


def skip_or_run_ssl_password_test_call(self):
    """
    Function to skip or run tests related to SSL password
    and SSL password files support. Supported only in Tarantool EE.
    Do not check Enterprise prefix since TNT_SSL_TEST already assumes
    it.

    Tarantool EE supports SSL passwords and password files only in
    current master since commit e1f47dd4 (after 2.11.0-entrypoint).
    See https://github.com/tarantool/tarantool-ee/issues/22
    """

    return skip_or_run_test_tarantool_call(self, '2.11.0',
                                           'does not support SSL passwords')


def skip_or_run_auth_type_test_call(self):
    """
    Function to skip or run tests related to configuring
    authentication method.

    Tarantool supports auth_type only in current master since
    commit 2574ff1a (after 2.11.0-entrypoint).
    See https://github.com/tarantool/tarantool/issues/7988
    https://github.com/tarantool/tarantool/issues/7989
    https://github.com/tarantool/tarantool-ee/issues/295
    https://github.com/tarantool/tarantool-ee/issues/322
    """

    return skip_or_run_test_tarantool_call(self, '2.11.0',
                                           'does not support auth type')


def skip_or_run_constraints_test(func):
    """
    Decorator to skip or run tests related to spaces with
    schema constraints.

    Tarantool supports schema constraints only since 2.10.0 version.
    See https://github.com/tarantool/tarantool/issues/6436
    """

    return skip_or_run_test_tarantool(func, '2.10.0',
                                      'does not support schema constraints')


def skip_or_run_iproto_basic_features_test(func):
    """
    Decorator to skip or run tests related to iproto ID requests,
    protocol version and features.

    Tarantool supports iproto ID requests only since 2.10.0 version.
    Protocol version is 3 for Tarantool 2.10.0,
    IPROTO_FEATURE_STREAMS, IPROTO_FEATURE_TRANSACTIONS
    and IPROTO_FEATURE_ERROR_EXTENSION are supported in Tarantool 2.10.0.
    See https://github.com/tarantool/tarantool/issues/6253
    """

    return skip_or_run_test_tarantool(func, '2.10.0',
                                      'does not support iproto ID and iproto basic features')
