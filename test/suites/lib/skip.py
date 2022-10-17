import functools
import pkg_resources
import re
import sys


def skip_or_run_test_tarantool(func, REQUIRED_TNT_VERSION, msg):
    """Decorator to skip or run tests depending on the tarantool
    version.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ == 'setUp':
            func(self, *args, **kwargs)

        if not hasattr(self, 'tnt_version'):
            srv = None

            if hasattr(self, 'servers'):
                srv = self.servers[0]

            if hasattr(self, 'srv'):
                srv = self.srv

            assert srv is not None

            self.__class__.tnt_version = srv.admin.tnt_version

        support_version = pkg_resources.parse_version(REQUIRED_TNT_VERSION)

        if self.tnt_version < support_version:
            self.skipTest('Tarantool %s %s' % (self.tnt_version, msg))

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper

def skip_or_run_test_pcall_require(func, REQUIRED_TNT_MODULE, msg):
    """Decorator to skip or run tests depending on tarantool
    module requre success or fail.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ == 'setUp':
            func(self, *args, **kwargs)

        srv = None

        if hasattr(self, 'servers'):
            srv = self.servers[0]

        if hasattr(self, 'srv'):
            srv = self.srv

        assert srv is not None

        resp = srv.admin("pcall(require, '%s')" % REQUIRED_TNT_MODULE)
        if not resp[0]:
            self.skipTest('Tarantool %s' % (msg, ))

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper


def skip_or_run_test_python(func, REQUIRED_PYTHON_VERSION, msg):
    """Decorator to skip or run tests depending on the Python version.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ == 'setUp':
            func(self, *args, **kwargs)

        ver = sys.version_info
        python_version_str = '%d.%d' % (ver.major, ver.minor)
        python_version = pkg_resources.parse_version(python_version_str)
        support_version = pkg_resources.parse_version(REQUIRED_PYTHON_VERSION)
        if python_version < support_version:
            self.skipTest('Python %s connector %s' % (python_version, msg))

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper


def skip_or_run_sql_test(func):
    """Decorator to skip or run SQL-related tests depending on the
    tarantool version.

    Tarantool supports SQL-related stuff only since 2.0.0 version.
    So this decorator should wrap every SQL-related test to skip it if
    the tarantool version < 2.0.0 is used for testing.
    """

    return skip_or_run_test_tarantool(func, '2.0.0', 'does not support SQL')


def skip_or_run_varbinary_test(func):
    """Decorator to skip or run VARBINARY-related tests depending on
    the tarantool version.

    Tarantool supports VARBINARY type only since 2.2.1 version.
    See https://github.com/tarantool/tarantool/issues/4201
    """

    return skip_or_run_test_tarantool(func, '2.2.1',
                                      'does not support VARBINARY type')


def skip_or_run_decimal_test(func):
    """Decorator to skip or run decimal-related tests depending on
    the tarantool version.

    Tarantool supports decimal type only since 2.2.1 version.
    See https://github.com/tarantool/tarantool/issues/692
    """

    return skip_or_run_test_pcall_require(func, 'decimal',
                                      'does not support decimal type')

def skip_or_run_UUID_test(func):
    """Decorator to skip or run UUID-related tests depending on
    the tarantool version.

    Tarantool supports UUID type only since 2.4.1 version.
    See https://github.com/tarantool/tarantool/issues/4268
    """

    return skip_or_run_test_tarantool(func, '2.4.1',
                                      'does not support UUID type')

def skip_or_run_datetime_test(func):
    """Decorator to skip or run datetime-related tests depending on
    the tarantool version.

    Tarantool supports datetime type only since 2.10.0 version.
    See https://github.com/tarantool/tarantool/issues/5941
    """

    return skip_or_run_test_pcall_require(func, 'datetime',
                                      'does not support datetime type')

def skip_or_run_error_extra_info_test(func):
    """Decorator to skip or run tests related to extra error info
    provided over iproto depending on the tarantool version.

    Tarantool provides extra error info only since 2.4.1 version.
    See https://github.com/tarantool/tarantool/issues/4398
    """

    return skip_or_run_test_tarantool(func, '2.4.1',
                                      'does not provide extra error info')

def skip_or_run_error_ext_type_test(func):
    """Decorator to skip or run tests related to error extension
    type depending on the tarantool version.

    Tarantool supports error extension type only since 2.4.1 version,
    yet encoding was introduced only in 2.10.0.
    See https://github.com/tarantool/tarantool/issues/4398,
    https://github.com/tarantool/tarantool/issues/6433
    """

    return skip_or_run_test_tarantool(func, '2.10.0',
                                      'does not support error extension type')
