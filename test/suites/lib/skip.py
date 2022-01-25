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
            self.__class__.tnt_version = re.match(
                r'[\d.]+', self.srv.admin('box.info.version')[0]
            ).group()

        tnt_version = pkg_resources.parse_version(self.tnt_version)
        support_version = pkg_resources.parse_version(REQUIRED_TNT_VERSION)

        if tnt_version < support_version:
            self.skipTest('Tarantool %s %s' % (self.tnt_version, msg))

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper


def skip_or_run_test_python_major(func, REQUIRED_PYTHON_MAJOR, msg):
    """Decorator to skip or run tests depending on the Python major
    version.

    Also, it can be used with the 'setUp' method for skipping
    the whole test suite.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if func.__name__ == 'setUp':
            func(self, *args, **kwargs)

        major = sys.version_info.major
        if major != REQUIRED_PYTHON_MAJOR:
            self.skipTest('Python %s connector %s' % (major, msg))

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

