import functools
import pkg_resources
import re

SQL_SUPPORT_TNT_VERSION = '2.0.0'


def skip_or_run_sql_test(func):
    """Decorator to skip or run SQL-related tests depending on the tarantool
    version.

    Tarantool supports SQL-related stuff only since 2.0.0 version. So this
    decorator should wrap every SQL-related test to skip it if the tarantool
    version < 2.0.0 is used for testing.

    Also, it can be used with the 'setUp' method for skipping the whole test
    suite.
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
        sql_support_tnt_version = pkg_resources.parse_version(
            SQL_SUPPORT_TNT_VERSION
        )

        if tnt_version < sql_support_tnt_version:
            self.skipTest(
                'Tarantool %s does not support SQL' % self.tnt_version
            )

        if func.__name__ != 'setUp':
            func(self, *args, **kwargs)

    return wrapper
