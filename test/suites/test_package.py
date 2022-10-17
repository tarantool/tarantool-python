import os
import sys
import unittest

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

import tarantool


def is_test_pure_install():
    env = os.getenv("TEST_PURE_INSTALL")
    if env:
        env = env.upper()
        return env == "1" or env == "TRUE"
    return False


class TestSuite_Package(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' PACKAGE '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)


    def test_version(self):
        if is_test_pure_install():
            self.assertEqual(tarantool.__version__, metadata.version('tarantool'))
        else:
            self.assertEqual(
                tarantool.__version__, '0.0.0-dev',
                'Ensure that there is no tarantool/version.py file in your dev build')
