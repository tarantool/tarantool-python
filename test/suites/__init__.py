"""
Module tests entrypoint.
"""

import os
import unittest

from .test_schema import TestSuiteSchemaUnicodeConnection
from .test_schema import TestSuiteSchemaBinaryConnection
from .test_dml import TestSuiteRequest
from .test_protocol import TestSuiteProtocol
from .test_reconnect import TestSuiteReconnect
from .test_mesh import TestSuiteMesh
from .test_pool import TestSuitePool
from .test_execute import TestSuiteExecute
from .test_dbapi import TestSuiteDBAPI
from .test_encoding import TestSuiteEncoding
from .test_ssl import TestSuiteSsl
from .test_decimal import TestSuiteDecimal
from .test_uuid import TestSuiteUUID
from .test_datetime import TestSuiteDatetime
from .test_interval import TestSuiteInterval
from .test_package import TestSuitePackage
from .test_error_ext import TestSuiteErrorExt
from .test_push import TestSuitePush
from .test_connection import TestSuiteConnection
from .test_crud import TestSuiteCrud

test_cases = (TestSuiteSchemaUnicodeConnection,
              TestSuiteSchemaBinaryConnection,
              TestSuiteRequest, TestSuiteProtocol, TestSuiteReconnect,
              TestSuiteMesh, TestSuiteExecute, TestSuiteDBAPI,
              TestSuiteEncoding, TestSuitePool, TestSuiteSsl,
              TestSuiteDecimal, TestSuiteUUID, TestSuiteDatetime,
              TestSuiteInterval, TestSuitePackage, TestSuiteErrorExt,
              TestSuitePush, TestSuiteConnection, TestSuiteCrud,)


def load_tests(loader, tests, pattern):
    """
    Add suites to test run.
    """
    # pylint: disable=unused-argument

    suite = unittest.TestSuite()
    for testc in test_cases:
        suite.addTests(loader.loadTestsFromTestCase(testc))
    return suite


__tmp = os.getcwd()
os.chdir(os.path.abspath(os.path.dirname(__file__)))

os.chdir(__tmp)

# Workaround to disable unittest output truncating
__import__('sys').modules['unittest.util']._MAX_LENGTH = 99999  # pylint: disable=protected-access
