import os
import unittest

__tmp = os.getcwd()
os.chdir(os.path.abspath(os.path.dirname(__file__)))

from .test_schema import TestSuite_Schema_UnicodeConnection
from .test_schema import TestSuite_Schema_BinaryConnection
from .test_dml import TestSuite_Request
from .test_protocol import TestSuite_Protocol
from .test_reconnect import TestSuite_Reconnect
from .test_mesh import TestSuite_Mesh
from .test_pool import TestSuite_Pool
from .test_execute import TestSuite_Execute
from .test_dbapi import TestSuite_DBAPI
from .test_encoding import TestSuite_Encoding
from .test_ssl import TestSuite_Ssl
from .test_decimal import TestSuite_Decimal
from .test_uuid import TestSuite_UUID
from .test_datetime import TestSuite_Datetime
from .test_interval import TestSuite_Interval
from .test_package import TestSuite_Package
from .test_error_ext import TestSuite_ErrorExt
from .test_push import TestSuite_Push
from .test_connection import TestSuite_Connection
from .test_crud import TestSuite_Crud

test_cases = (TestSuite_Schema_UnicodeConnection,
              TestSuite_Schema_BinaryConnection,
              TestSuite_Request, TestSuite_Protocol, TestSuite_Reconnect,
              TestSuite_Mesh, TestSuite_Execute, TestSuite_DBAPI,
              TestSuite_Encoding, TestSuite_Pool, TestSuite_Ssl,
              TestSuite_Decimal, TestSuite_UUID, TestSuite_Datetime,
              TestSuite_Interval, TestSuite_ErrorExt, TestSuite_Push,
              TestSuite_Connection, TestSuite_Crud,)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for testc in test_cases:
        suite.addTests(loader.loadTestsFromTestCase(testc))
    return suite


os.chdir(__tmp)

# Workaround to disable unittest output truncating
__import__('sys').modules['unittest.util']._MAX_LENGTH = 99999
