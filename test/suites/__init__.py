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

test_cases = (TestSuite_Schema_UnicodeConnection,
              TestSuite_Schema_BinaryConnection,
              TestSuite_Request, TestSuite_Protocol, TestSuite_Reconnect,
              TestSuite_Mesh, TestSuite_Execute, TestSuite_DBAPI,
              TestSuite_Encoding, TestSuite_Pool)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for testc in test_cases:
        suite.addTests(loader.loadTestsFromTestCase(testc))
    return suite


os.chdir(__tmp)


