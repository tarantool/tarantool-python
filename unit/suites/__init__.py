import os
import unittest

__tmp = os.getcwd()
os.chdir(os.path.abspath(os.path.dirname(__file__)))

from .test_schema import TestSuite_Schema
from .test_dml import TestSuite_Request
from .test_protocol import TestSuite_Protocol

test_cases = (TestSuite_Schema, TestSuite_Request, TestSuite_Protocol)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for testc in test_cases:
        suite.addTests(loader.loadTestsFromTestCase(testc))
    return suite


os.chdir(__tmp)


