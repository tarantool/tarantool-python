# -*- coding: utf-8 -*-
import setuptools
from glob import glob
import os.path
import sys
import unittest

class test(setuptools.Command):
    user_options = []
    description = 'Run unit tests'
    user_options = [
        ('verbosity=', 'v', 'Output verbosity')
    ]

    def initialize_options(self):
        self.verbosity = 2

    def finalize_options(self):
        try:
            self.verbosity = int(self.verbosity)
        except ValueError:
            self.verbosity = 2

    def run(self):
        '''
        Find all tests in test/tarantool/ and run them
        '''
        root = os.path.dirname(os.path.dirname(__file__))
        sys.path.insert(0, root)
        sys.path.insert(1, os.path.join(root, "src"))

        testfiles = []
        for filename in glob(os.path.join(root, "tests", "tarantool", "*.py")):
            if filename.endswith('__init__.py'): continue
            testfiles.append("tests.tarantool." + os.path.splitext(os.path.basename(filename))[0])

        tests = unittest.defaultTestLoader.loadTestsFromNames(testfiles)
        test_runner = unittest.TextTestRunner(verbosity = self.verbosity)
        test_runner.run(tests)
