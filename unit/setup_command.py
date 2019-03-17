#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import unittest
import setuptools
from distutils.errors import DistutilsError

from glob import glob

class test(setuptools.Command):
    user_options = []
    description = 'Run unit tests'

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        '''
        Find all tests in test/tarantool/ and run them
        '''

        tests = unittest.defaultTestLoader.discover('unit')
        test_runner = unittest.TextTestRunner(verbosity=2)
        result = test_runner.run(tests)
        if not result.wasSuccessful():
            raise DistutilsError('There are failed tests')
