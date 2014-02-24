#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os

os.chdir(os.path.abspath(os.path.dirname(__file__)))

# Read package version without importing it
for line in open(os.path.join("tarantool", "__init__.py")):
    if line.startswith("__version__"):
        exec line
        break

# Extra commands for documentation management
cmdclass = {}
command_options = {}

# Build Sphinx documentation (html)
# python setup.py build_sphinx
# generates files into build/sphinx/html
try:
    from sphinx.setup_command import BuildDoc
    cmdclass["build_sphinx"] = BuildDoc
except ImportError:
    pass


# Upload Sphinx documentation to PyPI (using Sphinx-PyPI-upload)
# python setup.py build_sphinx
# updates documentation at http://packages.python.org/tarantool/
try:
    from sphinx_pypi_upload import UploadDoc
    cmdclass["upload_sphinx"] = UploadDoc
    command_options["upload_sphinx"] = {
            'upload_dir': ('setup.py', os.path.join("build", "sphinx", "html"))
    }
except ImportError:
    pass


# Test runner
# python setup.py test
try:
    from tests.setup_command import test
    cmdclass["test"] = test
except ImportError:
    pass

setup(
    name = "tarantool",
    packages = ["tarantool"],
    package_dir = {"tarantool": os.path.join("tarantool")},
    version = __version__,
    platforms = ["all"],
    author = "Konstantin Cherkasoff",
    author_email = "k.cherkasoff@gmail.com",
    url = "https://github.com/coxx/tarantool-python",
    license = "BSD",
    description = "Python client library for Tarantool Database",
    long_description = open("README.rst").read(),
    classifiers = [
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Database :: Front-Ends"
    ],
    cmdclass = cmdclass,
    command_options = command_options,
    install_requires = [
        'msgpack-python>=0.4',
    ]
)
