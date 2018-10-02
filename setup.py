#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import os
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

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
    from unit.setup_command import test
    cmdclass["test"] = test
except ImportError:
    pass


def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"""^__version__\s*=\s*(['"])(.+)\1""",
                              version_file, re.M)
    if version_match:
        return version_match.group(2)
    raise RuntimeError("Unable to find version string.")


setup(
    name="tarantool",
    packages=["tarantool"],
    package_dir={"tarantool": os.path.join("tarantool")},
    version=find_version('tarantool', '__init__.py'),
    platforms=["all"],
    author="Konstantin Cherkasoff",
    author_email="k.cherkasoff@gmail.com",
    url="https://github.com/tarantool/tarantool-python",
    license="BSD",
    description="Python client library for Tarantool 1.6 Database",
    long_description=read('README.rst'),
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Database :: Front-Ends"
    ],
    cmdclass=cmdclass,
    command_options=command_options,
    install_requires=[
        'msgpack-python>=0.4',
    ]
)
