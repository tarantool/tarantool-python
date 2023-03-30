#!/usr/bin/env python
"""
Package setup commands.
"""
# pylint: disable=bad-option-value,too-many-ancestors

import codecs
import os

from setuptools import setup, find_packages
from setuptools.command.build_py import build_py

# Extra commands for documentation management
cmdclass = {}
command_options = {}


class BuildPyCommand(build_py):
    """
    Build the package
    python setup.py build_py
    builds the package with generating correspondent VERSION file
    """

    def run(self):
        """
        Run the command.
        """

        # Import here to allow to run commands
        # like `python setup.py test` without setuptools_scm.
        # pylint: disable=import-outside-toplevel,import-error
        from setuptools_scm import get_version
        version = get_version()

        package_dir = self.get_package_dir('tarantool')
        version_file = os.path.join(package_dir, 'version.py')
        with open(version_file, 'w', encoding='utf-8') as file:
            file.write(f"__version__ = '{version}'")

        return super().run()


cmdclass["build_py"] = BuildPyCommand

# Build Sphinx documentation (html)
# python setup.py build_sphinx
# generates files into build/sphinx/html
try:
    from sphinx.setup_command import BuildDoc
    cmdclass["build_sphinx"] = BuildDoc
except ImportError:
    pass


# Test runner
# python setup.py test
try:
    from test.setup_command import Test
    cmdclass["test"] = Test
except ImportError:
    pass


def read(*parts):
    """
    Read the file.
    """

    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as file:
        return file.read()


def get_dependencies(filename):
    """
    Get package dependencies from the `requirements.txt`.
    """

    root = os.path.dirname(os.path.realpath(__file__))
    requirements = os.path.join(root, filename)
    if os.path.isfile(requirements):
        with open(requirements, encoding='utf-8') as file:
            return file.read().splitlines()
    raise RuntimeError("Unable to get dependencies from file " + filename)


packages = [item for item in find_packages('.') if item.startswith('tarantool')]

setup(
    name="tarantool",
    packages=packages,
    package_dir={"tarantool": "tarantool"},
    include_package_data=True,
    use_scm_version=True,
    platforms=["all"],
    author="tarantool-python AUTHORS",
    author_email="admin@tarantool.org",
    url="https://github.com/tarantool/tarantool-python",
    license="BSD",
    description="Python client library for Tarantool",
    long_description=read('README.rst'),
    long_description_content_type='text/x-rst',
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Database :: Front-Ends"
    ],
    cmdclass=cmdclass,
    command_options=command_options,
    install_requires=get_dependencies('requirements.txt'),
    setup_requires=[
        'setuptools_scm==6.4.2',
    ],
    python_requires='>=3.6',
)
