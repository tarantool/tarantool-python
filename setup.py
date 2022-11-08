#!/usr/bin/env python

import codecs
import os
import re

try:
    from setuptools import setup, find_packages
    from setuptools.command.build_py import build_py
except ImportError:
    from distutils.core import setup, find_packages
    from distutils.command.build_py import build_py

# Extra commands for documentation management
cmdclass = {}
command_options = {}

# Build the package
# python setup.py build_py
# builds the package with generating correspondent VERSION file
class BuildPyCommand(build_py):
    def run(self):
        # Import here to allow to run commands
        # like `python setup.py test` without setuptools_scm.
        from setuptools_scm import get_version
        version = get_version()

        package_dir = self.get_package_dir('tarantool')
        version_file = os.path.join(package_dir, 'version.py')
        with open(version_file, 'w') as file:
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
    from test.setup_command import test
    cmdclass["test"] = test
except ImportError:
    pass


def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()

def get_dependencies(file):
    root = os.path.dirname(os.path.realpath(__file__))
    requirements = os.path.join(root, file)
    result = []
    if os.path.isfile(requirements):
        with open(requirements) as f:
            return f.read().splitlines()
    raise RuntimeError("Unable to get dependencies from file " + file)

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"""^__version__\s*=\s*(['"])(.+)\1""",
                              version_file, re.M)
    if version_match:
        return version_match.group(2)
    raise RuntimeError("Unable to find version string.")

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
