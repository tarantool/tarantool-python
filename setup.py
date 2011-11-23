# -*- coding: utf-8 -*-
from distutils.core import setup
import os.path

setup(
    name = "tarantool",
    packages = ["tarantool"],
    package_dir = {"tarantool": os.path.join("src", "tarantool")},
    version = "0.2.1",
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
    ]
)
