Summary: Python client library for Tarantool Database
Name: tarantool-python
Version: 0.6.5
Release: 1%{?dist}
Source0: tarantool-python-%{version}.tar.gz
License: BSD
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Konstantin Cherkasoff <k.cherkasoff@gmail.com>
Requires: python-msgpack
Url: https://github.com/tarantool/tarantool-python
BuildRequires: python-setuptools
%description
Python driver for Tarantool 1.6
===============================

This package is a pure-python client library for `Tarantool`_.

`Documentation`_  |  `Downloads`_  |  `PyPI`_  |  `GitHub`_  | `Issue tracker`_

.. _`Documentation`: http://tarantool-python.readthedocs.org/en/latest/
.. _`Downloads`: http://pypi.python.org/pypi/tarantool#downloads
.. _`PyPI`: http://pypi.python.org/pypi/tarantool
.. _`GitHub`: https://github.com/tarantool/tarantool-python
.. _`Issue tracker`: https://github.com/tarantool/tarantool-python/issues

.. image:: https://travis-ci.org/tarantool/tarantool-python.svg?branch=master
    :target: https://travis-ci.org/tarantool/tarantool-python

Download and Install
--------------------

The recommended way to install ``tarantool`` package is using PIP
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For Tarantool version < 1.6.0 you must get ``0.3.*`` connector version::

    $ pip install tarantool\<0.4

For later Tarantool use version ``0.5.*`` connector version::

    $ pip install tarantool\>0.4

You can also download zip archive, unpack it and run
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    $ python setup.py install

To install development version of the package using pip
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For Tarantool version < 1.6.0 you must get ``stable`` branch::

    $ pip install git+https://github.com/tarantool/tarantool-python.git@stable

For later Tarantool use ``master`` branch::

    $ pip install git+https://github.com/tarantool/tarantool-python.git@master

--------------------------------------------------------------------------------

What is Tarantool?
------------------

`Tarantool`_ is a NoSQL database running inside a Lua program. It combines the network programming power of Node.JS with data persistency capabilities of Redis. It's open source, `BSD licensed`_.

Features
--------

    * Lua packages for non-blocking I/O, fibers and HTTP
    * MsgPack data format and MsgPack based client-server protocol
    * Two data engines:
        * 100% in-memory with optional persistence
        * 2-level disk-based B-tree, to use with large data sets (powered by `Sophia`_)
    * secondary key and index iterators support (can be non-unique and composite)
    * multiple index types: HASH, BITSET, TREE
    * asynchronous master-master replication
    * authentication and access control


See More
^^^^^^^^

 * `Tarantool Homepage`_
 * `Tarantool at Github`_
 * `Tarantool User Guide`_
 * `Client-server Protocol Specification`_


.. _`Tarantool`:
.. _`Tarantool Database`:
.. _`Tarantool Homepage`: http://tarantool.org
.. _`Tarantool at Github`: https://github.com/tarantool/tarantool
.. _`Tarantool User Guide`: http://tarantool.org/doc/user_guide.html
.. _`Client-server protocol specification`: http://tarantool.org/doc/dev_guide/box-protocol.html
.. _`Sophia`: http://sphia.org
.. _`BSD licensed`: http://www.gnu.org/licenses/license-list.html#ModifiedBSD


%prep
%setup -q -n %{name}-%{version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
