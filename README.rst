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

.. code-block:: console

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

`Tarantool`_ is a NoSQL database running inside a Lua program. It combines the
network programming power of Node.JS with data persistency capabilities of
Redis. It's open source, `BSD licensed`_.

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

NOTE
^^^^

This driver is synchronous, so connection mustn't be shared between threads/processes.

.. _`Tarantool`:
.. _`Tarantool Database`:
.. _`Tarantool Homepage`: http://tarantool.org
.. _`Tarantool at Github`: https://github.com/tarantool/tarantool
.. _`Tarantool User Guide`: http://tarantool.org/doc/book/index.html
.. _`Client-server protocol specification`: http://tarantool.org/doc/dev_guide/box-protocol.html
.. _`Sophia`: http://sphia.org
.. _`BSD licensed`: http://www.gnu.org/licenses/license-list.html#ModifiedBSD
