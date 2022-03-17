Python driver for Tarantool
===========================

This package is a pure-python client library for `Tarantool`_.

`Documentation`_  |  `Downloads`_  |  `PyPI`_  |  `GitHub`_  | `Issue tracker`_

.. _`Documentation`: http://tarantool-python.readthedocs.org/en/latest/
.. _`Downloads`: http://pypi.python.org/pypi/tarantool#downloads
.. _`PyPI`: http://pypi.python.org/pypi/tarantool
.. _`GitHub`: https://github.com/tarantool/tarantool-python
.. _`Issue tracker`: https://github.com/tarantool/tarantool-python/issues

.. image:: https://github.com/tarantool/tarantool-python/actions/workflows/testing.yml/badge.svg?branch=master
    :target: https://github.com/tarantool/tarantool-python/actions/workflows/testing.yml

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
Redis. It's open source, `BSD-2-Clause`_ licensed.

Features
--------

* ANSI SQL, including views, joins, referential and check constraints
* Lua packages for non-blocking I/O, fibers and HTTP
* MsgPack data format and MsgPack based client-server protocol
* Two data engines:

  * memtx - the in-memory storage engine with optional persistence
  * vinyl - the on-disk storage engine to use with large data sets

* secondary key and index iterators support (can be non-unique and composite)
* multiple index types: HASH, BITSET, TREE, RTREE
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

Look at `asynctnt`_ for asynchronous Python driver based on asyncio. See
also the `feature comparison table`_.

Run tests
^^^^^^^^^

On Linux:

.. code-block:: console

   $ python setup.py test

On Windows:

* Setup a Linux machine with installed tarantool (called ``remote`` later).
* (on ``remote``) Copy ``test/suites/lib/tarantool_python_ci.lua`` to
  ``/etc/tarantool/instances.available``.
* (on ``remote``) Run ``tarantoolctl start tarantool_python_ci``.
* Set the following environment variables:
  * ``REMOTE_TARANTOOL_HOST=...``,
  * ``REMOTE_TARANTOOL_CONSOLE_PORT=3302``.
* Run ``python setup.py test``.

.. _`Tarantool`:
.. _`Tarantool Database`:
.. _`Tarantool Homepage`: https://tarantool.io
.. _`Tarantool at Github`: https://github.com/tarantool/tarantool
.. _`Tarantool User Guide`: https://www.tarantool.io/en/doc/latest/
.. _`Client-server protocol specification`: https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/
.. _`BSD-2-Clause`: https://opensource.org/licenses/BSD-2-Clause
.. _`asynctnt`: https://github.com/igorcoding/asynctnt
.. _`feature comparison table`: https://www.tarantool.io/en/doc/latest/book/connectors/#python-feature-comparison

License
^^^^^^^

BSD-2-Clause. See the ``LICENSE`` file.
