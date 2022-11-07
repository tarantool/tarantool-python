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

Download and install
--------------------

With pip (recommended)
^^^^^^^^^^^^^^^^^^^^^^

The recommended way to install the ``tarantool`` package is using ``pip``.

.. code-block:: bash

     $ pip3 install tarantool

ZIP archive
^^^^^^^^^^^

You can also download zip archive, unpack it and run:

.. code-block:: bash

    $ make install

Development version
^^^^^^^^^^^^^^^^^^^

You can also install the development version of the package using ``pip``.

.. code-block:: bash

    $ pip3 install git+https://github.com/tarantool/tarantool-python.git@master

--------------------------------------------------------------------------------

What is Tarantool?
------------------

`Tarantool`_ is an in-memory computing platform originally designed by 
`VK`_ and released under the terms of `BSD license`_.

Features
--------

* ANSI SQL, including views, joins, referential and check constraints
* Lua packages for non-blocking I/O, fibers, and HTTP
* MessagePack data format and MessagePack-based client-server protocol
* Two data engines:

  * memtx – in-memory storage engine with optional persistence
  * vinyl – on-disk storage engine to use with larger data sets

* Secondary key and index iterator support (can be non-unique and composite)
* Multiple index types: HASH, BITSET, TREE, RTREE
* Asynchronous master-master replication
* Authentication and access control

See More
^^^^^^^^

* `Tarantool homepage`_
* `Tarantool on GitHub`_
* `Tarantool documentation`_
* `Client-server protocol specification`_

NOTE
^^^^

This driver is synchronous, so connection mustn't be shared between threads/processes.

If you're looking for an asynchronous Python driver based on ``asyncio``,
consider using `asynctnt`_ . See also the `feature comparison table`_.

Run tests
^^^^^^^^^

On Linux:

.. code-block:: bash

   $ make test

On Windows:

* Setup a Linux machine with Tarantool installed.
  This machine will be referred to as ``remote`` in this instruction.
* (On ``remote``) Copy ``test/suites/lib/tarantool_python_ci.lua`` to
  ``/etc/tarantool/instances.available``.
* (On ``remote``) Run ``tarantoolctl start tarantool_python_ci``.
* Set the following environment variables:
  * ``REMOTE_TARANTOOL_HOST=...``,
  * ``REMOTE_TARANTOOL_CONSOLE_PORT=3302``.
* Run ``make test``.

Build docs
^^^^^^^^^^

To build documentation, first you must install its build requirements:

.. code-block:: bash

    $ pip3 install -r docs/requirements.txt

Then run

.. code-block:: bash

    $ make docs

You may host local documentation server with

.. code-block:: bash

    $ python3 -m http.server --directory build/sphinx/html

Open ``localhost:8000`` in your browser to read the docs.

.. _`Tarantool`:
.. _`Tarantool Database`:
.. _`Tarantool homepage`: https://tarantool.io
.. _`Tarantool on GitHub`: https://github.com/tarantool/tarantool
.. _`Tarantool documentation`: https://www.tarantool.io/en/doc/latest/
.. _`VK`: https://vk.company
.. _`Client-server protocol specification`: https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/
.. _`BSD`:
.. _`BSD license`:
.. _`BSD-2-Clause`: https://opensource.org/licenses/BSD-2-Clause
.. _`asynctnt`: https://github.com/igorcoding/asynctnt
.. _`feature comparison table`: https://www.tarantool.io/en/doc/latest/book/connectors/#python-feature-comparison

License
^^^^^^^

BSD-2-Clause. See the ``LICENSE`` file.
