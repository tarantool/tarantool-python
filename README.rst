Python client library for Tarantool Database
============================================

This package is a pure-python client library for `Tarantool Database`_.

`Documentation`_  |  `Downloads`_  |  `PyPI`_  |  `GitHub`_  | `Issue tracker`_

.. _`Documentation`: http://packages.python.org/tarantool
.. _`Downloads`: http://pypi.python.org/pypi/tarantool#downloads
.. _`PyPI`: http://pypi.python.org/pypi/tarantool
.. _`GitHub`: https://github.com/mailru/tarantool-python
.. _`Issue tracker`: https://github.com/mailru/tarantool-python/issues


Download and Install
--------------------

The recommended way to install ``tarantool`` package::

    $ pip install tarantool

You can also download zip archive, unpack it and run::

    $ python setup.py install

To install development version of the package use pip::

    $ pip install git+https://github.com/mailru/tarantool-python


--------------------------------------------------------------------------------


What is Tarantool?
------------------

`Tarantool`_ is a damn fast key/value data store originally designed by
`Mail.Ru`_ and released under the terms of `BSD license`_. `Tarantool`_ is
production-ready and actively used at `Mail.Ru`_ - one of the leading
Russian web content providers.

 * NoSQL database
 * In-memory storage
 * Data is protected by on-disk write-ahead-log and snapshots
 * Key-value data model: each record is a tuple of multiple values,
   identified by primary key
 * Records can be accessed using secondary indexes
 * Secondary indexes can be non-unique and composite (include multiple fields)
 * Server-side stored procedures in Lua
 * Very fast binary client-server protocol


See More
^^^^^^^^

 * `Tarantool Homepage`_
 * `Tarantool at Github`_
 * `Tarantool User Guide`_
 * `Client-server Protocol Specification`_


.. _`Tarantool`:
.. _`Tarantool Database`:
.. _`Tarantool Homepage`: http://tarantool.org
.. _`Tarantool at Github`: https://github.com/mailru/tarantool
.. _`Tarantool User Guide`: http://tarantool.org/tarantool_user_guide.html
.. _`Client-server protocol specification`: https://raw.github.com/mailru/tarantool/master/doc/box-protocol.txt
.. _`Mail.Ru`: http://mail.ru
.. _`BSD license`: http://www.gnu.org/licenses/license-list.html#ModifiedBSD
