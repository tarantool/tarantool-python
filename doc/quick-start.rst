Quick start
===========

Connecting to the server
------------------------

Create a connection to the server:

.. code-block:: python

    >>> import tarantool
    >>> conn = tarantool.Connection('localhost', 3301, user='user', password='pass')

Data manipulation
-----------------

Select
^^^^^^

:meth:`~tarantool.Connection.select` a tuple with id ``'AAAA'`` from
the space ``demo`` using primary index:

.. code-block:: python

    >>> resp = conn.select('demo', 'AAAA')
    >>> len(resp)
    1
    >>> resp[0]
    ['AAAA', 'Alpha']

:meth:`~tarantool.Connection.select` a tuple with secondary index
key ``'Alpha'`` from the space ``demo`` with secondary index ``sec``:

.. code-block:: python

    >>> resp = conn.select('demo', 'Alpha', index='sec')
    >>> resp
    - ['AAAA', 'Alpha']

Insert
^^^^^^

:meth:`~tarantool.Connection.insert` the tuple ``('BBBB', 'Bravo')``
into the space ``demo``:

.. code-block:: python

    >>> conn.insert('demo', ('BBBB', 'Bravo'))
    - ['BBBB', 'Bravo']

Throws an error if there is already a tuple with the same primary key.

.. code-block:: python

    >>> try:
    ...     conn.insert('demo', ('BBBB', 'Bravo'))
    ... except Exception as exc:
    ...     print(exc)
    ... 
    (3, 'Duplicate key exists in unique index "pk" in space "demo" with old tuple - ["BBBB", "Bravo"] and new tuple - ["BBBB", "Bravo"]')

Replace
^^^^^^^

:meth:`~tarantool.Connection.replace` inserts the tuple
``('CCCC', 'Charlie')`` into the space ``demo``, if there is no tuple
with primary key ``'CCCC'``:

.. code-block:: python

    >>> conn.replace('demo', ('CCCC', 'Charlie'))
    - ['CCCC', 'Charlie']

If there is already a tuple with the same primary key, replaces it:

.. code-block:: python

    >>> conn.replace('demo', ('CCCC', 'Charlie-2'))
    - ['CCCC', 'Charlie-2']

Update
^^^^^^

:meth:`~tarantool.Connection.update` the tuple with id ``'BBBB'`` placing
the value ``'Bravo-2'`` into the field ``1``:

.. code-block:: python

    >>> conn.update('demo', 'BBBB', [('=', 1, 'Bravo-2')])
    - ['BBBB', 'Bravo-2']

Field numeration starts from zero, so the field ``0`` is the first element
in the tuple. Tarantool 2.3.1 and newer supports field name identifiers.

Upsert
^^^^^^

:meth:`~tarantool.Connection.upsert` inserts the tuple, if tuple with
id ``'DDDD'`` not exists. Otherwise, updates tuple fields.

.. code-block:: python

    >>> conn.upsert('demo', ('DDDD', 'Delta'), [('=', 1, 'Delta-2')])

    >>> conn.select('demo', 'DDDD')
    - ['DDDD', 'Delta']
    >>> conn.upsert('demo', ('DDDD', 'Delta'), [('=', 1, 'Delta-2')])

    >>> conn.select('demo', 'DDDD')
    - ['DDDD', 'Delta-2']

Delete
^^^^^^

:meth:`~tarantool.Connection.delete` a tuple identified by id ``'AAAA'``:

.. code-block:: python

    >>> conn.delete('demo', 'AAAA')
    - [('AAAA', 'Alpha')]

Creating a space instance
-------------------------

An instance of :class:`~tarantool.space.Space` is a named object to access
the key space.

Create a ``demo`` object that will be used to access the space 
with id ``'demo'``:

.. code-block:: python

    >>> demo = conn.space('demo')

You can use the space instance to do data manipulations without
specifying space id.

.. code-block:: python

    >>> demo.select('AAAA')
    - ['AAAA', 'Alpha']
    >>> demo.insert(('BBBB', 'Bravo'))
    - ['BBBB', 'Bravo']

Call server-side functions
--------------------------

:meth:`~tarantool.Connection.call` a stored Lua procedure:

.. code-block:: python

    >>> conn.call("my_add", (1, 2))
    - 3

Evaluate Lua code
-----------------

:meth:`~tarantool.Connection.eval` arbitrary Lua code on a server:

.. code-block:: python

    >>> lua_code = r"""
    ...     local a, b = ...
    ...     return a + b
    ... """
    >>> conn.eval(lua_code, (1, 2))
    - 3

Execute SQL query
-----------------

:meth:`~tarantool.Connection.execute` SQL query on a Tarantool server:

.. code-block:: python

    >>> conn.execute('insert into "demo" values (:id, :name)', {'id': 'BBBB', 'name': 'Bravo'})


Connecting to a cluster of servers
----------------------------------

Create a connection to several servers:

.. code-block:: python

    >>> import tarantool
    >>> conn = tarantool.ConnectionPool(
    ...        [{'host':'localhost', 'port':3301},
    ...        {'host':'localhost', 'port':3302}],
    ...        user='user', password='pass')

:class:`~tarantool.ConnectionPool` is best suited to work with
a single replicaset. Its API is the same as a single server
:class:`~tarantool.Connection`, but requests support ``mode``
parameter (a :class:`tarantool.Mode` value) to choose between
read-write and read-only pool instances:

.. code-block:: python

    >>> resp = conn.select('demo', 'AAAA', mode=tarantool.Mode.PREFER_RO)
    >>> resp
    - ['AAAA', 'Alpha']
