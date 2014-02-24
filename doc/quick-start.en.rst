Quick start
===========

Connecting to the server
------------------------

Create connection to the server::

    >>> import tarantool
    >>> server = tarantool.connect("localhost", 33013)


Creating a space instance
-------------------------

Instance of :class:`~tarantool.space.Space` is a named object to access
the key space.

Create `` demo `` object which will be used to access the space `` 0 `` ::

    >>> demo = server.space(0)

All subsequent operations with space ``0`` performed using methods of the ``demo``.


Data Manipulation
-----------------

Select
^^^^^^

Select one single record with id ``'AAAA'`` from the space ``demo`` 
using primary key (index zero)::

    >>> demo.select('AAAA')

Select several records using primary index::

    >>> demo.select(['AAAA', 'BBBB', 'CCCC'])
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo'), ('CCCC', 'Charlie')]


Insert
^^^^^^

Insert tuple ``('DDDD', 'Delta')`` into the space ``demo``::

    >>> demo.insert(('DDDD', 'Delta'))

The first element is the primary key for the tuple.


Update
^^^^^^

Update the record with id ``'DDDD'`` placing the value ``'Denver'`` 
into the field ``1``::

    >>> demo.update('DDDD', [(1, '=', 'Denver')])
    [('DDDD', 'Denver')]

To find the record :meth:`~tarantool.space.Space.update` always uses 
the primary index.
Fields numbers are starting from zero. 
So field ``0`` is the first element in the tuple. 


Delete
^^^^^^

Delete single record identified by id ``'DDDD'``::

    >>> demo.delete('DDDD')
    [('DDDD', 'Denver')]

To find the record :meth:`~tarantool.space.Space.delete` always uses 
the primary index.


Call server-side functions
--------------------------

To call stored function method 
:meth:`Connection.call() <tarantool.connection.Connection.call()>` can be used::

    >>> server.call("box.select_range", (0, 0, 2, 'AAAA'))
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo')]

The same can be done using 
:meth:`Space.call() <tarantool.space.Space.call()>` method::

    >>> demo = server.space(0)
    >>> demo.call("box.select_range", (0, 0, 2, 'AAAA'))
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo')]

Method :meth:`Space.call() <tarantool.space.Space.call()>` is just
an alias for
:meth:`Connection.call() <tarantool.connection.Connection.call()>`
