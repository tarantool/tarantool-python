.. encoding: utf-8

Developer's guide
=================

Basic concepts
--------------

Spaces
^^^^^^

A space is a collection of tuples.
Usually, tuples in one space represent objects of the same type,
although not necessarily.

.. note:: Spaces are analogous to tables in traditional (SQL) databases.

Spaces have integer identifiers defined in the server configuration.
One of the ways to access a space as a named object is by using the method
:meth:`Connection.space() <tarantool.connection.Connection.space>`
and an instance of :class:`~tarantool.space.Space`.

Example::

    >>> customer = connection.space(0)
    >>> customer.insert(('FFFF', 'Foxtrot'))


Field types
^^^^^^^^^^^

Three field types are supported in Tarantool: ``STR``, ``NUM``, and ``NUM64``.
These types are used only for index configuration.
They are neither saved in the tuple data nor transferred between the client and the server.
Thus, from the client point of view, fields are raw byte arrays
without explicitly defined types.

For a Python developer, it is much easier to use native types:
``int``, ``long``, ``unicode`` (``int`` and ``str`` for Python 3.x).
For raw binary data, use ``bytes`` (in this case, type casting is not performed).

Tarantool data types corresponds to the following Python types:
    • ``RAW`` - ``bytes``
    • ``STR`` - ``unicode`` (``str`` for Python 3.x)
    • ``NUM`` - ``int``
    • ``NUM64`` - ``int`` or ``long`` (``int`` for Python 3.x)

To enable automatic type casting, please define a schema for the spaces:

    >>> import tarantool
    >>> schema = {
            0: { # Space description
                'name': 'users', # Space name
                'default_type': tarantool.STR, # Type that is used to decode fields not listed below
                'fields': {
                    0: ('numfield', tarantool.NUM), # (field name, field type)
                    1: ('num64field', tarantool.NUM64),
                    2: ('strfield', tarantool.STR),
                    #2: { 'name': 'strfield', 'type': tarantool.STR }, # Alternative syntax
                    #2: tarantool.STR # Alternative syntax
                },
                'indexes': {
                    0: ('pk', [0]), # (name, [field_no])
                    #0: { 'name': 'pk', 'fields': [0]}, # Alternative syntax
                    #0: [0], # Alternative syntax
                }
            }
        }
    >>> connection = tarantool.connect(host = 'localhost', port=33013, schema = schema)
    >>> demo = connection.space('users')
    >>> demo.insert((0, 12, u'this is a unicode string'))
    >>> demo.select(0)
    [(0, 12, u'this is a unicode string')]

As you can see, original "raw" fields were cast to native types as defined in the schema.

A Tarantool tuple can contain any number of fields.
If some fields are not defined, then ``default_type`` will be used.

To prevent implicit type casting for strings, use the ``RAW`` type.
Raw byte fields should be used if the application uses binary data
(like images or Python objects packed with ``pickle``).

You can also specify a schema for CALL results:

    >>> ...
    # Copy schema decription from the 'users' space
    >>> connection.call("box.select", '0', '0', 0L, space_name='users');
    [(0, 12, u'this is unicode string')]
    # Provide schema description explicitly
    >>> field_defs = [('numfield', tarantool.NUM), ('num64field', tarantool.NUM)]
    >>> connection.call("box.select", '0', '1', 184L, field_defs = field_defs, default_type = tarantool.STR);
    [(0, 12, u'this is unicode string')]

.. note::

   Python 2.6 adds :class:`bytes` as a synonym for the :class:`str` type, and it also supports the ``b''`` notation.


.. note:: **utf-8** is always used for type conversion between ``unicode`` and ``bytes``.



Request response
^^^^^^^^^^^^^^^^

Requests (:meth:`insert() <tarantool.space.Space.insert>`,
:meth:`delete() <tarantool.space.Space.delete>`,
:meth:`update() <tarantool.space.Space.update>`,
:meth:`select() <tarantool.space.Space.select>`) return a
:class:`~tarantool.response.Response` instance.

The class :class:`~tarantool.response.Response` inherits from `list`,
so a response is, in fact, a list of tuples.

In addition, a :class:`~tarantool.response.Response` instance has the ``rowcount`` attribute.
The value of ``rowcount`` equals to the number of records affected by the request.
For example, for :meth:`delete() <tarantool.space.Space.delete>`,
the request ``rowcount`` equals to ``1`` if a record was deleted.



Connect to a server
-------------------

To connect to a server, use the :meth:`tarantool.connect` method.
It returns a :class:`~tarantool.connection.Connection` instance.

Example::

    >>> import tarantool
    >>> connection = tarantool.connect("localhost", 33013)
    >>> type(connection)
    <class 'tarantool.connection.Connection'>



Data manipulation
-----------------

Tarantool supports four basic operations:
**insert**, **delete**, **update** and **select**.


Inserting and replacing records
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To insert or replace records, use the :meth:`Space.insert() <tarantool.space.Space.insert>`
method.

Example::

    >>> user.insert((user_id, email, int(time.time())))

The first element of a tuple is always its unique primary key.

If an entry with the same key already exists, it will be replaced
without any warning or error message.

.. note:: For ``insert`` requests, ``Response.rowcount`` always equals ``1``.


Deleting records
^^^^^^^^^^^^^^^^

To delete records, use the :meth:`Space.delete() <tarantool.space.Space.delete>` method.

Example::

    >>> user.delete(primary_key)

.. note:: If the record was deleted, ``Response.rowcount`` equals ``1``.
          If the record was not found, ``Response.rowcount`` equals ``0``.


Updating records
^^^^^^^^^^^^^^^^

An *update* request in Tarantool allows updating multiple
fields of a tuple simultaneously and atomically.

To update records, use the :meth:`Space.update() <tarantool.space.Space.update>`
method.

Example::

    >>> user.update(1001, [('=', 1, 'John'), ('=', 2, 'Smith')])

In this example, fields ``1`` and ``2`` are assigned new values.

The :meth:`Space.update() <tarantool.space.Space.update>` method allows changing
multiple fields of the tuple at a time.

Tarantool supports the following update operations:
    • ``'='`` – assign new value to the field
    • ``'+'`` – add argument to the field (*both arguments are treated as signed 32-bit ints*)
    • ``'^'`` – bitwise AND (*only for 32-bit integers*)
    • ``'|'`` – bitwise XOR (*only for 32-bit integers*)
    • ``'&'`` – bitwise OR  (*only for 32-bit integers*)
    • ``'splice'`` – implementation of `Perl splice <http://perldoc.perl.org/functions/splice.html>`_


.. note:: The 0th field of the tuple cannot be updated, because it is the primary key.

.. seealso:: See :meth:`Space.update() <tarantool.space.Space.update>` documentation for details.

.. warning:: The ``'splice'`` operation is not implemented yet.


Selecting records
^^^^^^^^^^^^^^^^^

To select records, use the :meth:`Space.select() <tarantool.space.Space.select>` method.
A *SELECT* query can return one or many records.


.. rubric:: Select by primary key

Select a record using its primary key, ``3800``::

    >>> world.select(3800)
    [(3800, u'USA', u'Texas', u'Dallas', 1188580)]


.. rubric:: Select by a secondary index

::

    >>> world.select('USA', index=1)
    [(3796, u'USA', u'Texas', u'Houston', 1953631),
     (3801, u'USA', u'Texas', u'Huston', 10000),
     (3802, u'USA', u'California', u'Los Angeles', 10000),
     (3805, u'USA', u'California', u'San Francisco', 776733),
     (3800, u'USA', u'Texas', u'Dallas', 1188580),
     (3794, u'USA', u'California', u'Los Angeles', 3694820)]


The argument ``index=1`` indicates that a secondary index (``1``) should be used.
The primary key (``index=0``) is used by default.

.. note:: Secondary indexes must be explicitly declared in the server configuration.


.. rubric:: Select by several keys

.. note:: This conforms to ``where key in (k1, k2, k3...)``.

Select records with primary key values ``3800``, ``3805`` and ``3796``::

    >>> world.select([3800, 3805, 3796])
    [(3800, u'USA', u'Texas', u'Dallas', 1188580),
     (3805, u'USA', u'California', u'San Francisco', 776733),
     (3796, u'USA', u'Texas', u'Houston', 1953631)]


.. rubric:: Retrieve a record by using a composite index

Select data on cities in Texas::

    >>> world.select([('USA', 'Texas')], index=1)
    [(3800, u'USA', u'Texas', u'Dallas', 1188580), (3796, u'USA', u'Texas', u'Houston', 1953631)]


.. rubric:: Select records by explicitly specifying field types

Tarantool has no strict schema, so all fields are raw binary byte arrays.
You can specify field types in the ``schema`` parameter of the connection.

Call server-side functions
--------------------------

A server-side function written in Lua can select and modify data,
access configuration, and perform administrative tasks.

To call a stored function, use the
:meth:`Connection.call() <tarantool.connection.Connection.call>` method.
(This method has an alias, :meth:`Space.call() <tarantool.space.Space.call>`.)

Example::

    >>> server.call("box.select_range", (1, 3, 2, 'AAAA'))
    [(3800, u'USA', u'Texas', u'Dallas', 1188580), (3794, u'USA', u'California', u'Los Angeles', 3694820)]

.. seealso::

    Tarantool documentation » `Insert one million tuples with a Lua stored procedure <https://www.tarantool.io/en/doc/latest/tutorials/lua_tutorials/#insert-one-million-tuples-with-a-lua-stored-procedure>`_
