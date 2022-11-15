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


Receiving out-of-band messages
----------------------------------

Receiving out-of-band messages from a server that uses box.session.push 
call is supported for methods: :meth:`~tarantool.Connection.call`, 
:meth:`~tarantool.Connection.eval`, :meth:`~tarantool.Connection.select`, 
:meth:`~tarantool.Connection.insert`, :meth:`~tarantool.Connection.replace`, 
:meth:`~tarantool.Connection.update`, :meth:`~tarantool.Connection.upsert`, 
:meth:`~tarantool.Connection.delete`.

To work with out-of-band messages, 2 optional arguments are used in 
the methods listed above:

 * `on_push` - callback, launched with the received data for each out-of-band message. Two arguments for this callback are expected:
   
    * the first is the received from an out-of-band message data.

    * the second is `on_push_ctx`, variable for working with callback context (for example, recording the result or pass data to callback).
 * `on_push_ctx` - result of the `on_push` work can be written to this variable, or through this variable you can pass data to `on_push` callback.

Below is an example of the proposed API with method :meth:`~tarantool.Connection.call` 
and :meth:`~tarantool.Connection.insert`. In the described example, before the end 
of the :meth:`~tarantool.Connection.call` and :meth:`~tarantool.Connection.insert`, 
out-of-band messages are processed via specified callback.

In the example below, two shells are used, in the first we will configure the server:

.. code-block:: lua

    fiber = require('fiber')
    box.cfg({listen = 3301})
    box.schema.user.grant(
        'guest',
        'read,write,execute',
        'universe'
    )
    function server_function()
        x = {0,0}
        while x[1] < 3 do
            x[1] = x[1] + 1
            fiber.sleep(1)
            box.session.push(x)
        end
        fiber.sleep(1)
        return x
    end

In the second shell, we will execute a :meth:`~tarantool.Connection.call` 
with receiving out-of-band messages from the server:

.. code-block:: python

    import tarantool

    def callback(data, on_push_ctx=[]):
        print('run callback with data: ', data)
        data[0][1] = data[0][1] + 1
        on_push_ctx.append(data)

    callback_res = []

    conn = tarantool.Connection(port=3301)
    res = conn.call(
        'server_function',
        on_push=callback, 
        on_push_ctx=callback_res
    )

    # receiving out-of-band messages, 
    # the conn.call is not finished yet.

    >>> run callback with data:  [[1, 0]]
    >>> run callback with data:  [[2, 0]]
    >>> run callback with data:  [[3, 0]]

    # the conn.call is finished now.

    print(res)
    >>> [3, 0]

    print(callback_res)
    >>> [[[1, 1]], [[2, 1]], [[3, 1]]]

Let's go back to the first shell with the server and 
create a space and a trigger for it:

.. code-block:: lua

    box.schema.create_space(
     'tester', {
      format = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
      }
    })
    box.space.tester:create_index(
     'primary_index', {
      parts = {
        {field = 1, type = 'unsigned'},
      }
    })
    function on_replace_callback()
        x = {0,0}
        while x[1] < 300 do
            x[1] = x[1] + 100
            box.session.push(x)
        end
        return x
    end
    box.space.tester:on_replace(
        on_replace_callback
    )

Now, in the second shell, we will execute an :meth:`~tarantool.ConnectionPool.insert` 
with out-of-band message processing:

.. code-block:: python

    callback_res = []

    conn_pool = tarantool.ConnectionPool(
            [{'host':'localhost', 'port':3301}],
            user='guest')

    res = conn_pool.insert(
        'tester',
        (1, 'Mike'),
        on_push=callback,
        on_push_ctx=callback_res,
    )

    # receiving out-of-band messages, 
    # the conn_pool.insert is not finished yet.

    >>> run callback with data:  [[100, 0]]
    >>> run callback with data:  [[200, 0]]
    >>> run callback with data:  [[300, 0]]

    # the conn_pool.insert is finished now.

    print(res)
    >>> [1, 'Mike']

    print(callback_res)
    >>> [[[100, 1]], [[200, 1]], [[300, 1]]]


Interaction with the crud module
----------------------------------

Through the :class:`~tarantool.Connection` object, you can access 
`crud module <https://github.com/tarantool/crud>`_ methods:

.. code-block:: python

    >>> import tarantool
    >>> from tarantool.error import CrudModuleError, CrudModuleManyError, DatabaseError
    >>> conn = tarantool.Connection(host='localhost',port=3301)

    >>> conn.crud_
    conn.crud_count(                conn.crud_insert(               conn.crud_insert_object_many(   
    conn.crud_min(                  conn.crud_replace_object(       conn.crud_stats(                
    conn.crud_unflatten_rows(       conn.crud_upsert_many(          conn.crud_delete(               
    conn.crud_insert_many(          conn.crud_len(                  conn.crud_replace(              
    conn.crud_replace_object_many(  conn.crud_storage_info(         conn.crud_update(               
    conn.crud_upsert_object(        conn.crud_get(                  conn.crud_insert_object(        
    conn.crud_max(                  conn.crud_replace_many(         conn.crud_select(               
    conn.crud_truncate(             conn.crud_upsert(               conn.crud_upsert_object_many(

As an example, consider :meth:`~tarantool.Connection.crud_insert` and :meth:`~tarantool.Connection.crud_insert_object_many`. 
It is recommended to enclose calls in the try-except construction as follows:

.. code-block:: python

    # Insert without exception:
    >>> res = conn.crud_insert('tester', (3500,300,'Rob'))
    >>> res
    <tarantool.crud.CrudResult object at 0x11a56e320>
    >>> res.
    res.metadata  res.rows
    >>> res.rows
    [[3500, 300, 'Rob']]
    >>> res.metadata
    [{'name': 'id', 'type': 'unsigned'}, {'name': 'bucket_id', 'type': 'unsigned'}, {'name': 'name', 'type': 'string'}]

    # Insert with exception (duplicate key exists):
    >>> try:
    ...     res = conn.crud_insert('tester', (3500,300,'Rob'))
    ... except CrudModuleError as e:
    ...     exc_crud = e
    ... 
    >>> exc_crud
    CrudModuleError(0, 'Failed to insert: Duplicate key exists in unique index "primary_index" in space "tester" with old tuple - [3500, 300, "Rob"] and new tuple - [3500, 300, "Rob"]')
    >>> exc_crud.extra_info_error
    <tarantool.crud.CrudError object at 0x10a276950>
    >>> exc_crud.extra_info_error.
    exc_crud.extra_info_error.class_name  exc_crud.extra_info_error.err         exc_crud.extra_info_error.file        exc_crud.extra_info_error.line        exc_crud.extra_info_error.str       
    >>> exc_crud.extra_info_error.class_name
    'InsertError'
    >>> exc_crud.extra_info_error.str
    'InsertError: Failed to insert: Duplicate key exists in unique index "primary_index" in space "tester" with old tuple - [3500, 300, "Rob"] and new tuple - [3500, 300, "Rob"]'

    # In case of batch operation (*_many), CrudModuleManyError exception contains both result and errors (if there is a problem with at least one row).
    >>> try:
    ...     res = conn.crud_insert_object_many('tester', ({'id':3,'bucket_id':100,'name':'Ann'}, {'id':4,'bucket_id':100,'name':'Sam'}), {'timeout':100, 'rollback_on_error':False})
    ... except CrudModuleManyError as e:
    ...     exc_crud = e
    ... 
    >>> exc_crud
    CrudModuleManyError(0, 'Got multiple errors, see errors_list')
    >>> exc_crud.success_list # some of the rows were inserted.
    <tarantool.crud.CrudResult object at 0x11a56f310>
    >>> exc_crud.success_list.rows
    [[1, 100, 'Bob'], [2, 100, 'Rob']]
    >>> exc_crud.errors_list # some of the rows were not inserted.
    [<tarantool.crud.CrudError object at 0x11a56e9e0>, <tarantool.crud.CrudError object at 0x11a56f490>]
    >>> exc_crud.errors_list[0].str
    'CallError: Failed for 037adb3a-b9e3-4f78-a6d1-9f0cdb6cbefc: Function returned an error: Duplicate key exists in unique index "primary_index" in space "tester" with old tuple - [3500, 300, "Rob"] and new tuple - [3500, 100, "Mike"]'
    >>> exc_crud.errors_list[1].str
    'InsertManyError: Failed to flatten object: FlattenError: Object is specified in bad format: FlattenError: Unknown field "second_name" is specified'
    
    # If there are no problems with any rows, the entire response will be contained in the res variable.
    >>> res = conn.crud_insert_object_many('tester', ({'id':3,'bucket_id':100,'name':'Ann'}, {'id':4,'bucket_id':100,'name':'Sam'}), {'timeout':100, 'rollback_on_error':False})
    >>> res.rows
    [[3, 100, 'Ann'], [4, 100, 'Sam']]

If module crud not found on the router or user has not sufficient grants:

.. code-block:: python

    >>> try:
    ...     res = conn.crud_insert('tester', (22221,300,'Rob'))
    ... except DatabaseError as e:
    ...     exc_db = e
    ... 
    >>> exc_db
    DatabaseError(33, "Procedure 'crud.insert' is not defined. Ensure that you're calling crud.router and user has sufficient grants")
    >>> exc_db.extra_info
    BoxError(type='ClientError', file='/tmp/tarantool-20221003-6335-edruh3/tarantool-2.10.3/src/box/lua/call.c', line=112, message="Procedure 'crud.insert' is not defined", errno=0, errcode=33, fields=None, prev=None)

Using :meth:`~tarantool.Connection.crud_select` and :meth:`~tarantool.Connection.crud_unflatten_rows`:

.. code-block:: python

    >>> res = conn.crud_select('tester', [], {'first':2})
    >>> res
    <tarantool.crud.CrudResult object at 0x10a276d10>
    >>> res.rows
    [[1, 100, 'Mike'], [2, 100, 'Bill']]
    >>> res.metadata
    [{'name': 'id', 'type': 'unsigned'}, {'name': 'bucket_id', 'type': 'unsigned'}, {'name': 'name', 'type': 'string'}]
    >>> r = conn.crud_unflatten_rows(res.rows, res.metadata)
    >>> r
    [{'id': 1, 'bucket_id': 100, 'name': 'Mike'}, {'id': 2, 'bucket_id': 100, 'name': 'Bill'}]

Using :meth:`~tarantool.Connection.crud_truncate` and :meth:`~tarantool.Connection.crud_len`:

.. code-block:: python

    >>> res = conn.crud_len('tester')
    >>> res
    26
    >>> res = conn.crud_truncate('tester')
    >>> res
    True
