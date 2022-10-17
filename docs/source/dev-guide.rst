.. encoding: utf-8

Developer's guide
=================

Tarantool database basic concepts
---------------------------------

To understand, what is "space", "tuple" and what basic operations are,
refer to `Tarantool data model documentation`_.

Field types
-----------

Tarantool uses `MessagePack`_ as a format for receiving requests and sending
responses. Refer to `Lua versus MessagePack`_ to see how types are encoded
and decoded.

While working with Tarantool from Python with this connector,
each request data is encoded to MessagePack and each response data
is decoded from MessagePack with the `Python MessagePack`_ module. See its
documentation to explore how basic types are encoded and decoded.

There are several cases when you may tune up the behavior.
Use :class:`tarantool.Connection` parameters to set Python MessagePack
module options.

Use :paramref:`~tarantool.Connection.params.encoding` to tune
behavior for string encoding.

``encoding='utf-8'`` (default):

    +--------------+----+----------------------------------+----+--------------+
    | Python       | -> | MessagePack (Tarantool/Lua)      | -> | Python       |
    +==============+====+==================================+====+==============+
    | :obj:`str`   | -> | `mp_str`_ (``string``)           | -> | :obj:`str`   |
    +--------------+----+----------------------------------+----+--------------+
    | :obj:`bytes` | -> | `mp_bin`_ (``binary``/``cdata``) | -> | :obj:`bytes` |
    +--------------+----+----------------------------------+----+--------------+

``encoding=None`` (work with non-UTF8 strings):

    +--------------+----+----------------------------------+----+--------------+
    | Python       | -> | MessagePack (Tarantool/Lua)      | -> | Python       |
    +==============+====+==================================+====+==============+
    | :obj:`bytes` | -> | `mp_str`_ (``string``)           | -> | :obj:`bytes` |
    +--------------+----+----------------------------------+----+--------------+
    | :obj:`str`   | -> | `mp_str`_ (``string``)           | -> | :obj:`bytes` |
    +--------------+----+----------------------------------+----+--------------+
    |              | -> | `mp_bin`_ (``binary``/``cdata``) | -> | :obj:`bytes` |
    +--------------+----+----------------------------------+----+--------------+

Use :paramref:`~tarantool.Connection.params.use_list` to tune
behavior for `mp_array`_ (Lua ``table``) decoding.

``use_list='True'`` (default):

    +--------------+----+-----------------------------+----+--------------+
    | Python       | -> | MessagePack (Tarantool/Lua) | -> | Python       |
    +==============+====+=============================+====+==============+
    | :obj:`list`  | -> | `mp_array`_ (``table``)     | -> | :obj:`list`  |
    +--------------+----+-----------------------------+----+--------------+
    | :obj:`tuple` | -> | `mp_array`_ (``table``)     | -> | :obj:`list`  |
    +--------------+----+-----------------------------+----+--------------+

``use_list='False'``:

    +--------------+----+-----------------------------+----+--------------+
    | Python       | -> | MessagePack (Tarantool/Lua) | -> | Python       |
    +==============+====+=============================+====+==============+
    | :obj:`list`  | -> | `mp_array`_ (``table``)     | -> | :obj:`tuple` |
    +--------------+----+-----------------------------+----+--------------+
    | :obj:`tuple` | -> | `mp_array`_ (``table``)     | -> | :obj:`tuple` |
    +--------------+----+-----------------------------+----+--------------+

Tarantool implements several `extension types`_. In Python,
they are represented with in-built and custom types:

    +-----------------------------+----+-------------+----+-----------------------------+
    | Python                      | -> | Tarantool   | -> | Python                      |
    +=============================+====+=============+====+=============================+
    | :obj:`decimal.Decimal`      | -> | `DECIMAL`_  | -> | :obj:`decimal.Decimal`      |
    +-----------------------------+----+-------------+----+-----------------------------+
    | :obj:`uuid.UUID`            | -> | `UUID`_     | -> | :obj:`uuid.UUID`            |
    +-----------------------------+----+-------------+----+-----------------------------+
    | :class:`tarantool.BoxError` | -> | `ERROR`_    | -> | :class:`tarantool.BoxError` |
    +-----------------------------+----+-------------+----+-----------------------------+
    | :class:`tarantool.Datetime` | -> | `DATETIME`_ | -> | :class:`tarantool.Datetime` |
    +-----------------------------+----+-------------+----+-----------------------------+
    | :class:`tarantool.Interval` | -> | `INTERVAL`_ | -> | :class:`tarantool.Interval` |
    +-----------------------------+----+-------------+----+-----------------------------+

Request response
----------------

Server requests (except for :meth:`~tarantool.Connection.ping`)
return :class:`~tarantool.response.Response` instance in case
of success.

:class:`~tarantool.response.Response` is inherited from
:class:`collections.abc.Sequence`, so you can index response data
and iterate through it as with any other serializable object.

.. _Tarantool data model documentation: https://www.tarantool.io/en/doc/latest/concepts/data_model/
.. _MessagePack: https://msgpack.org/
.. _Lua versus MessagePack: https://www.tarantool.io/en/doc/latest/concepts/data_model/value_store/#lua-versus-msgpack
.. _Python MessagePack: https://pypi.org/project/msgpack/
.. _mp_str: https://github.com/msgpack/msgpack/blob/master/spec.md#str-format-family
.. _mp_bin: https://github.com/msgpack/msgpack/blob/master/spec.md#bin-format-family
.. _mp_array: https://github.com/msgpack/msgpack/blob/master/spec.md#array-format-family
.. _extension types: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/
.. _DECIMAL: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-decimal-type
.. _UUID: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-uuid-type
.. _ERROR: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-error-type
.. _DATETIME: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-datetime-type
.. _INTERVAL: https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-interval-type
