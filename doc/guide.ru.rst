.. encoding: utf-8

Руководство разработчика
========================

Базовые понятия
---------------

Пространства
^^^^^^^^^^^^

Пространства в Tarantool — это коллекции кортежей.
Как правило, кортежи в пространстве представляют собой объекты одного типа,
хотя это и не обязательно.

.. note:: Аналог пространства — это таблица в традиционных (SQL) базах данных.

Пространства имеют целочисленные идентификаторы, которые задаются в конфигурации сервера.
Чтобы обращаться к пространству, как к именованному объекту, можно использовать метод
:meth:`Connection.space() <tarantool.connection.Connection.space>`
и экземпляр класса :class:`~tarantool.space.Space`.

Пример::

    >>> customer = connection.space(0)
    >>> customer.insert(('FFFF', 'Foxtrot'))


Типы полей
^^^^^^^^^^

Tarantool поддерживает три типа полей: ``STR``, ``NUM`` и ``NUM64``.
Эти типы используются только при конфигурации индексов,
но не сохраняются с данными кортежа и не передаются между сервером и клиентом.
Таким образом, с точки зрения клиента, поля кортежей являются просто байтовыми массивами
без явно заданных типов.

Для разработчика на Python намного удобнее использовать родные типы:
``int``, ``long``, ``unicode`` (для Python 3.x  - ``int`` и ``str``).
Для бинарных данных следует использовать тип ``bytes``
(в этом случае приведение типов не производится).

Для автоматического приведения типов необходимо передать типы полей в параметре ``field_types``::

    >>> demo = connection.space(0, field_types=(int, unicode))
    >>> demo.insert((0, u'this is unicode string'))
    >>> demo.select(0)
    [(0, u'this is unicode string')]

Как видно из примера, в аргументе ``field_types`` передается кортеж типов.
Исходные "сырые" поля будут приводиться к этим типам.

В Tarantool кортеж может содержать произвольное количество полей.

Если полей в кортеже больше, чем перечислено типов в ``field_types``,
то последний тип из ``field_types`` применяется ко всем оставшимся полям кортежа.

Пример::

    >>> demo = connection.space(0, field_types=(int, unicode))
    >>> demo.insert((0, u'I am the Alpha (Α) and Omega (Ω)', b'AAAA', b'BBBB', b'CCCC', b'DDDD'))
    >>> demo.select(0)
    [(0, u'I am the Α and Ω', u'AAAA', u'BBBB', u'CCCC', u'DDDD')]

Как видно из примера, все значения были преобразованы в unicode-строки.

Чтобы запретить такое неявное преобразование, достаточно в кортеже типов ``field_types``
последним указать тип ``bytes``::

    >>> demo = connection.space(0, field_types=(int, unicode, bytes))
    >>> demo.insert((0, u'I am the Alpha (Α) and Omega (Ω)', b'AAAA', b'BBBB', b'CCCC', b'DDDD'))
    >>> demo.select(0)
    [(0, u'I am the Α and Ω', 'AAAA', 'BBBB', 'CCCC', 'DDDD')]


Поля с "сырыми" байтами следует использовать, если приложение работает с
двоичными данными (например, изображения или python-объекты, сохраненные с помощью ``picke``).


.. note::

   Python 2.6 adds :class:`bytes` as a synonym for the :class:`str` type, and it also supports the ``b''`` notation.


.. note:: Для преобразования между ``bytes`` и ``unicode`` всегда используется **utf-8**



Результат запроса
^^^^^^^^^^^^^^^^^

Запросы (:meth:`insert() <tarantool.space.Space.insert>`,
:meth:`delete() <tarantool.space.Space.delete>`,
:meth:`update() <tarantool.space.Space.update>`,
:meth:`select() <tarantool.space.Space.select>`) возвращают экземпляр
класса :class:`~tarantool.response.Response`.

Класс :class:`~tarantool.response.Response` унаследован от стандартного типа `list`,
поэтому, по сути, результат всегда представляет собой список кортежей.

Кроме того, у экземпляра :class:`~tarantool.response.Response` есть атрибут ``rowcount``.
Этот атрибут содержит число записей, которые затронул запроc.
Например, для запроса :meth:`delete() <tarantool.space.Space.delete>`
``rowcount`` равен ``1``, если запись была удалена.



Подключение к серверу
---------------------

Для подключения к серверу следует использовать метод :meth:`tarantool.connect`.
Он возвращает экземпляр класса :class:`~tarantool.connection.Connection`.

Пример::

    >>> import tarantool
    >>> connection = tarantool.connect("localhost", 33013)
    >>> type(connection)
    <class 'tarantool.connection.Connection'>



Работа с данными
----------------

Tarantool поддерживает четыре базовых операции:
**insert**, **delete**, **update** и **select**.

.. Note:: НЕОБХОДИМО ОБЪЯСНИТЬ КАКИЕ ДАННЫЕ ИСПОЛЬЗУЮТСЯ ДЛЯ ПРИМЕРА


Добавление и замещение записей
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Для добавления и замещения записей следует использовать метод
:meth:`Space.insert() <tarantool.space.Space.insert>`::

    >>> user.insert((user_id, email, int(time.time())))

Первый элемент кортежа всегда является его уникальным первичным ключом.

Если запись с таким ключом уже существует, она будет замещена
без какого либо предупреждения или сообщения об ошибке.

.. note:: Для :meth:`Space.insert() <tarantool.space.Space.insert>` ``Response.rowcount`` всегда равен ``1``.


Удаление записей
^^^^^^^^^^^^^^^^

Для удаления записей следует использовать метод
:meth:`Space.delete() <tarantool.space.Space.delete>`::

    >>> user.delete(primary_key)

.. note:: ``Response.rowcount`` равен ``1``, если запись была удалена.
          Если запись не найдена, то ``Response.rowcount`` равен ``0``.


Обновление записей
^^^^^^^^^^^^^^^^^^

Запрос *update* в Tarantool позволяет одновременно и атомарно обновить несколько
полей одного кортежа.

Для обновления записей следует использовать метод
:meth:`Space.update() <tarantool.space.Space.update>`.

Пример::

    >>> user.update(1001, [(1, '=', 'John'), (2, '=', 'Smith')])

В этом примере для полей ``1`` и ``2`` устанавливаются новые значения.

Метод :meth:`Space.update() <tarantool.space.Space.update>` позволяет обновлять
сразу несколько полей кортежа.

Tarantool поддерживает следующие операции обновления:
    • ``'='`` – установить новое значение поля
    • ``'+'`` – прибавить аргумент к значению поля (*оба аргумента рассматриваются как знаковые 32-битные целые числа*)
    • ``'^'`` – битовый AND (*только для 32-битных полей*)
    • ``'|'`` – битовый XOR (*только для 32-битных полей*)
    • ``'&'`` – битовый OR  (*только для 32-битных полей*)
    • ``'splice'`` – аналог функции `splice в Perl <http://perldoc.perl.org/functions/splice.html>`_


.. note:: Нулевое (т.е. [0]) поле кортежа нельзя обновить,
          поскольку оно является первичным ключом

.. seealso:: Подробности в документации по методу :meth:`Space.update() <tarantool.space.Space.update>`

.. warning:: Операция ``'splice'`` пока не реализована


Выборка записей
^^^^^^^^^^^^^^^

Для выборки записей следует использовать метод
:meth:`Space.select() <tarantool.space.Space.select>`.
Запрос *SELECT* может возвращать одну или множество записей.


.. rubric:: Запрос по первичному ключу

Извлечь запись по её первичному ключу ``3800``::

    >>> world.select(3800)
    [(3800, u'USA', u'Texas', u'Dallas', 1188580)]


.. rubric:: Запрос по вторичному индексу

::

    >>> world.select('USA', index=1)
    [(3796, u'USA', u'Texas', u'Houston', 1953631),
     (3801, u'USA', u'Texas', u'Huston', 10000),
     (3802, u'USA', u'California', u'Los Angeles', 10000),
     (3805, u'USA', u'California', u'San Francisco', 776733),
     (3800, u'USA', u'Texas', u'Dallas', 1188580),
     (3794, u'USA', u'California', u'Los Angeles', 3694820)]


Аргумент ``index=1`` указывает, что при запросе следует использовать индекс ``1``.
По умолчанию используется первыичный ключ (``index=0``).

.. note:: Вторичные индексы должны быть явно объявлены в конфигурации севера


.. rubric:: Запрос записей по нескольким ключам

.. note:: Это аналог ``where key in (k1, k2, k3...)``

Извлечь записи со значениями первичного ключа ``3800``, ``3805`` and ``3796``::

    >>>> world.select([3800, 3805, 3796])
    [(3800, u'USA', u'Texas', u'Dallas', 1188580),
     (3805, u'USA', u'California', u'San Francisco', 776733),
     (3796, u'USA', u'Texas', u'Houston', 1953631)]


.. rubric:: Запрос по составному индексу

Извлечь данные о городах в Техасе::

    >>> world.select([('USA', 'Texas')], index=1)
    [(3800, u'USA', u'Texas', u'Dallas', 1188580), (3796, u'USA', u'Texas', u'Houston', 1953631)]


.. rubric:: Запрос с явным указанием типов полей

Tarantool не имеет строгой схемы и поля кортежей являются просто байтовыми массивами.
Можно указать типа полей непосредственно в
методе :meth:`Space.select() <tarantool.space.Space.select>`
при помощи ``field_types`` keyword argument::

    >>> world.select(3800, field_types=(bytes, str, str, str, bytes))
    [('\xd8\x0e\x00\x00', 'USA', 'Texas', 'Dallas', '\xe4"\x12\x00')]

Как видно из примера, значение ``3800`` возвращается
в виде 4-байтного массива (строки) вместо целого значения.


Вызов хранимых функций
----------------------

Хранимые процедуры на Lua могут делать выборки и изменять данные,
имеют доcтуп к конфигурации и могут выполнять административные функции.

Для вызова хранимых функций следует использовать метод
:meth:`Connection.call() <tarantool.connection.Connection.call>`.
Кроме того, у этого метода есть псевдоним: :meth:`Space.call() <tarantool.space.Space.call>`.

Пример::

    >>> server.call("box.select_range", (1, 3, 2, 'AAAA'))
    [(3800, u'USA', u'Texas', u'Dallas', 1188580), (3794, u'USA', u'California', u'Los Angeles', 3694820)]

.. seealso::

    Tarantool/Box User Guide » `Writing stored procedures in Lua <http://tarantool.org/tarantool_user_guide.html#stored-programs>`_