Краткое руководство
===================

Подключение к серверу
---------------------

Создаем подключение к серверу::

    >>> import tarantool
    >>> server = tarantool.connect("localhost", 33013)


Создаем объект доступа к пространству
-------------------------------------

Экземпляр :class:`~tarantool.space.Space` - это именованный объект для доступа
к пространству ключей.

Создаем объект ``demo``, который будет использоваться для доступа к пространству ``0``::

    >>> demo = server.space(0)

Все последующие операции с пространством ``0`` выполняются при помощи методов объекта ``demo``.


Работа с данными
----------------

Select
^^^^^^

Извлечь одну запись с id ``'AAAA'`` из пространства ``demo``
по первичному ключу (нулевой индекс)::

    >>> demo.select('AAAA')

Извлечь несколько записей используя первичный индекс::

    >>> demo.select(['AAAA', 'BBBB', 'CCCC'])
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo'), ('CCCC', 'Charlie')]


Insert
^^^^^^

Вставить кортеж ``('DDDD', 'Delta')`` в пространство ``demo``::

    >>> demo.insert(('DDDD', 'Delta'))

Первый элемент является первичным ключом для данного кортежа.


Update
^^^^^^

Обновить запись с id ``'DDDD'``, поместив значение ``'Denver'`` 
в поле ``1``::

    >>> demo.update('DDDD', [(1, '=', 'Denver')])
    [('DDDD', 'Denver')]

Для поиска записи :meth:`~tarantool.space.Space.update` всгеда использует
первичный индекс.
Номера полей начинаются с нуля.
Таким образом, поле ``0`` - это первый элемент кортежа. 


Delete
^^^^^^

Удалить одиночную запись с идентификатором ``'DDDD'``::

    >>> demo.delete('DDDD')
    [('DDDD', 'Denver')]

Для поиска записи :meth:`~tarantool.space.Space.delete` всгеда использует 
первичный индекс.


Вызов хранимых функций
----------------------

Для вызова хранимых функций можно использовать метод 
:meth:`Connection.call() <tarantool.connection.Connection.call()>`::

    >>> server.call("box.select_range", (0, 0, 2, 'AAAA'))
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo')]

Тоже самое можно получить при помощи метода
:meth:`Space.call() <tarantool.space.Space.call()>`::

    >>> demo.call("box.select_range", (0, 0, 2, 'AAAA'))
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo')]

Метод :meth:`Space.call() <tarantool.space.Space.call()>` - это просто
псевдоним для
:meth:`Connection.call() <tarantool.connection.Connection.call()>` 
