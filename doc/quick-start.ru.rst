Краткое руководство
===================

Подключение к серверу
---------------------

Создаем подключение к серверу::

    >>> import tarantool
    >>> server = tarantool.connect("localhost", 33013)


Создаем объект доступа к спейсу
-------------------------------

Экземпляр :class:`~tarantool.space.Space` — это именованный объект для доступа
к спейсу ключей.

Создаем объект ``demo``, который будет использоваться для доступа к спейсу ``cool_space``::

    >>> demo = server.space(cool_space)

Все последующие операции с ``cool_space`` выполняются при помощи методов объекта ``demo``.


Работа с данными
----------------

Select
^^^^^^

Извлечь одну запись с id ``'AAAA'`` из спейса ``demo``
по первичному ключу (нулевой индекс)::

    >>> demo.select('AAAA')

Извлечь несколько записей, используя первичный индекс::

    >>> demo.select(['AAAA', 'BBBB', 'CCCC'])
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo'), ('CCCC', 'Charlie')]


Insert
^^^^^^

Вставить кортеж ``('DDDD', 'Delta')`` в спейс ``demo``::

    >>> demo.insert(('DDDD', 'Delta'))

Первый элемент является первичным ключом для этого кортежа.


Update
^^^^^^

Обновить запись с id ``'DDDD'``, поместив значение ``'Denver'`` 
в поле ``1``::

    >>> demo.update('DDDD', [(1, '=', 'Denver')])
    [('DDDD', 'Denver')]

Для поиска записи :meth:`~tarantool.space.Space.update` всегда использует
первичный индекс.
Номера полей начинаются с нуля.
Таким образом, поле ``0`` — это первый элемент кортежа. 


Delete
^^^^^^

Удалить одиночную запись с идентификатором ``'DDDD'``::

    >>> demo.delete('DDDD')
    [('DDDD', 'Denver')]

Для поиска записи :meth:`~tarantool.space.Space.delete` всегда использует 
первичный индекс.


Вызов хранимых функций
----------------------

Для вызова хранимых функций можно использовать метод 
:meth:`Connection.call() <tarantool.connection.Connection.call()>`::

    >>> server.call("box.select_range", (0, 0, 2, 'AAAA'))
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo')]

То же самое можно получить при помощи метода
:meth:`Space.call() <tarantool.space.Space.call()>`::

    >>> demo.call("box.select_range", (0, 0, 2, 'AAAA'))
    [('AAAA', 'Alpha'), ('BBBB', 'Bravo')]

Метод :meth:`Space.call() <tarantool.space.Space.call()>` — это просто
псевдоним для
:meth:`Connection.call() <tarantool.connection.Connection.call()>` 
