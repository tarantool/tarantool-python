.. encoding: utf-8

Python client library for Tarantool
===================================

:Version: |version|

`Tarantool`_ is an in-memory computing platform originally designed by 
`VK`_ and released under the terms of `BSD license`_.

Install Tarantool Python connector with ``pip`` (`PyPI`_ page):

.. code-block:: bash

     $ pip3 install tarantool

Otherwise, you can install ``python3-tarantool`` RPM package if you use Fedora (34, 35, 36).

Add the repository

.. code-block:: bash

     $ curl -L https://tarantool.io/OtKysgx/release/2/installer.sh | bash

and then install the package

.. code-block:: bash

     $ dnf install -y python3-tarantool

Otherwise, you can install ``python3-tarantool`` deb package if you use Debian (10, 11)
or Ubuntu (20.04, 22.04).

Add the repository

.. code-block:: bash

     $ curl -L https://tarantool.io/OtKysgx/release/2/installer.sh | bash

and then install the package

.. code-block:: bash

     $ apt install -y python3-tarantool

Source code is available on `GitHub`_.

Documentation
-------------
.. toctree::
   :maxdepth: 1

   quick-start
   dev-guide

.. seealso:: `Tarantool documentation`_

API Reference
-------------
.. toctree::
   :maxdepth: 2

   api/module-tarantool.rst
   api/submodule-connection.rst
   api/submodule-connection-pool.rst
   api/submodule-crud.rst
   api/submodule-dbapi.rst
   api/submodule-error.rst
   api/submodule-mesh-connection.rst
   api/submodule-msgpack-ext.rst
   api/submodule-msgpack-ext-types.rst
   api/submodule-request.rst
   api/submodule-response.rst
   api/submodule-schema.rst
   api/submodule-space.rst
   api/submodule-types.rst
   api/submodule-utils.rst

.. Indices and tables
.. ==================
.. 
.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`

.. _`Tarantool`:
.. _`Tarantool homepage`: https://tarantool.io
.. _`Tarantool documentation`: https://www.tarantool.io/en/doc/latest/
.. _`VK`: https://vk.company
.. _`BSD`:
.. _`BSD license`: http://www.gnu.org/licenses/license-list.html#ModifiedBSD
.. _`PyPI`: http://pypi.python.org/pypi/tarantool
.. _`GitHub`: https://github.com/tarantool/tarantool-python
