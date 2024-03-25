Installing
========================

The :code:`btrdb` package has only a few requirements and is relatively easy to install.
A number of installation options are available as detailed below.

Installing with pip
-------------------

We recommend using :code:`pip` to install :code:`btrdb-python` on all platforms:

.. code-block:: bash

    $ pip install btrdb

With :code:`btrdb>=5.30.2`, there are now extra dependencies that can be installed with ``pip``.
We recommend installing the :code:`data` extra dependencies (the second option in the code block below).


.. code-block:: shell-session

    $ pip install "btrdb>=5.30.2" # standard btrdb
    $ pip install "btrdb[data]>=5.30.2" # btrdb with data science packages included (recommended)
    $ pip install "btrdb[all]>=5.30.2" # btrdb with testing, data science and all other optional packages


To get a specific version of :code:`btrdb-python` supply the version number.  The major
version of this library is tied to the major version of the BTrDB database as
in the 4.X bindings are best used to speak to a 4.X BTrDB database, the 5.X bindings for 5.X platform..

.. code-block:: bash

    $ pip install "btrdb[data]==5.30.2"


To upgrade using pip:

.. code-block:: bash

    $ pip install --upgrade btrdb


Installing with Anaconda
------------------------

We recommend installing using :code:`pip`.
