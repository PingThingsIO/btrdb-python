Server Connection and Info
==============================

There are a number of options available when connecting to a BTrDB server or
server cluster.  First, you will need to identify the appropriate IP or FQDN to
use as well as the access port.

By default BTrDB servers expose port 4410 for unencrypted access and 4411 for
encrypted access using TLS.  You may also opt for authentication using an API key
which can be provided to you by the BTrDB server administrators.  Using such a
key will require the TLS port (4411) and attempting to use a different port with
an API key will raise an exception.

Connecting to servers
---------------------------

The btrdb library comes with a high level :code:`connnect` function to interface
with a BTrDB server.  Upon successfully connecting, you will be returned a
:code:`BTrDB` object which is the starting point for all of your server
interactions.

For your convenience, you may default all connection parameters to environment
variables if these are configured on your system.  If no arguments are provided, the
:code:`btrdb.connect` function will attempt to connect using the
:code:`BTRDB_ENDPOINTS` and :code:`BTRDB_API_KEY` environment variables.  If
these cannot be found then a connection error will be raised.

Several connection options are shown in the code below:

.. code-block:: python

    import btrdb

    # connect using BTRDB_ENDPOINTS and BTRDB_API_KEY ENV variables
    conn = btrdb.connect()

    # connect without credentials
    conn = btrdb.connect("192.168.1.101:4410")

    # connect without credentials using TLS
    conn = btrdb.connect("192.168.1.101:4411")

    # connect with API key
    conn = btrdb.connect("192.168.1.101:4411", apikey="123456789123456789")



Viewing server status
---------------------------

Server version and connection information can be viewed by calling the :code:`info`
method of the server object as shown below.

.. code-block:: python

    conn = btrdb.connect()
    conn.info()
    >> majorVersion: 5
      build: "5.0.0"
      proxy {
          proxyEndpoints: "localhost:4410"
      }