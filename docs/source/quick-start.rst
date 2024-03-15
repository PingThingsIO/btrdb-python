========================
Quick Start
========================

Connecting to a server
----------------------

Connecting to a server is easy with the supplied :code:`connect` function from the btrdb package.

.. code-block:: python

    >>> import btrdb
    >>> # connect with API key
    >>> conn = btrdb.connect("192.168.1.101:4411", apikey="123456789123456789")
    >>> conn
    <btrdb.conn.BTrDB at 0x...>

Get Platform Information
^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: python

    >>> conn.info()
    {'majorVersion': ...,
     'minorVersion': ...,
     'build': ...,
     'proxy': {...}}


Refer to :ref:`the connection API documentation page. <Conn info>`


Retrieving a Stream
----------------------

In order to interact with data, you'll need to obtain or create a :code:`Stream` object.
A number of options are available to get existing streams.

Find streams by collection
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Multiple streams are often organized under a single collection which is similar
to the concept of a directory path.  To search for all streams under a given
collection you can use the :code:`streams_in_collection` method.

.. code-block:: python

    >>> streams = conn.streams_in_collection("USEAST_NOC1/90807")
    >>> for stream in streams:
    >>>     print(stream.uuid, stream.name)

Find stream by UUID
^^^^^^^^^^^^^^^^^^^^^
A method has also been provided if you already know the UUID of a single stream you
would like to retrieve. For convenience, this method accepts instances of either
:code:`str` or :code:`UUID`.

.. code-block:: python

    >>> stream = conn.stream_from_uuid("07d28a44-4991-492d-b9c5-2d8cec5aa6d4")



Viewing a Stream's Data
------------------------

To view data within a stream, you'll need to specify a time range to query for as
well as a version number (defaults to latest version).  Remember that BTrDB
stores data to the nanosecond and so Unix timestamps will need to be converted
if needed.

.. code-block:: python

    >>> start = datetime(2018,1,1,12,30, tzinfo=timezone.utc)
    >>> start = start.timestamp() * 1e9
    >>> end = start + (3600 * 1e9)

    >>> for point, _ in stream.values(start, end):
    >>>   print(point.time, point.value)

Some convenience functions are available to make it easier to deal with
converting to nanoseconds.

.. code-block:: python

    >>> from btrdb.utils.timez import to_nanoseconds, currently_as_ns

    >>> start = to_nanoseconds(datetime(2018,1,1, tzinfo=timezone.utc))
    >>> end = currently_as_ns()

    >>> for point, _ in stream.values(start, end):
    >>>   print(point.time, point.value)

You can also view windows of data at arbitrary levels of detail.  One such
windowing feature is shown below.

.. code-block:: python

    >>> # query for windows of data 10,000 nanoseconds wide using a depth of zero
    >>> # which is accurate to the nanosecond.
    >>> params = {
    ...     "start": 1500000000000000000,
    ...     "end": 1500000000010000000,
    ...     "width": 2000000,
    ...     "depth": 0,
    ... }
    >>> for window in stream.windows(**params):
    >>>     for point, version in window:
    >>>         print(point, version)


Return data as :code:`arrow` tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Instead of returning data a :code:`RawPoint` at a time, which can be more computationally intensive, there is now the ability to return the data in a tabular format from the start, which can drastically save on run time as well as facilitate interoperability with many more data-science driven tools.
`Apache Arrow is a language agnostic columnar data schema <https://arrow.apache.org/>`_ that has become a defacto standard for in-memory data analytics.
All data retrieval methods in :code:`BTrDB` now have corresponding :code:`arrow-` prepended methods that natively return :code:`pyarrow` data tables.

.. code-block:: python

    >>> s.arrow_values(start=1500000000000000000, end=1500000002000000001).to_pandas()
                                      time  value
        0        2017-07-14 02:40:00+00:00    1.0
        1 2017-07-14 02:40:00.100000+00:00    2.0
        2 2017-07-14 02:40:00.200000+00:00    3.0
        3 2017-07-14 02:40:00.300000+00:00    4.0
        4 2017-07-14 02:40:00.400000+00:00    5.0
        5 2017-07-14 02:40:00.500000+00:00    6.0
        6 2017-07-14 02:40:00.600000+00:00    7.0
        7 2017-07-14 02:40:00.700000+00:00    8.0
        8 2017-07-14 02:40:00.800000+00:00    9.0
        9 2017-07-14 02:40:00.900000+00:00   10.0


Using StreamSets
--------------------
A :code:`StreamSet` is a wrapper around a list of :code:`Stream` objects with a
number of convenience methods available.  Future updates will allow you to
query for streams using a SQL-like syntax but for now you will need to provide
a list of UUIDs.

The StreamSet allows you to interact with a group of streams rather than at the
level of the individual :code:`Stream` object.  Aside from being useful to see
concurrent data across streams, you can also easily transform the data to other
data structures or even serialize the data to disk in one operation.

Some quick examples are shown below but please review the :ref:`API docs <API REF>` for the full
list of features.


.. note::

    In the following examples, notice that the end time is **not** inclusive of the data that is present at :code:`end` . :code:`start` is **inclusive** while :code:`end` is **exclusive**. This is the case for **all** :code:`BTrDB` data query operations.

    .. math::
        [start, end)


.. code-block:: python

    >>> streams = db.streams(*uuid_list)

    >>> # serialize data to disk as CSV
    >>> streams.filter(start=1500000000000000000, end=1500000000900000000).to_csv("data.csv")

    >>> # convert data to a pandas DataFrame
    >>> streams.filter(start=1500000000000000000, end=1500000000900000000).to_dataframe()
                         nw/stream0  nw/stream1
    time
    1500000000000000000         nan         1.0
    1500000000100000000         2.0         nan
    1500000000200000000         nan         3.0
    1500000000300000000         4.0         nan
    1500000000400000000         nan         5.0
    1500000000500000000         6.0         nan
    1500000000600000000         nan         7.0
    1500000000700000000         8.0         nan
    1500000000800000000         nan         9.0




    >>> # materialize the streams' data
    >>> streams.filter(start=1500000000000000000,  end=1500000000900000000).values()
    [[RawPoint(1500000000100000000, 2.0),
        RawPoint(1500000000300000000, 4.0),
        RawPoint(1500000000500000000, 6.0),
        RawPoint(1500000000700000000, 8.0),
        RawPoint(1500000000900000000, 10.0)],
       [RawPoint(1500000000000000000, 1.0),
        RawPoint(1500000000200000000, 3.0),
        RawPoint(1500000000400000000, 5.0),
        ...






Return data as :code:`arrow` tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:code:`StreamSets` are also able to return :code:`arrow` tables for the group of streams they represent.
This is especially convenient and is usually **much** faster than using the traditional :code:`RawPoint` -based data representation.
We recommend using the :code:`arrow` functions whenever possible.

.. code-block:: python

    >>> # convert data to a pandas DataFrame, using pyarrow
    >>> streams.filter(start=1500000000000000000, end=1500000000900000000)
    ...        .arrow_to_dataframe()
                                      NW/stream0  NW/stream1
    time
    2017-07-14 02:40:00+00:00                NaN         1.0
    2017-07-14 02:40:00.100000+00:00         2.0         NaN
    2017-07-14 02:40:00.200000+00:00         NaN         3.0
    2017-07-14 02:40:00.300000+00:00         4.0         NaN
    2017-07-14 02:40:00.400000+00:00         NaN         5.0
    2017-07-14 02:40:00.500000+00:00         6.0         NaN
    2017-07-14 02:40:00.600000+00:00         NaN         7.0
    2017-07-14 02:40:00.700000+00:00         8.0         NaN
    2017-07-14 02:40:00.800000+00:00         NaN         9.0


    >>> # materialize the streams' data as an arrow table
    >>> streams.filter(start=1500000000000000000, end=1500000000900000000).arrow_values()
        pyarrow.Table
        time: timestamp[ns, tz=UTC] not null
        b29204f4-6c13-4ec7-a149-88e2ff950a72: double not null
        99a0d0b0-e24f-4875-b7d8-eae0036f2149: double not null
        ----
        time: [
        ... [2017-07-14 02:40:00.000000000Z,2017-07-14 02:40:00.100000000Z,
        ... 2017-07-14 02:40:00.200000000Z,2017-07-14 02:40:00.300000000Z,
        ... 2017-07-14 02:40:00.400000000Z,2017-07-14 02:40:00.500000000Z,
        ... 2017-07-14 02:40:00.600000000Z,2017-07-14 02:40:00.700000000Z,
        ... 2017-07-14 02:40:00.800000000Z]]
        b29204f4-6c13-4ec7-a149-88e2ff950a72: [[nan,2,nan,4,nan,6,nan,8,nan]]
        99a0d0b0-e24f-4875-b7d8-eae0036f2149: [[1,nan,3,nan,5,nan,7,nan,9]]
