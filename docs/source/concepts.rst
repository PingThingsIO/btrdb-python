Concepts
========
If you are relatively new to BTrDB, then there are a few things you should be
aware of about interacting with the server.  First of all, time series databases
such as BTrDB are not relational databases and so they behave differently, have
different access methods, and provide different guarantees.

The following sections provide insight into the high level objects and aspects
of their behavior which will allow you to use them effectively.


.. note::

    Data requests are fully materialized at this time.  A future release will include the option to process data using generators to save on memory usage.


BTrDB Server
------------
Like most time series databases, the BTrDB server contains multiple streams of
data in which each stream contains a data point at a given time.  However,
BTrDB focuses on univariate data which opens a host of benefits and is one of
the reasons BTrDB is able to process incredibly large amounts of data quickly
and easily.

.. _Points Described:
Points
------------
Points of data within a time series make up the smallest objects you will be
dealing with when making calls to the database.  Because there are different
types of interactions with the database, there are different types of points
that could be returned to you: :code:`RawPoint` and :code:`StatPoint`.

RawPoint
^^^^^^^^^^^^
The RawPoint represents a single time/value pair and is the simpler of the two
types of points.  This is most useful when you need to process every single
value within the stream.

.. code-block:: python

    >>> # view time and value of a single point in the stream
    >>> point.time
    1547241923338098176

    >>> point.value
    120.5

StatPoint
^^^^^^^^^^^^
The StatPoint provides statistics about multiple points and gives
aggregation values such as :code:`min`, :code:`max`, :code:`mean`, :code:`count` and :code:`stddev` (standard deviation).
This is most useful when you don't need to touch every individual value such as
when you only need the count of the values over a range of time.

These statistical queries execute in time proportional to the number of
results, not the number of underlying points (i.e logarithmic time) and so you
can attain valuable data in a fraction of the time when compared with retrieving
all of the individual values.  Due to the internal data structures, :code:`BTrDB` does
not need to read the underlying points to return these statistics!

.. code-block:: python

    >>> # view aggregate values for points in a stream
    >>> point.time
    1547241923338098176

    >>>point.min
    42.1

    >>> point.mean
    78.477

    >>> point.max
    122.4

    >>> point.count
    18600

    >>> point.stddev
    3.4


Tabular Data
------------
In addition to working with the :code:`RawPoint` or :code:`StatPoint` objects, newer versions of the platform now natively support some tabular data formats as well.
This is enabled for commercial customers and are available using the :code:`stream.arrow_` or :code:`streamset.arrow_` methods.
Refer to the :ref:`arrow enabled queries page <Arrow Page>` and the :ref:`API docs <API REF>`


Streams
------------
:code:`Stream` s represent a single series of time/value pairs.  As such, the database
can hold an almost unlimited amount of individual streams.  Each stream has a
:code:`collection` which is similar to a "path" or grouping for multiple streams.  Each
steam will also have a :code:`name` as well as a :code:`uuid` which is guaranteed to be unique
across streams.

:code:`BTrDB` data is versioned such that changes to a given stream (time series) will
result in a new version for the stream.  In this manner, you can pin your interactions to a
specific version ensuring the values do not change over the course of your
interactions.

.. note::

    If you want to work with the most recent version/data then specify a version of :code:`0` (the default).

Each stream has a number of attributes and methods available and these are documented
within the :ref:`API Reference <API REF>` section of this publication.  But the most common interactions
by users are to access the :code:`UUID`, :code:`tags`, :code:`annotations`, :code:`version`, and underlying data.

Each stream uses a :code:`UUID` as its unique identifier which can also be used when querying
for streams.  Metadata is provided by :code:`tags` and :code:`annotations` which are both provided
as dictionaries of data.  :code:`tags` are used internally and have very specific keys
while :code:`annotations` are more free-form and can be used by you to store your own
metadata.

.. code-block:: python

    >>> # retrieve stream's UUID
    >>> stream.uuid
    UUID("0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a")

    >>> # retrieve stream's current version
    >>> stream.version()
    244

    >>> # retrieve stream tags
    >>> stream.tags()
    {'name': 'L1MAG', 'unit': 'volts', 'ingress': ''}

    >>> # retrieve stream annotations
    >>> stream.annotations()
    ({'poc': 'Salvatore McFesterson', 'region': 'northwest', 'state': 'WA'}, 23)

    >>> # loop through points in the stream
    >>> for point, _ in stream.values(end=1547241923338098176, version=133):
    >>>     print(point)
    RawPoint(1500000000100000000, 2.4)
    RawPoint(1500000000200000000, 2.8)
    RawPoint(1500000000300000000, 3.6)
    ...


StreamSets
------------

Often you will want to query and work with multiple streams instead of just an
individual stream - :code:`StreamSets` allow you to do this effectively.  It is a light
wrapper around a list of :code:`Stream` objects with convenience methods provided to
help you work with multiple streams of data.

As an example, you can filter the stream data with a single method call and then
easily transform the data into other data types such as a pandas DataFrame or to
disk as a CSV file.  See the examples below for a quick sample and then visit
our API docs to see the full list of features provided to you.

.. note::

    :code:`StreamSet` methods that filter and operate on the :code:`StreamSet` object (like :code:`StreamSet.filter` ) return new copies of the :code:`StreamSet` itself rather than modifying in place. Similar to how most :code:`pandas.DataFrame` methods return a new :code:`DataFrame` object. This lets you compose multiple functions in a single call, which can improve readability, but can be tricky if you are not expecting this behavior.

Lets explore a common use-case, filtering a streamset.

.. code-block:: python

    >>> # create a streamset and apply a few filters
    >>> streamset = btrdb.stream.StreamSet(list_of_streams)
    >>> print(f"Total streams: {len(streamset)}")
    Total streams: 89

    >>> streamset.filter(units="Volts")
    >>> print(f"Total streams: {len(streamset)}")
    Total streams: 89

    >>> filtered_streamset = streamset.filter(units="Volts")
    >>> print(f"Total streams: {len(filtered_streamset)}")
    Total streams: 23

    >>> multiple_filters_streamset = (streamset.filter(unit="Volts")
    >>>                                 .filter(name="Sensor 1")
    >>>                                 .filter(annotations={"phase":"A"})
    >>>                              )
    >>> print(f"Total streams: {len(multiple_filters_streamset)}")
    Total streams: 1

.. code-block:: python

    >>> # establish database connection and query for streams by UUID
    >>> db = connect()
    >>> uuid_list = ["0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a", ...]
    >>> streams = db.streams(*uuid_list)

    >>> streams.filter(start=1500000000000000000).to_csv("data.csv")

    >>> streams.filter(start=1500000000000000000).to_dataframe()
                  time              NW/stream0  NW/stream1
        0  1500000000000000000         NaN         1.0
        1  1500000000100000000         2.0         NaN
        2  1500000000200000000         NaN         3.0
        3  1500000000300000000         4.0         NaN
        4  1500000000400000000         NaN         5.0
        5  1500000000500000000         6.0         NaN
        6  1500000000600000000         NaN         7.0
        7  1500000000700000000         8.0         NaN
        8  1500000000800000000         NaN         9.0
        9  1500000000900000000        10.0         NaN


Apache-Arrow Accelerated Methods
--------------------------------

* Refer to :ref:`Arrow Page`
