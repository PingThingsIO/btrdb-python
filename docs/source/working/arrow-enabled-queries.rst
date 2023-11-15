.. -*- mode: rst -*-

.. _Arrow Page:

Arrow-enabled Queries
=====================

In more recent deployments of the BTrDB platform (>=5.30.0), commercial customers also have access to additional accelerated functionality for data fetching and inserting.

Also, most :code:`StreamSet` based value queries (:code:`AlignedWindows`, :code:`Windows`, :code:`Values`) are multithreaded by default.
This leads to decent performance improvements for fetching and inserting data using the standard :code:`StreamSet` api without any edits by the user.
Refer to :ref:`The StreamSet API <StreamSet API>`

In addition to these improvements to the standard API, commercial customers also have access to additional accelerated data fetching and inserting methods that can dramatically speed up their workflows.

Apache Arrow Data Format
^^^^^^^^^^^^^^^^^^^^^^^^

While keeping our standard API consistent with our :code:`Point` and :code:`StatPoint` :ref:`python object model <Points Described>`, we have also created additional methods that will provide this same type of data, but in a tabular format by default.
Leveraging the `language-agnostic columnar data format Arrow <https://arrow.apache.org/>`_, we can transmit our timeseries data in a format that is already optimized for data analytics with well-defined schemas that take the guesswork out of the data types, timezones, etc.
To learn more about these methods, please refer to the :ref:`arrow_ prefixed methods for both Stream and StreamSet objects <StreamGeneralDocs>` and the :ref:`StreamSet transformer methods <TransformersDocs>`.

.. _ArrowMultistreamDocs:
True Multistream Support
^^^^^^^^^^^^^^^^^^^^^^^^

Until now, there has not been a true multistream query support, our previous api and with the new edits, emulates multistream support with :code:`StreamSet`s and using multithreading.
However, this will still only scale to an amount of streams based on the amount of threads that the python threadpool logic can support.

Due to this, raw data queries for :code:`StreamSet`s using our arrow api :code:`StreamSet.filter(start=X, end=Y,).arrow_values()` will now perform true multistream queries.
The platform, instead of the python client, will now quickly grab all the stream data for all streams in your streamset, and then package that back to the python client in an :code:`arrow` table!
This leads to data fetch speedups on the order of 10-50x based on the amount and kind of streams.
