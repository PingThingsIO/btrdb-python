.. _changelog:
Changelog
=========

5.32.0
------

What's Changed
^^^^^^^^^^^^^^
- Initial docstring overhaul and a new test for better documentation and test coverage. by @JustinGilmer in `#82 <https://github.com/pingthingsio/btrdb-python/pull/82>`_
- Test new join logic for improved data loading for windowed queries. by @JustinGilmer in `#80 <https://github.com/pingthingsio/btrdb-python/pull/80>`_
- Improve :code:`arrow_to_dataframe` function for handling large amounts of columns, enhancing performance and usability. by @Jefflinf in `#73 <https://github.com/pingthingsio/btrdb-python/pull/73>`_
- Expand testing to include Python 3.11, ensuring compatibility and stability. by @JustinGilmer in `#74 <https://github.com/pingthingsio/btrdb-python/pull/74>`_
- Update exception handling to better support :code:`RpcErrors`, improving error management and debugging. by @JustinGilmer in `#72 <https://github.com/pingthingsio/btrdb-python/pull/72>`_
- Introduce an option for specifying the schema of the returned raw data, allowing for more flexibility in data handling. by @andrewchambers in `#51 <https://github.com/pingthingsio/btrdb-python/pull/51>`_
- Remove non-required dependencies and migrate to 'data' optional dependency for a lighter package and easier installation. by @JustinGilmer in `#71 <https://github.com/pingthingsio/btrdb-python/pull/71>`_
- New method to get first and last timestamps from ``aligned_windows``, enhancing data analysis capabilities. by @Jefflinf in `#70 <https://github.com/pingthingsio/btrdb-python/pull/70>`_
- Add ``to_timedelta`` method for ``pointwidth`` class, providing more options for time-based data manipulation. by @Jefflinf in `#69 <https://github.com/pingthingsio/btrdb-python/pull/69>`_

Fixed
^^^^^
- Fix ``NoneType`` error for ``earliest/latest`` for empty streams, ensuring reliability and error handling. by @Jefflinf in `#64 <https://github.com/pingthingsio/btrdb-python/pull/64>`_
- Correct integration tests where the time column is not automatically set as the index, improving test accuracy and reliability. by @JustinGilmer in `#56 <https://github.com/pingthingsio/btrdb-python/pull/56>`_

Deprecated
^^^^^^^^^^
- FutureWarning for ``streams_in_collection`` to return ``StreamSet`` in the future, preparing users for upcoming API changes. by @Jefflinf in `#60 <https://github.com/pingthingsio/btrdb-python/pull/60>`_

**Full Changelog**: `GitHub compare view <https://github.com/PingThingsIO/btrdb-python/compare/v5.31.0...v5.32.0>`_


5.31.0
------
What's Changed
^^^^^^^^^^^^^^
* Have release script update pyproject.toml file by @youngale-pingthings in https://github.com/PingThingsIO/btrdb-python/pull/48
* Provide option to sort the arrow tables by @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/47
* Remove 4MB limit for gRPC message payloads by @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/49
* Update documentation for arrow methods by @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/50
* Update from staging by @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/54
* Sort tables by time by default for any ``pyarrow`` tables. by @justinGilmer in
* Fix deprecation warnings for pip installations. by @jleifnf in

**Full Changelog**: `GitHub compare view <https://github.com/PingThingsIO/btrdb-python/compare/v5.30.2...v5.31.0>`_

5.30.2
------
What’s Changed
^^^^^^^^^^^^^^
-  Update readthedocs to new yaml for testing. by @justinGilmer in
   https://github.com/PingThingsIO/btrdb-python/pull/40
-  Converting pandas index takes very long, fix this in arrow_table. by
   @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/41

**Full Changelog**:
`5.30.2 <https://github.com/PingThingsIO/btrdb-python/compare/v5.30.1…v5.30.2>`_

.. _section-1:

5.30.1
------
.. _whats-changed-1:

What’s Changed
^^^^^^^^^^^^^^

-  Small version bump for pypi release

**Full Changelog**:
`5.30.1 <https://github.com/PingThingsIO/btrdb-python/compare/v5.30.0…v5.30.1>`_

.. _section-2:

5.30.0
------
.. _whats-changed-2:

What’s Changed
^^^^^^^^^^^^^^

-  Merge Arrow support into Main for Release by @youngale-pingthings in
   https://github.com/PingThingsIO/btrdb-python/pull/37

   -  This PR contains many changes that support the commercial Arrow
      data fetches and inserts
   -  ``arrow_`` prefixed methods for ``Stream`` Objects:

      -  ``insert, aligned_windows, windows, values``

   -  ``arrow_`` prefixed methods for StreamSet\` objects:

      -  ``insert, values, to_dataframe, to_polars, to_arrow_table, to_numpy, to_dict, to_series``

-  Justin gilmer patch 1 by @justinGilmer in
   https://github.com/PingThingsIO/btrdb-python/pull/39

**Full Changelog**:
`5.30.0 <https://github.com/PingThingsIO/btrdb-python/compare/v5.28.1…v5.30.0>`_

.. _section-3:

5.28.1
------
.. _whats-changed-3:

What’s Changed
^^^^^^^^^^^^^^

-  Upgrade ray versions by @jleifnf in
   https://github.com/PingThingsIO/btrdb-python/pull/15
-  Release v5.28.1 and Update Python by @youngale-pingthings in
   https://github.com/PingThingsIO/btrdb-python/pull/17

New Contributors
^^^^^^^^^^^^^^^^

-  @jleifnf made their first contribution in
   https://github.com/PingThingsIO/btrdb-python/pull/15

**Full Changelog**:
`5.28.1 <https://github.com/PingThingsIO/btrdb-python/compare/v5.15.1…v5.28.1>`_
