import concurrent.futures
import io
import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from pyarrow.feather import write_feather

import btrdb
from btrdb.stream import Stream, StreamSet, INSERT_BATCH_SIZE
from btrdb.transformers import _stream_names, _STAT_PROPERTIES

logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.DEBUG)

STREAMSET_API_DEFAULT_PARALLEL_REQUESTS = 64


class ArrowStream(Stream):
    """Arrow-accelerated queries where applicable for a single stream."""

    def __init__(self, btrdb: btrdb.BTrDB = None, uuid: str = None):
        self._data = None
        super().__init__(btrdb=btrdb, uuid=uuid)

    @classmethod
    def from_stream(cls, stream: btrdb.stream.Stream):
        return cls(uuid=stream.uuid, btrdb=stream._btrdb)

    def arrowInsert(self, data: pa.Table, merge="never"):
        """
        Insert new data in the form (time, value) into the series.

        Inserts a list of new (time, value) tuples into the series. The tuples
        in the list need not be sorted by time. If the arrays are larger than
        appropriate, this function will automatically chunk the inserts. As a
        consequence, the insert is not necessarily atomic, but can be used with
        a very large array.

        Parameters
        ----------
        data: pyarrow.Table
            The arrow table of data to insert, expects only 2 columns, one named
            "time", and the other named "value"
        merge: str
            A string describing the merge policy. Valid policies are:
              - 'never': the default, no points are merged
              - 'equal': points are deduplicated if the time and value are equal
              - 'retain': if two points have the same timestamp, the old one is kept
              - 'replace': if two points have the same timestamp, the new one is kept

        Returns
        -------
        int
            The version of the stream after inserting new points.

        """
        chunksize = INSERT_BATCH_SIZE
        tmp_table = data.rename_columns(["time", "value"])
        logger.debug(f"tmp_table schema: {tmp_table.schema}")
        num_rows = tmp_table.num_rows

        # Calculate the number of batches based on the chunk size
        num_batches = num_rows // chunksize
        if num_rows % chunksize != 0 or num_batches == 0:
            num_batches = num_batches + 1

        table_slices = []

        for i in range(num_batches):
            start_idx = i * chunksize
            t = tmp_table.slice(offset=start_idx, length=chunksize)
            table_slices.append(t)

        # Process the batches as needed
        version = []
        for tab in table_slices:
            logger.debug(f"Table Slice: {tab}")
            feather_bytes = _table_slice_to_feather_bytes(table_slice=tab)
            version.append(
                self._btrdb.ep.arrowInsertValues(
                    uu=self.uuid, values=feather_bytes, policy=merge
                )
            )
        return max(version)

    class _AsyncArrowValuesFuture(object):
        def __init__(self, fut, version, uuid):
            self.version = version
            self.fut = fut
            self.uuid = uuid

        def result(self) -> pa.Table:
            arr_bytes = list(self.fut.result())
            tab = _materialize_stream_as_table(arr_bytes)
            tab = tab.rename_columns(["time", str(self.uuid)])
            return tab

    def _async_values(self, start, end, version=0):
        fut = self._btrdb.ep.async_ArrowRawValues(self._uuid, start, end, version)
        return self._AsyncArrowValuesFuture(fut, version, self.uuid)

    def values(self, start: int, end: int):
        """Return the raw timeseries data between start and end.

        Parameters
        ----------
        start : int, required
            The beginning time to return data from, in nanoseconds.
        end : int, required
            The end time to return data from, in nanoseconds.

        Returns
        -------
        btrdb.experimental.arrow.ArrowStream
            The stream object with the populated _data member.
        """
        logger.debug(f"For stream - {self.uuid} -  {self.name}")
        arr_bytes = self._btrdb.ep.arrowRawValues(
            uu=self.uuid, start=start, end=end, version=0
        )
        # exhausting the generator from above
        bytes_materialized = list(arr_bytes)

        logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
        logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
        # ignore versions for now
        self._data = _materialize_stream_as_table(bytes_materialized)
        self._data = self._data.rename_columns(["time", str(self.uuid)])
        return self

    class _AsyncArrowWindowsFuture(object):
        def __init__(self, fut, version, uuid):
            self.version = version
            self.fut = fut
            self.uuid = uuid

        def result(self) -> pa.Table:
            arr_bytes = list(self.fut.result())
            tab = _materialize_stream_as_table(arr_bytes)
            stream_names = [
                "/".join([str(self.uuid), prop]) for prop in _STAT_PROPERTIES
            ]
            tab = tab.rename_columns(["time", *stream_names])
            return tab

    def _async_windows(
        self, start: int, end: int, width: int, depth: int = 0, version: int = 0
    ):
        fut = self._btrdb.ep.async_ArrowWindows(
            self.uuid, start=start, end=end, width=width, depth=depth, version=version
        )
        return self._AsyncArrowWindowsFuture(fut, version, self.uuid)

    def windows(
        self, start: int, end: int, width: int, depth: int = 0, version: int = 0
    ):
        """Read arbitrarily-sized windows of data from BTrDB.

        StatPoint objects will be returned representing the data for each window.

        Parameters
        ----------
        start : int or datetime like object
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        width : int
            The number of nanoseconds in each window.
        version : int
            The version of the stream to query.

        Returns
        -------
        btrdb.experimenal.arrow.ArrowStream
            The stream object with the populated _data member.
        """
        logger.debug(f"For stream - {self.uuid} -  {self.name}")
        arr_bytes = self._btrdb.ep.arrowWindows(
            self.uuid, start=start, end=end, width=width, depth=depth, version=version
        )
        # exhausting the generator from above
        bytes_materialized = list(arr_bytes)

        logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
        logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
        # ignore versions for now
        self._data = _materialize_stream_as_table(bytes_materialized)
        stream_names = [
            "/".join([self.collection, self.name, prop]) for prop in _STAT_PROPERTIES
        ]
        self._data = self._data.rename_columns(["time", *stream_names])
        return self

    class _AsyncArrowAlignedWindowsFuture(object):
        def __init__(self, fut, version, uuid):
            self.version = version
            self.fut = fut
            self.uuid = uuid

        def result(self) -> pa.Table:
            arr_bytes = list(self.fut.result())
            tab = _materialize_stream_as_table(arr_bytes)
            stream_names = [
                "/".join([str(self.uuid), prop]) for prop in _STAT_PROPERTIES
            ]
            tab = tab.rename_columns(["time", *stream_names])
            return tab

    def _async_AlignedWindows(
        self, start: int, end: int, pointwidth: int, version: int = 0
    ):
        fut = self._btrdb.ep.async_ArrowAlignedWindows(
            self.uuid, start=start, end=end, pointwidth=pointwidth, version=version
        )
        return self._AsyncArrowAlignedWindowsFuture(fut, version, self.uuid)

    def aligned_windows(self, start: int, end: int, pointwidth: int, version: int = 0):
        """Read statistical aggregates of windows of data from BTrDB.

        Query BTrDB for aggregates (or roll ups or windows) of the time series
        with `version` between time `start` (inclusive) and `end` (exclusive) in
        nanoseconds. Each point returned is a statistical aggregate of all the
        raw data within a window of width 2**`pointwidth` nanoseconds. These
        statistical aggregates currently include the mean, minimum, and maximum
        of the data and the count of data points composing the window.

        Note that `start` is inclusive, but `end` is exclusive. That is, results
        will be returned for all windows that start in the interval [start, end).
        If end < start+2^pointwidth you will not get any results. If start and
        end are not powers of two, the bottom pointwidth bits will be cleared.
        Each window will contain statistical summaries of the window.
        Statistical points with count == 0 will be omitted.

        Parameters
        ----------
        start : int or datetime like object
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        pointwidth : int
            Specify the number of ns between data points (2**pointwidth)
        version : int
            Version of the stream to query

        Returns
        -------
        btrdb.experimental.arrow.ArrowStream
            The stream object with the populated _data member.

        """
        logger.debug(f"For stream - {self.uuid} -  {self.name}")
        arr_bytes = self._btrdb.ep.arrowAlignedWindows(
            self.uuid, start=start, end=end, pointwidth=pointwidth, version=version
        )
        # exhausting the generator from above
        bytes_materialized = list(arr_bytes)

        logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
        logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
        # ignore versions for now
        self._data = _materialize_stream_as_table(bytes_materialized)
        stream_names = [
            "/".join([self.collection, self.name, prop]) for prop in _STAT_PROPERTIES
        ]
        self._data = self._data.rename_columns(["time", *stream_names])
        return self

    def to_pyarrow(self) -> pa.Table:
        """Return the _data of the stream as a pyarrow table."""
        if self._data is not None:
            return self._data

    def to_dataframe(self) -> pd.DataFrame:
        """Return the _data member of the stream as a pandas dataframe."""
        if self._data is not None:
            df = self._data.to_pandas()
            df = df.set_index("time")
            return df

    def to_array(self) -> np.array:
        return self._data.to_pandas().values

    def to_csv(self):
        raise NotImplementedError(
            """Method to_csv has not been implemented yet for Arrow-backed btrdb Streams/StreamSets.
        Please convert to a pandas dataframe, polars dataframe, pyarrow table, or numpy array to write to csv."""
        )

    def to_series(self):
        raise NotImplementedError(
            """Method to_series has not been implemented for Arrow-backed btrdb Streams/Streamsets.
        Please convert your streamset to a pandas dataframe."""
        )

    def to_dict(self):
        raise NotImplementedError(
            """Method to_dict has not been implemented yet for Arrow-backed btrdb Streams/StreamSets.
        Using to_pyarrow to return the data as a pyarrow Table and then calling Table.to_pydict can work in the meantime."""
        )

    def to_polars(self) -> pl.DataFrame:
        """Return the _data member of the stream as a polars dataframe."""
        if self._data is not None:
            return pl.from_arrow(self._data)


def _materialize_stream_as_table(arrow_bytes):
    table_list = []
    for b, _ in arrow_bytes:
        with pa.ipc.open_stream(b) as reader:
            schema = reader.schema
            logger.debug(f"schema: {schema}")
            table_list.append(reader.read_all())
    logger.debug(f"table list: {table_list}")
    table = pa.concat_tables(table_list)
    return table


def _table_slice_to_feather_bytes(table_slice: pa.Table) -> bytes:
    my_bytes = io.BytesIO()
    write_feather(table_slice, dest=my_bytes)
    return my_bytes.getvalue()


def _coalesce_table_deque(tables: deque):
    main_table = tables.popleft()
    idx = 0
    while len(tables) != 0:
        idx = idx + 1
        t2 = tables.popleft()
        main_table = main_table.join(
            t2, "time", join_type="full outer", right_suffix=f"_{idx}"
        )
    return main_table


class ArrowStreamSet(StreamSet):
    """Arrow-accelerated queries where applicable for a set of streams."""

    def __init__(self, streams):
        super().__init__(streams=streams)

    @classmethod
    def from_streamset(cls, streamset: btrdb.stream.StreamSet):
        streams = [ArrowStream.from_stream(s) for s in streamset._streams]
        return cls(streams=streams)

    def values(self, start: int, end: int):
        """Return a numpy array from arrow bytes

        Parameters
        ----------
        start : int, required
            The beginning time to return data from, in nanoseconds.
        end : int, required
            The end time to return data from, in nanoseconds.
        """
        logger.debug("In values method for ArrowStreamSet")
        tables = deque(self._streams[0]._btrdb._executor.map(lambda s: s.values(start, end)._data, self._streams))
        self._data = _coalesce_table_deque(tables)
        return self

    def windows(
        self, start: int, end: int, width: int, depth: int = 0, version: int = 0
    ):
        """Read arbitrarily-sized windows of data from BTrDB.

        StatPoint objects will be returned representing the data for each window.

        Parameters
        ----------
        start : int or datetime like object
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        width : int
            The number of nanoseconds in each window.
        version : int
            The version of the stream to query.

        Returns
        -------
        pd.DataFrame
        """
        self.width = int(width)
        self.depth = int(depth)
        stream_tables = deque()
        futs = deque()
        for s in self._streams:
            logger.debug(f"For stream - {s.uuid} -  {s.name}")
            if len(futs) == STREAMSET_API_DEFAULT_PARALLEL_REQUESTS:
                stream_tables.append(futs.pop().result())
            futs.appendleft(
                s._async_windows(
                    start=start, end=end, width=width, depth=depth, version=version
                )
            )
            logger.debug(f"Futs deque: {futs}")
        logger.debug(f"futs[0]: {futs[0].fut.fut}")
        while len(futs) != 0:
            stream_tables.append(futs.pop().result())
        self._data = _coalesce_table_deque(stream_tables)
        return self

    def aligned_windows(
        self, start: int, end: int, pointwidth: int, version: int = 0
    ) -> pd.DataFrame:
        """Read statistical aggregates of windows of data from BTrDB.

        Query BTrDB for aggregates (or roll ups or windows) of the time series
        with `version` between time `start` (inclusive) and `end` (exclusive) in
        nanoseconds. Each point returned is a statistical aggregate of all the
        raw data within a window of width 2**`pointwidth` nanoseconds. These
        statistical aggregates currently include the mean, minimum, and maximum
        of the data and the count of data points composing the window.

        Note that `start` is inclusive, but `end` is exclusive. That is, results
        will be returned for all windows that start in the interval [start, end).
        If end < start+2^pointwidth you will not get any results. If start and
        end are not powers of two, the bottom pointwidth bits will be cleared.
        Each window will contain statistical summaries of the window.
        Statistical points with count == 0 will be omitted.

        Parameters
        ----------
        start : int or datetime like object
            The start time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        end : int or datetime like object
            The end time in nanoseconds for the range to be queried. (see
            :func:`btrdb.utils.timez.to_nanoseconds` for valid input types)
        pointwidth : int
            Specify the number of ns between data points (2**pointwidth)
        version : int
            Version of the stream to query

        """
        self.pointwidth = int(pointwidth)
        stream_tables = deque()
        futs = deque()
        for s in self._streams:
            logger.debug(f"For stream - {s.uuid} -  {s.name}")
            if len(futs) == STREAMSET_API_DEFAULT_PARALLEL_REQUESTS:
                stream_tables.append(futs.pop().result())
            futs.appendleft(
                s._async_AlignedWindows(
                    start=start, end=end, pointwidth=self.pointwidth, version=version
                )
            )
            logger.debug(f"Futs deque: {futs}")
        logger.debug(f"futs[0]: {futs[0].fut.fut}")
        while len(futs) != 0:
            stream_tables.append(futs.pop().result())
        self._data = _coalesce_table_deque(stream_tables)
        return self

    def to_pyarrow(self):
        return self._data

    def to_polars(self):
        return pl.from_arrow(self._data)

    def to_csv(self):
        raise NotImplementedError(
            """Method to_csv has not been implemented yet for Arrow-backed btrdb Streams/StreamSets.
        Please convert to a pandas dataframe, polars dataframe, pyarrow table, or numpy array to write to csv."""
        )

    def to_series(self):
        raise NotImplementedError(
            """Method to_series has not been implemented for Arrow-backed btrdb Streams/Streamsets.
        Please convert your streamset to a pandas dataframe."""
        )

    def to_dict(self):
        raise NotImplementedError(
            """Method to_dict has not been implemented yet for Arrow-backed btrdb Streams/StreamSets.
        Using to_pyarrow to return the data as a pyarrow Table and then calling Table.to_pydict can work in the meantime."""
        )

    def to_table(self):
        raise NotImplementedError(
            """Method to_table has not been implemented yet for Arrow-backed btrdb Streams/StreamSets."""
        )
