import logging

import pandas as pd
import polars as pl
import pyarrow as pa

import btrdb
from btrdb.stream import Stream, StreamSet

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class ArrowStream(Stream):
    """Arrow-accelerated queries where applicable for a single stream."""

    def __init__(self, btrdb: btrdb.BTrDB = None, uuid: str = None):
        super().__init__(btrdb=btrdb, uuid=uuid)

    @classmethod
    def from_stream(cls, stream: btrdb.stream.Stream):
        return cls(uuid=stream.uuid, btrdb=stream._btrdb)

    def values(self, start: int, end: int) -> pd.DataFrame:
        """Return the raw timeseries data between start and end.

        Parameters
        ----------
        start : int, required
            The beginning time to return data from, in nanoseconds.
        end : int, required
            The end time to return data from, in nanoseconds.
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
        table_list = []
        for b, _ in bytes_materialized:
            with pa.ipc.open_stream(b) as reader:
                schema = reader.schema
                logger.debug(f"schema: {schema}")
                table_list.append(reader.read_all())
        logger.debug(f"table list: {table_list}")
        table = pa.concat_tables(table_list)
        return table.to_pandas()

    def windows(
        self, start: int, end: int, width: int, depth: int = 0, version: int = 0
    ) -> pd.DataFrame:
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
        logger.debug(f"For stream - {self.uuid} -  {self.name}")
        arr_bytes = self._btrdb.ep.arrowWindows(
            self.uuid, start=start, end=end, width=width, depth=depth, version=0
        )
        # exhausting the generator from above
        bytes_materialized = list(arr_bytes)

        logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
        logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
        # ignore versions for now
        table_list = []
        for b, _ in bytes_materialized:
            with pa.ipc.open_stream(b) as reader:
                schema = reader.schema
                logger.debug(f"schema: {schema}")
                table_list.append(reader.read_all())
        logger.debug(f"table list: {table_list}")
        table = pa.concat_tables(table_list)
        return table.to_pandas()

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
        logger.debug(f"For stream - {self.uuid} -  {self.name}")
        arr_bytes = self._btrdb.ep.arrowAlignedWindows(
            self.uuid, start=start, end=end, pointwidth=pointwidth, version=0
        )
        # exhausting the generator from above
        bytes_materialized = list(arr_bytes)

        logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
        logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
        # ignore versions for now
        table_list = []
        for b, _ in bytes_materialized:
            with pa.ipc.open_stream(b) as reader:
                schema = reader.schema
                logger.debug(f"schema: {schema}")
                table_list.append(reader.read_all())
        logger.debug(f"table list: {table_list}")
        table = pa.concat_tables(table_list)
        return table.to_pandas()


class ArrowStreamSet(StreamSet):
    """Arrow-accelerated queries where applicable for a set of streams."""

    def __init__(self, streams):
        super().__init__(streams=streams)

    @classmethod
    def from_streamset(cls, streamset: btrdb.stream.StreamSet):
        return cls(streams=streamset._streams)

    # TODO: Need to figure out how to append multiple streams
    # TODO: Need to support multiindex, or key on uuid
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
        stream_tables = []
        for s in self._streams:
            logger.debug(f"For stream - {s.uuid} -  {s.name}")
            arr_bytes = s._btrdb.ep.arrowRawValues(
                uu=s.uuid, start=start, end=end, version=0
            )
            # exhausting the generator from above
            bytes_materialized = list(arr_bytes)

            logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
            logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
            # ignore versions for now
            table_list = []
            for b, _ in bytes_materialized:
                with pa.ipc.open_stream(b) as reader:
                    schema = reader.schema
                    logger.debug(f"schema: {schema}")
                    table_list.append(reader.read_all())
            logger.debug(f"table list: {table_list}")
            table = pa.concat_tables(table_list)
            stream_tables.append(table)
        return table.to_pandas(), pl.from_arrow(table)

    def windows(
        self, start: int, end: int, width: int, depth: int = 0, version: int = 0
    ) -> pd.DataFrame:
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
        stream_tables = []
        for s in self._streams:
            logger.debug(f"For stream - {s.uuid} -  {s.name}")
            arr_bytes = s._btrdb.ep.arrowWindows(
                s.uuid, start=start, end=end, width=width, depth=depth, version=0
            )
            # exhausting the generator from above
            bytes_materialized = list(arr_bytes)

            logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
            logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
            # ignore versions for now
            table_list = []
            for b, _ in bytes_materialized:
                with pa.ipc.open_stream(b) as reader:
                    schema = reader.schema
                    logger.debug(f"schema: {schema}")
                    table_list.append(reader.read_all())
            logger.debug(f"table list: {table_list}")
            table = pa.concat_tables(table_list)
            stream_tables.append(table)
        return table.to_pandas()

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
        stream_tables = []
        for s in self._streams:
            logger.debug(f"For stream - {s.uuid} -  {s.name}")
            arr_bytes = s._btrdb.ep.arrowAlignedWindows(
                s.uuid, start=start, end=end, pointwidth=pointwidth, version=0
            )
            # exhausting the generator from above
            bytes_materialized = list(arr_bytes)

            logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
            logger.debug(f"materialized bytes[0:1]: {bytes_materialized[0:1]}")
            # ignore versions for now
            table_list = []
            for b, _ in bytes_materialized:
                with pa.ipc.open_stream(b) as reader:
                    schema = reader.schema
                    logger.debug(f"schema: {schema}")
                    table_list.append(reader.read_all())
            logger.debug(f"table list: {table_list}")
            table = pa.concat_tables(table_list)
            stream_tables.append(table)
        return table.to_pandas()
