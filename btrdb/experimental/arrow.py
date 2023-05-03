import logging

import polars as pl
import pyarrow as pa

import btrdb
from btrdb.stream import Stream, StreamSet

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class ArrowStream(Stream):
    """Arrow-accelerated queries where applicable for a single stream."""

    def __init__(self, btrdb: btrdb.BTrDB, uuid: str, db_values: dict = None):
        super().__init__(self, btrdb=btrdb, uuid=uuid, **db_values)

    @classmethod
    def from_stream(cls, stream: btrdb.stream.Stream):
        return cls(btrdb=stream.btrdb, uuid=stream.uuid, db_values=None)


class ArrowStreamSet(StreamSet):
    """Arrow-accelerated queries where applicable for a set of streams."""

    def __init__(self, streams):
        super().__init__(streams=streams)

    @classmethod
    def from_streamset(cls, streamset: btrdb.stream.StreamSet):
        return cls(streams=streamset._streams)

    # TODO: how to decode the arrow bytes?
    # TODO: need to ensure we exhaust the generator from the endpoint
    def values(self, start: int, end: int):
        """Return a numpy array from arrow bytes"""
        logger.debug("In values method for ArrowStreamSet")
        for s in self._streams:
            logger.debug(f"For stream - {s.uuid} -  {s.name}")
            arr_bytes = s._btrdb.ep.arrowRawValues(
                uu=s.uuid, start=start, end=end, version=0
            )
            # exhausting the generator from above
            bytes_materialized = list(arr_bytes)
            logger.debug(f"materialized bytes: {bytes_materialized}")
            logger.debug(f"Length of materialized list: {len(bytes_materialized)}")
            logger.debug(f"arr bytes: {arr_bytes}")
            with pa.ipc.open_stream(bytes_materialized[0][0]) as reader:
                schema = reader.schema
                logger.debug(f"schema: {schema}")
                df = reader.read_pandas()
                logger.debug(f"Dataframe: {df}")
            with pa.ipc.open_stream(bytes_materialized[0][0]) as reader:
                pldf = pl.from_arrow(reader.read_all())
                logger.debug(f"Polars: {pldf}")
        return df, pldf
