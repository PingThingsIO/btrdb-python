import logging

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
        list_of_data = []
        logger.debug("In values method for ArrowStreamSet")
        for s in self._streams:
            logger.debug(f"For stream - {s.uuid} -  {s.name}")
            arr_bytes, ver = s._btrdb.ep.arrowRawValues(
                uu=s.uu.bytes, start=start, end=end, version=0
            )
            with pa.ipc.open_stream(arr_bytes) as reader:
                schema = reader.schema
                metadata = reader.metadata
                logger.debug(f"schema: {schema}")
                logger.debug(f"metadata: {metadata}")
                batches = [b for b in reader]
            list_of_data.append(pa.table(data=batches, schema=schema))
        return list_of_data
