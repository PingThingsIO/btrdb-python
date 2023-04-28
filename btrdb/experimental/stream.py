import btrdb
from btrdb.stream import StreamSet, Stream
import pyarrow as pa


class ArrowStream(Stream):
    """Arrow-accelerated queries where applicable for a single stream."""
    def __init__(self, btrdb:btrdb.BTrDB, uuid:str, db_values:dict = None):
        super().__init__(self, btrdb=btrdb, uuid=uuid, **db_values)

    @classmethod
    def from_stream(cls, stream:btrdb.stream.Stream):
        return cls(btrdb=stream.btrdb, uuid=stream.uuid, db_values=None)

class ArrowStreamSet(StreamSet):
    """Arrow-accelerated queries where applicable for a set of streams."""
    def __init__(self, streams):
        super().__init__(streams=streams)

    @classmethod
    def from_streamset(cls, streamset:btrdb.stream.StreamSet):
        return cls(streams=streamset._streams)

# TODO: how to decode the arrow bytes?
    def values(self, start:int, end:int):
        """Return a numpy array from arrow bytes"""
        arrow_vals = []
        for s in self._streams:
            arrow_vals.append(s._btrdb.ep.arrowRawValues(uu=s.uu.bytes, start=start, end=end, version=0))
        arr = pa.Array().from_buffers(buffers=arrow_vals, length=len(arrow_vals[0]), DataType_type=None)
        return arr.to_numpy()

