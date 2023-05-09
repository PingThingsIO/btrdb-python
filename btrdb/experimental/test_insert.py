import uuid
import pyarrow as pa
import btrdb
from btrdb.experimental.arrow import ArrowStream

conn = btrdb.connect(profile="andy")

print(conn.info())

streams = conn.streams_in_collection("justin_test")

print(streams[0].earliest())
print(streams[0].latest())
start = streams[0].earliest()[0].time
end = streams[0].latest()[0].time


prev_arr_stream = ArrowStream.from_stream(streams[0])
print(prev_arr_stream)
prev_arr_stream = prev_arr_stream.values(start, end)
print(prev_arr_stream.to_polars())
# print(pa.serialize(prev_arr_stream._data).to_buffer().to_pybytes())

# new_uu = uuid.uuid4()
# tags = {"name":"test_arr_insert", "unit":"TEST"}
# new_stream = conn.create(uuid=new_uu, tags=tags, collection="justin_test_arrow_insert")
empty_stream = conn.streams_in_collection("justin_test_arrow_insert")
print(empty_stream)
arr_stream_ins = ArrowStream.from_stream(empty_stream[0])
print(arr_stream_ins)
ver = arr_stream_ins.arrowInsert(data=prev_arr_stream._data, merge="never")
print(ver)
