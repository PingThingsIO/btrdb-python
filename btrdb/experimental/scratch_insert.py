import uuid
import pyarrow as pa
from pyarrow.feather import write_feather
import io
import btrdb
from btrdb.experimental.arrow import ArrowStream
import time

import logging
logging.basicConfig(level=logging.DEBUG)

conn = btrdb.connect(profile="andy")

print(conn.info())

streams = conn.streams_in_collection("justin_test")

print(streams[0].earliest())
print(streams[0].latest())
start = streams[0].earliest()[0].time
end = streams[0].latest()[0].time
# end = btrdb.utils.timez.ns_delta(seconds=2) + start

prev_arr_stream = ArrowStream.from_stream(streams[0])
print(prev_arr_stream)
prev_arr_stream = prev_arr_stream.values(start, end)

# bytes_f = io.BytesIO()
# write_feather(df=prev_arr_stream._data.rename_columns(["time", "value"]), dest=bytes_f)
# print(bytes_f)
# buf = pa.BufferReader(bytes_f.getvalue())
# print(buf.__dir__())
# print(pa.feather.read_table(buf))
#
# print(pa.ipc.open_file(buf))
# with pa.ipc.open_file(buf) as reader:
#     print(reader.schema)
#     print(f"num batches: {reader.num_record_batches}")
#     b = reader.get_batch(0)
#     print(b)
#     print(b.serialize().to_pybytes())
#
# print(prev_arr_stream.to_polars())
# # print(pa.serialize(prev_arr_stream._data).to_buffer().to_pybytes())
#
# # new_uu = uuid.uuid4()
# # tags = {"name":"test_arr_insert", "unit":"TEST"}
# # new_stream = conn.create(uuid=new_uu, tags=tags, collection="justin_test_arrow_insert")
empty_stream = conn.streams_in_collection("justin_test_arrow_insert")
# print(empty_stream)
arr_stream_ins = ArrowStream.from_stream(empty_stream[0])
# print(arr_stream_ins)
ver = arr_stream_ins.arrowInsert(data=prev_arr_stream._data, merge="never")
print(ver)
