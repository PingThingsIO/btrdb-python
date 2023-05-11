import asyncio

import btrdb
import logging
from btrdb.experimental.arrow import ArrowStream
import uuid

async def main():
    logging.basicConfig(level=logging.INFO)
    conn = btrdb.connect(profile='paul')
    init_stream = (await conn.streams_in_collection('benchmarktwo'))[0]
    print(init_stream)
    # start = init_stream.earliest()[0].time
    start = 1681782479929367025
    end = btrdb.utils.timez.ns_delta(seconds=10000) + start
    init_stream = ArrowStream.from_stream(init_stream)
    await init_stream.values(start=start, end=end)
    print(init_stream.to_polars())

    # Create new empty stream.
    STREAM_NAME = 'arrowtesttwo'
    uuid_test = uuid.uuid5(uuid.NAMESPACE_URL, '')
    # try:
    c_stream = ArrowStream.from_stream(init_stream)
    # except:
    ver = await c_stream.arrowInsert(data=init_stream._data, merge='never')
    print(ver)

if __name__ == "__main__":
    asyncio.run(main(), debug=True)

