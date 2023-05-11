import logging

import time

import pandas as pd
import asyncio
import btrdb
from btrdb.experimental.arrow import ArrowStreamSet, ArrowStream

logging.basicConfig(level=logging.INFO)


async def main():
    conn = btrdb.connect(profile="andy")
    # stream_uu = "295dff19-e289-5bf3-af71-415928fc95cd"
    std_streamset = btrdb.stream.StreamSet(await conn.streams_in_collection("justin_test"))
    # std_streamset = btrdb.stream.StreamSet([stream, stream, stream])
    # print(std_streamset[0])
    # print(std_streamset[0].count())
    #
    arr_streamset = ArrowStreamSet.from_streamset(std_streamset)
    # print(arr_streamset[0])
    # print(arr_streamset[0].__dir__())
    # arr_stream = ArrowStream.from_stream(arr_streamset[0])
    # print(arr_streamset[0])
    # print(arr_stream)
    #
    # start = (await std_streamset.earliest())[0].time
    # end = (await std_streamset.latest())[0].time
    start = 1682488808200000000
    end = 1683325277600000000
    #
    # print(f"Start time: {start}\nEnd time: {end}")
    #
    times = []
    for i in range(5):
        tic = time.perf_counter()
        print((await arr_streamset.values(start, end)).to_polars())
        toc = time.perf_counter()
        times.append(toc - tic)
    print(times)
    # tmp_stream = arr_stream.values(start=start, end=end)
    # print(tmp_stream._data)
    # print(tmp_stream.to_df())
    # print(tmp_stream.to_pyarrow())
    # print(tmp_stream.to_polars())
    # print(tmp_stream.to_numpy())

    # tmp_stream = arr_stream.aligned_windows(start=start, end=end, pointwidth=32)
    # print(tmp_stream.to_df())
    # print(tmp_stream.to_pyarrow())
    # print(tmp_stream.to_polars())
    # print(tmp_stream.to_numpy())


    # print(arr_streamset.values(start, end))
    # print(arr_stream.values(start, end))
    #
    # print(arr_stream.windows(start, end, width=int(2**30)))
    #
    # print(arr_stream.aligned_windows(start, end, pointwidth=31))

    # std_df_tic = []
    # std_df_toc = []
    # arr_df_tic = []
    # arr_df_toc = []
    #
    # for i in range(5):
    #     print(i)
    #     std_df_tic.append(time.perf_counter())
    #     std_streamset = std_streamset.filter(start=start, end=end)
    #     df = std_streamset.to_dataframe()
    #     std_df_toc.append(time.perf_counter())
    #     print(i)
    #
    #     arr_df_tic.append(time.perf_counter())
    #     arr_streamset = arr_streamset.values(start, end)
    #     df2 = arr_streamset.to_dataframe(from_arrow=True)
    #     arr_df_toc.append(time.perf_counter())
    #
    # a_dict = {"std_tic":std_df_tic, "std_toc":std_df_toc, "arr_tic":arr_df_tic, "arr_toc":arr_df_toc}
    # print(a_dict)
    #
    # pd.DataFrame.from_dict(a_dict).to_csv('time.csv')


if __name__ == "__main__":
    asyncio.run(main())


def main():
    conn = btrdb.connect(profile="andy")
    std_streamset = btrdb.stream.StreamSet(conn.streams_in_collection("justin_test"))

    arr_streamset = ArrowStreamSet.from_streamset(std_streamset)

    start = 1682488808200000000
    end = 1683325277600000000


    std_df_tic = []
    std_df_toc = []
    arr_df_tic = []
    arr_df_toc = []

    for i in range(10):
        print(i)
        std_df_tic.append(time.perf_counter())
        std_streamset = std_streamset.filter(start=start, end=end)
        df = std_streamset.to_dataframe()
        std_df_toc.append(time.perf_counter())
        print(i)

        arr_df_tic.append(time.perf_counter())
        arr_streamset = arr_streamset.values(start, end)
        df2 = arr_streamset.to_dataframe(from_arrow=True)
        arr_df_toc.append(time.perf_counter())

    a_dict = {"std_tic":std_df_tic, "std_toc":std_df_toc, "arr_tic":arr_df_tic, "arr_toc":arr_df_toc}
    print(a_dict)

    pd.DataFrame.from_dict(a_dict).to_csv('time.csv')