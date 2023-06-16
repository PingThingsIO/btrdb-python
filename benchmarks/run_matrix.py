import json

import pandas as pd
from utils.benchmark_stream_reads import (
    time_single_stream_aligned_windows_values,
    time_single_stream_arrow_aligned_windows_values,
    time_single_stream_arrow_raw_values,
    time_single_stream_arrow_windows_values,
    time_single_stream_raw_values,
    time_single_stream_windows_values,
)

import btrdb


def main():
    json_file = "bench_list.json"
    df = pd.read_json(json_file)
    print(df)

    conn = btrdb.connect(profile="andy")
    stream = conn.stream_from_uuid(
        list(conn.streams_in_collection("andy/7064-6684-5e6e-9e14-ff9ca7bae46e"))[
            0
        ].uuid
    )
    func_list = [
        time_single_stream_arrow_windows_values,
        time_single_stream_windows_values,
        time_single_stream_arrow_aligned_windows_values,
        time_single_stream_aligned_windows_values,
        time_single_stream_raw_values,
        time_single_stream_arrow_raw_values,
    ]
    for fun in func_list[0:1]:
        for row in df.itertuples():
            # print(row)
            if row.func_name == fun.__name__:
                print("HERE")


if __name__ == "__main__":
    main()
