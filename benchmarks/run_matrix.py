import time
import uuid
from multiprocessing import Manager, Pool

import pandas as pd
import pyarrow as pa
from utils.benchmark_stream_inserts import time_stream_arrow_insert, time_stream_insert
from utils.benchmark_stream_reads import (
    time_single_stream_aligned_windows_values,
    time_single_stream_arrow_aligned_windows_values,
    time_single_stream_arrow_raw_values,
    time_single_stream_arrow_windows_values,
    time_single_stream_raw_values,
    time_single_stream_windows_values,
)
from utils.benchmark_streamset_inserts import (
    time_streamset_arrow_inserts,
    time_streamset_inserts,
)
from utils.benchmark_streamset_reads import (
    time_streamset_aligned_windows_values,
    time_streamset_arrow_aligned_windows_values,
    time_streamset_arrow_multistream_raw_values_non_timesnapped,
    time_streamset_arrow_multistream_raw_values_timesnapped,
    time_streamset_arrow_raw_values,
    time_streamset_arrow_windows_values,
    time_streamset_raw_values,
    time_streamset_windows_values,
)
from utils.data_create import get_end_from_start_time_npoints, make_data

import btrdb

func_list = [
    time_streamset_arrow_windows_values,
    time_streamset_arrow_raw_values,
    time_streamset_aligned_windows_values,
    time_streamset_arrow_aligned_windows_values,
    time_streamset_arrow_multistream_raw_values_non_timesnapped,
    time_streamset_arrow_multistream_raw_values_timesnapped,
    time_streamset_raw_values,
    time_streamset_windows_values,
    time_single_stream_aligned_windows_values,
    time_single_stream_arrow_aligned_windows_values,
    time_single_stream_arrow_raw_values,
    time_single_stream_arrow_windows_values,
    time_single_stream_raw_values,
    time_single_stream_windows_values,
    time_stream_arrow_insert,
    time_stream_insert,
    time_streamset_inserts,
    time_streamset_arrow_inserts,
]

func_dict = {my_func.__name__: my_func for my_func in func_list}


def _make_data_multiproc(uu: uuid.UUID, n_points: int, shared_dict: dict):
    shared_dict[uu] = make_data(n_points)


def setup_read_streams(
    coll: str,
    n_streams: int,
    n_points: int,
    conn_params: dict,
    name_prefix: str = "foo",
    n_proc: int = 4,
):
    """Create and insert data into a collection for testing.

    Parameters
    ----------
    coll : str, required
        The collection for these streams
    n_streams : int, required
        How many streams to create/insert data into
    n_points : int, required
        How many points in the stream at 30Hz
    conn_params : dict, required
        The connection parameters to pass to the btrdb.connect constructor
    name_prefix : str, optional, default=foo
        What name prefix to use when creating the streams?
    n_proc : int, optional, default=4
        How many separate processes to use to generate the data map.


    Notes
    -----
    The points inserted will start at 2023-01-01 00:00:00

    These data will be randomly distributed and inserted at a 30hz sampling frequency
    The insert policy will be 'latest'

    The name prefix will look as so:
        if 2 streams and name_prefix='foo'
         -> 'foo0', 'foo1'
    """
    policy = "replace"
    conn = btrdb.connect(**conn_params)
    name_list = [f"{name_prefix}{i}" for i in range(n_streams)]
    uus = [uuid.uuid4() for _ in range(n_streams)]
    streams = []
    for name, uu in zip(name_list, uus):
        streams.append(
            conn.create(uuid=uu, collection=coll, tags={"name": name, "unit": "foo"})
        )

    assert len(streams) == n_streams
    streamset = btrdb.stream.StreamSet(streams)
    assert len(streamset) == n_streams
    with Manager() as manager:
        data_map = manager.dict()
        with Pool(processes=n_proc) as pool:
            _ = pool.starmap(
                _make_data_multiproc, [(s.uuid, n_points, data_map) for s in streamset]
            )

        vers = streamset.arrow_insert(data_map=data_map, merge=policy)
        print(vers)
        return vers


def get_matrix_df(fn: str):
    """Create the test matrix dataframe from the json file.

    Parameters
    ----------
    fn : str, required
        The Json filename containing the benchmark matrix

    """
    return pd.read_json(fn, dtype_backend="pyarrow")


def _process_df(df: pd.DataFrame):
    """Split dataframe by the kind of benchmark."""
    df["mask"] = df["func_name"].apply(lambda x: None if "insert" in x else True)
    df = df.drop_duplicates()
    # print(df.mask)
    # print(df.iloc[0])
    # print(df.info())
    # print(help(func_dict[df.func_name[100]]))
    # print((df['func_name'].apply(lambda x: "insert" in x)).sum())
    # filter out the insert methods for the time being
    ins_df = df.copy()
    ins_df = ins_df[ins_df["mask"].isna()].drop(columns=["mask"]).reset_index(drop=True)
    df = (
        df.dropna(subset=["mask"], axis="index")
        .drop(columns=["mask"])
        .reset_index(drop=True)
    )
    raw_val_df = df.copy()
    raw_val_df = raw_val_df[raw_val_df["func_name"].str.contains("raw")].reset_index(
        drop=True
    )
    # print(raw_val_df.func_name.unique())
    aligned_windows_df = df.copy()
    aligned_windows_df = aligned_windows_df[
        aligned_windows_df["func_name"].str.contains("aligned")
    ].reset_index(drop=True)
    # print(aligned_windows_df.func_name.unique())
    windows_df = df.copy()
    windows_df = windows_df[windows_df["func_name"].str.contains("window")].reset_index(
        drop=True
    )
    windows_df = windows_df[windows_df["width_ns"].notna()]
    return ins_df, raw_val_df, windows_df, aligned_windows_df


def bench_single_stream_reads(
    matrix_filename: str,
    collection: str,
    conn_params: dict,
    start_time: str = "2023-01-01",
    sampling_freq: int = 1,
):
    """Run the benchmarks for the single stream read operations.

    Parameters
    ----------
    matrix_filename : str, required
        The JSON filename containing the single stream benchmark parameters
    collection : str, required
        The stream collection to run the benchmarks against
    conn_params : Dict[str, str], required
        The connection parameters to the btrdb server for benchmarking.
    start_time : str, optional, default = '2023-01-01'
        The start time for data reading, in ISO datetime format
    sampling_freq : int, optional, default = 1
        The sampling frequency of the data, in Hz
    """
    df = get_matrix_df(matrix_filename)
    ins_df, raw_df, win_df, align_df = _process_df(df)
    conn = btrdb.connect(**conn_params)
    my_stream = conn.streams_in_collection(collection)[0]
    res_list = []
    start_time = btrdb.utils.timez.to_nanoseconds(start_time)
    for row in win_df.itertuples():
        end_time = get_end_from_start_time_npoints(
            start=start_time, n_points=row.n_points, freq=sampling_freq
        )
        res = func_dict.get(row.func_name, KeyError)(
            my_stream, start_time, end_time, row.width_ns, 0
        )
        tmp = row._asdict()
        tmp.update(res)
        res_list.append(tmp)
    print("done with single stream windows")
    pd.DataFrame(res_list).to_csv("single_stream_windows.csv")
    res_list = []
    for row in align_df.itertuples():
        end_time = get_end_from_start_time_npoints(
            start=start_time, n_points=row.n_points, freq=sampling_freq
        )
        res = func_dict.get(row.func_name, KeyError)(
            my_stream, start_time, end_time, row.pw, 0
        )
        tmp = row._asdict()
        tmp.update(res)
        res_list.append(tmp)
    print("done with single stream aligned windows")
    pd.DataFrame(res_list).to_csv("single_stream_aligned_windows.csv")
    res_list = []
    for row in raw_df.itertuples():
        end_time = get_end_from_start_time_npoints(
            start=start_time, n_points=row.n_points, freq=sampling_freq
        )
        res = func_dict.get(row.func_name, KeyError)(my_stream, start_time, end_time, 0)
        tmp = row._asdict()
        tmp.update(res)
        res_list.append(tmp)
    print("done with single stream raw values")
    pd.DataFrame(res_list).to_csv("single_stream_raw_val.csv")


def bench_streamset_reads(
    matrix_filename: str,
    collection: str,
    conn_params: dict,
    start_time: str = "2023-01-01",
    sampling_freq: int = 1,
):
    """Run the benchmarks for the streamset read operations.

    Parameters
    ----------
    matrix_filename : str, required
        The JSON filename containing the streamset benchmark parameters
    collection : str, required
        The stream collection to run the benchmarks against
    conn_params : Dict[str, str], required
        The connection parameters to the btrdb server for benchmarking.
    start_time : str, optional, default = '2023-01-01'
        The start time for data reading, in ISO datetime format
    sampling_freq : int, optional, default = 1
        The sampling frequency of the data, in Hz
    """
    df = get_matrix_df(matrix_filename)
    ins_df, raw_df, win_df, align_df = _process_df(df)
    conn = btrdb.connect(**conn_params)
    res_list = []
    start_time = btrdb.utils.timez.to_nanoseconds(start_time)
    for row in win_df.itertuples():
        my_streams = btrdb.stream.StreamSet(
            conn.streams_in_collection(collection)[0 : row.n_streams]
        )
        end_time = get_end_from_start_time_npoints(
            start=start_time, n_points=row.n_points, freq=sampling_freq
        )
        res = func_dict.get(row.func_name, KeyError)(
            my_streams, start_time, end_time, row.width_ns, 0
        )
        tmp = row._asdict()
        tmp.update(res)
        res_list.append(tmp)
    print("done with streamset windows")
    pd.DataFrame(res_list).to_csv("streamset_windows.csv")
    # res_list = []
    # for row in align_df.itertuples():
    #     end_time = get_end_from_start_time_npoints(start=start_time, n_points=row.n_points, freq=sampling_freq)
    #     res = func_dict.get(row.func_name,KeyError)(my_stream, start_time, end_time, row.pw, 0)
    #     tmp = row._asdict()
    #     tmp.update(res)
    #     res_list.append(tmp)
    # print("done with single stream aligned windows")
    # pd.DataFrame(res_list).to_csv('streamset_aligned_windows.csv')
    # res_list = []
    # for row in raw_df.itertuples():
    #     end_time = get_end_from_start_time_npoints(start=start_time, n_points=row.n_points, freq=sampling_freq)
    #     res = func_dict.get(row.func_name,KeyError)(my_stream, start_time, end_time, 0)
    #     tmp = row._asdict()
    #     tmp.update(res)
    #     res_list.append(tmp)
    # print("done with single stream raw values")
    # pd.DataFrame(res_list).to_csv('streamset_raw_val.csv')


def main():
    conn_params = {"profile": "andy"}
    coll = "jbg_bench_read"
    name_prefix = "foo"
    n_points = 10000000
    n_streams = 100
    # read_fn = "single_stream_bench_list.json"
    read_fn = "streamset_bench_list.json"
    start_time = "2023-01-01"
    # bench_single_stream_reads(
    #     matrix_filename=read_fn,
    #     collection=coll,
    #     start_time=start_time,
    #     conn_params=conn_params,
    #     sampling_freq=1,
    # )
    bench_streamset_reads(
        matrix_filename=read_fn,
        collection=coll,
        start_time=start_time,
        conn_params=conn_params,
        sampling_freq=1,
    )
    # THIS OPERATION TAKES A WHILE, Creating 1B points, which is 1B * 16bytes ~ 16GB of data to create and then insert
    # ONLY UNCOMMENT IF YOUR MACHINE CAN HANDLE CREATING THIS MUCH DATA
    # setup_read_streams(coll=coll, n_streams=n_streams, n_points=n_points, conn_params=conn_params, n_proc=4)
    # conn = btrdb.connect(**conn_params)
    # strmset = btrdb.stream.StreamSet(conn.streams_in_collection(coll))
    # print(strmset.count())
    # start = btrdb.utils.timez.to_nanoseconds("2023-01-01")
    # strmset = strmset.filter(
    #     start=start,
    #     end=get_end_from_start_time_npoints(start=start, n_points=1000000, freq=1),
    #     sampling_frequency=None,
    # )
    # tic = time.perf_counter()
    # print(strmset.arrow_to_dataframe())
    # toc = time.perf_counter()
    # print(toc - tic)


if __name__ == "__main__":
    main()
