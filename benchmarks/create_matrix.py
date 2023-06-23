import itertools as it
import json
from typing import List

import benchmarks.utils.benchmark_stream_inserts as stream_inserts
import benchmarks.utils.benchmark_stream_reads as stream_read
import benchmarks.utils.benchmark_streamset_inserts as streamset_inserts
import benchmarks.utils.benchmark_streamset_reads as streamset_reads
import btrdb.utils.timez


def _create_param_dict(
    num_points, my_func, num_repeat, width_ns, pw, num_streams
) -> dict:
    tmp = {
        "n_points": num_points,
        "func_name": my_func.__name__,
        "replica": num_repeat,
        "n_streams": num_streams,
    }
    if "window" in my_func.__name__:
        if "aligned" in my_func.__name__:
            tmp["width_ns"] = None
            tmp["pw"] = pw
        else:
            tmp["width_ns"] = width_ns
            tmp["pw"] = None
    else:
        tmp["width_ns"] = None
        tmp["pw"] = None
    return tmp


def make_single_stream_benchmark_matrix(
    filename: str, pts: List[int], repeats: int, widths_ns: List[int], pws: List[int]
):
    """Create the json benchmark matrix for single stream operations.

    Parameters
    ----------
    filename : str, required
        The json filename to write out the benchmark parameters to.
    pts : List[int], required
        The list of amount of points to return for reading
    repeats : int, required
        How many replicas for statistics?
    widths_ns : List[int], required
        With window width parameters for the windows queries
    pws : List[int], required
        The pointwidths for the aligned_window queries
    """
    prev_methods = [
        stream_read.time_single_stream_raw_values,
        stream_read.time_single_stream_windows_values,
        stream_read.time_single_stream_aligned_windows_values,
    ]
    arrow_methods = [
        stream_read.time_single_stream_arrow_raw_values,
        stream_read.time_single_stream_arrow_windows_values,
        stream_read.time_single_stream_arrow_aligned_windows_values,
    ]
    arrow_ins_methods = [stream_inserts.time_stream_arrow_insert]
    stream_ins_methods = [stream_inserts.time_stream_insert]

    bench_list = list()
    for num_points, my_func, num_repeat, width_ns, pw in it.product(
        pts,
        [*arrow_methods, *prev_methods, *arrow_ins_methods, *stream_ins_methods],
        range(repeats),
        widths_ns,
        pws,
    ):
        tmp = _create_param_dict(
            num_points, my_func, num_repeat, width_ns, pw, num_streams=1
        )
        bench_list.append(tmp)

    with open(filename, "w") as fp:
        json.dump(fp=fp, obj=bench_list)


def make_streamset_benchmark_matrix(
    filename: str,
    pts: List[int],
    repeats: int,
    n_streams: List[int],
    widths_ns: List[int],
    pws: List[int],
):
    """Create the json benchmark matrix for streamset operations.

    Parameters
    ----------
    filename : str, required
        The json filename to write out the benchmark parameters to.
    pts : List[int], required
        The list of amount of points to return for reading
    repeats : int, required
        How many replicas for statistics?
    n_streams : List[int], required
        How many streams in the streamset?
    widths_ns : List[int], required
        With window width parameters for the windows queries
    pws : List[int], required
        The pointwidths for the aligned_window queries
    """
    prev_methods = [
        streamset_reads.time_streamset_raw_values,
        streamset_reads.time_streamset_windows_values,
        streamset_reads.time_streamset_aligned_windows_values,
    ]
    arrow_methods = [
        streamset_reads.time_streamset_arrow_raw_values,
        streamset_reads.time_streamset_arrow_windows_values,
        streamset_reads.time_streamset_arrow_aligned_windows_values,
        streamset_reads.time_streamset_arrow_multistream_raw_values_non_timesnapped,
        streamset_reads.time_streamset_arrow_multistream_raw_values_timesnapped,
    ]
    arrow_ins_methods = [streamset_inserts.time_streamset_arrow_inserts]
    stream_ins_methods = [streamset_inserts.time_streamset_inserts]

    bench_list = list()
    for num_points, my_func, num_repeat, num_streams, width_ns, pw in it.product(
        pts,
        [*arrow_methods, *prev_methods, *arrow_ins_methods, *stream_ins_methods],
        range(repeats),
        n_streams,
        widths_ns,
        pws,
    ):
        # print(num_points, my_func.__name__, num_repeat)
        if num_points > 1_000_000:
            if "arrow" not in my_func.__name__:
                continue
        tmp = _create_param_dict(
            num_points, my_func, num_repeat, width_ns, pw, num_streams
        )
        bench_list.append(tmp)

    with open(filename, "w") as fp:
        json.dump(fp=fp, obj=bench_list)


def main():
    n_points = [10_000, 100_000, 1_000_000, 3_000_000, 5_000_000, 10_000_000]
    n_repeats = 5
    n_streams = [1, 5, 10, 20, 50, 100]
    window_width_ns = [
        btrdb.utils.timez.ns_delta(minutes=1),
        btrdb.utils.timez.ns_delta(minutes=5),
    ]
    aligned_window_pw = [36, 38]  # 1.15min, 4.58min
    stream_fn = "single_stream_bench_list.json"
    streamset_fn = "streamset_bench_list_modified.json"
    make_single_stream_benchmark_matrix(
        filename=stream_fn,
        pts=n_points,
        repeats=n_repeats,
        widths_ns=window_width_ns,
        pws=aligned_window_pw,
    )
    make_streamset_benchmark_matrix(
        filename=streamset_fn,
        pts=n_points,
        repeats=n_repeats,
        n_streams=n_streams,
        widths_ns=window_width_ns,
        pws=aligned_window_pw,
    )


if __name__ == "__main__":
    main()
