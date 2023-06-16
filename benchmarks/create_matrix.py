import json
from itertools import product

import benchmarks.utils.benchmark_stream_reads as stream_read
from benchmarks.utils.data_create import make_data

n_points = [10_000, 100_000, 1_000_000, 3_000_000, 5_000_000, 10_000_000]
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

n_repeats = 5

bench_list = list()
for num_points, my_func, num_repeat in product(
    n_points, [*arrow_methods, *prev_methods], range(n_repeats)
):
    # print(num_points, my_func.__name__, num_repeat)
    tmp = {"n_points": num_points, "func_name": my_func.__name__, "replica": num_repeat}
    bench_list.append(tmp)

print(bench_list)
with open("bench_list.json", "w") as fp:
    json.dump(fp=fp, obj=bench_list)
