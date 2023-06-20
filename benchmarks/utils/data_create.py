import numpy as np
import pandas as pd
import pyarrow as pa

import btrdb.utils.timez


def make_data(n: int = 10000) -> pa.Table:
    gen = np.random.default_rng()
    dat = gen.random(size=n)
    freq = "1S"
    time = pd.date_range(start="2023-01-01 00:00:00", periods=n, freq=freq).astype(int)
    df = pd.DataFrame(data=[time.values, dat]).T
    df = df.rename(columns={0: "time", 1: "value"})
    df["time"] = df["time"].astype(int)
    return pa.Table.from_pandas(
        df=df,
    )


def get_end_from_start_time_npoints(start: int, n_points: int, freq: int = 30):
    """Return the end time to use for the data retrieval if you want n_points returned.

    Parameters
    ----------
    start : int, required
        start time in nanoseconds
    n_points : int, required
        amount of points to return
    freq : int, optional, default=30
        Sampling rate of the data stream, in Hz

    Returns
    -------
    end : int
        The end time for the data query to return n_points of data
    """
    # 1/hz (1/(samples/second)) * (1e9 nanoseconds/second) -> nanoseconds/sample
    period_ns = int(np.ceil((1 / freq) * 1e9))
    print(f"For a sampling rate of {freq}Hz is {period_ns} nanoseconds/sample")

    delta_t_ns = int(np.ceil(period_ns * n_points))
    return start + delta_t_ns
