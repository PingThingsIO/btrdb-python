import numpy as np
import pandas as pd
import pyarrow as pa


def make_data(n: int = 10000) -> pa.Table:
    gen = np.random.default_rng()
    dat = gen.random(size=n)
    freq = "1S"
    time = pd.date_range(start="2023-01-01 00:00:00", periods=n, freq=freq).astype(int)
    df = pd.DataFrame(data=[time.values, dat]).T
    df = df.rename(columns={0: "time", 1: "value"})
    df["time"] = df["time"].astype(int)
    # print(time)
    # print(dat)
    # print(df.info())
    return pa.Table.from_pandas(
        df=df,
    )
