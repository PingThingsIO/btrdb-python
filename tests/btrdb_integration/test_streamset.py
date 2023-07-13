import logging
from math import isnan, nan
from uuid import uuid4 as new_uuid

import pytest

import btrdb
import btrdb.stream
from btrdb.utils.timez import currently_as_ns

try:
    import pyarrow as pa
except ImportError:
    pa = None
try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import polars as pl
except ImportError:
    pl = None


def test_streamset_values(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.values()
    assert len(values) == 2
    assert len(values[0]) == len(t1)
    assert len(values[1]) == len(t2)
    assert [(p.time, p.value) for p in values[0]] == list(zip(t1, d1))
    assert [(p.time, p.value) for p in values[1]] == list(zip(t2, d2))


def test_streamset_arrow_windows_vs_windows(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = (
        btrdb.stream.StreamSet([s1, s2])
        .filter(start=100, end=121)
        .windows(width=btrdb.utils.timez.ns_delta(nanoseconds=10))
    )
    values_arrow = ss.arrow_to_dataframe()
    values_prev = ss.to_dataframe()
    assert values_arrow.equals(values_prev)


def test_streamset_arrow_values(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_col1 = [0.0, None, 1.0, None, 2.0, None, 3.0, None, 4.0]
    expected_col2 = [None, 5.0, None, 6.0, 7.0, 8.0, None, 9.0, None]
    expected_schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(tmp_collection + "/" + "s1", pa.float64(), nullable=False),
            pa.field(tmp_collection + "/" + "s2", pa.float64(), nullable=False),
        ]
    )
    values = ss.arrow_values()
    times = [t.value for t in values["time"]]
    col1 = [
        None if isnan(v.as_py()) else v.as_py() for v in values[tmp_collection + "/s1"]
    ]
    col2 = [
        None if isnan(v.as_py()) else v.as_py() for v in values[tmp_collection + "/s2"]
    ]
    assert times == expected_times
    assert col1 == expected_col1
    assert col2 == expected_col2
    assert expected_schema.equals(values.schema)


def test_streamset_empty_arrow_values(conn, tmp_collection):
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    ss = btrdb.stream.StreamSet([s]).filter(start=100, end=200)
    values = ss.arrow_values()
    expected_schema = pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field(tmp_collection + "/" + "s", pa.float64(), nullable=False),
        ]
    )
    assert [t.value for t in values["time"]] == []
    assert [v for v in values[tmp_collection + "/s"]] == []
    assert expected_schema.equals(values.schema)


def test_streamset_to_dataframe(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.to_dataframe()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_col1 = [0.0, None, 1.0, None, 2.0, None, 3.0, None, 4.0]
    expected_col2 = [None, 5.0, None, 6.0, 7.0, 8.0, None, 9.0, None]
    expected_dat = {
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    expected_df = pd.DataFrame(expected_dat, index=expected_times)
    assert values.equals(expected_df)


def test_arrow_streamset_to_dataframe(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.arrow_to_dataframe()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_times = [
        pa.scalar(v, type=pa.timestamp("ns", tz="UTC")).as_py() for v in expected_times
    ]
    expected_col1 = [0.0, None, 1.0, None, 2.0, None, 3.0, None, 4.0]
    expected_col2 = [None, 5.0, None, 6.0, 7.0, 8.0, None, 9.0, None]
    expected_dat = {
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    expected_df = pd.DataFrame(expected_dat, index=pd.DatetimeIndex(expected_times))
    assert values.equals(expected_df)


def test_arrow_streamset_to_polars(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values = ss.arrow_to_polars()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_times = [
        pa.scalar(v, type=pa.timestamp("ns", tz="UTC")).as_py() for v in expected_times
    ]
    expected_col1 = [0.0, None, 1.0, None, 2.0, None, 3.0, None, 4.0]
    expected_col2 = [None, 5.0, None, 6.0, 7.0, 8.0, None, 9.0, None]
    expected_dat = {
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    expected_df = pd.DataFrame(
        expected_dat, index=pd.DatetimeIndex(expected_times)
    ).reset_index(names="time")
    expected_df_pl = pl.from_pandas(expected_df)
    assert values.frame_equal(expected_df_pl)


def test_streamset_arrow_polars_vs_old_to_polars(conn, tmp_collection):
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    t1 = [100, 105, 110, 115, 120]
    t2 = [101, 106, 110, 114, 119]
    d1 = [0.0, 1.0, 2.0, 3.0, 4.0]
    d2 = [5.0, 6.0, 7.0, 8.0, 9.0]
    s1.insert(list(zip(t1, d1)))
    s2.insert(list(zip(t2, d2)))
    ss = btrdb.stream.StreamSet([s1, s2]).filter(start=100, end=121)
    values_arrow = ss.arrow_to_polars()
    values_non_arrow = ss.to_polars()
    expected_times = [100, 101, 105, 106, 110, 114, 115, 119, 120]
    expected_times = [
        pa.scalar(v, type=pa.timestamp("ns", tz="UTC")).as_py() for v in expected_times
    ]
    expected_col1 = [0.0, None, 1.0, None, 2.0, None, 3.0, None, 4.0]
    expected_col2 = [None, 5.0, None, 6.0, 7.0, 8.0, None, 9.0, None]
    expected_dat = {
        tmp_collection + "/s1": expected_col1,
        tmp_collection + "/s2": expected_col2,
    }
    expected_df = pd.DataFrame(
        expected_dat, index=pd.DatetimeIndex(expected_times)
    ).reset_index(names="time")
    expected_df_pl = pl.from_pandas(expected_df)
    assert values_arrow.frame_equal(expected_df_pl)
    assert values_non_arrow.frame_equal(expected_df_pl)


def test_timesnap_backward_extends_range(conn, tmp_collection):
    sec = 10**9
    tv1 = [
        [int(0.5 * sec), 0.5],
        [2 * sec, 2.0],
    ]
    tv2 = [
        [int(0.5 * sec) - 1, 0.5],
        [2 * sec, 2.0],
    ]
    tv3 = [
        [1 * sec, 1.0],
        [2 * sec, 2.0],
    ]
    s1 = conn.create(new_uuid(), tmp_collection, tags={"name": "s1"})
    s2 = conn.create(new_uuid(), tmp_collection, tags={"name": "s2"})
    s3 = conn.create(new_uuid(), tmp_collection, tags={"name": "s3"})
    s1.insert(tv1)
    s2.insert(tv2)
    s3.insert(tv3)
    ss = btrdb.stream.StreamSet([s1, s2, s3]).filter(
        start=1 * sec, end=3 * sec, sampling_frequency=1
    )
    values = ss.arrow_values()
    assert [1 * sec, 2 * sec] == [t.value for t in values["time"]]
    assert [0.5, 2.0] == [v.as_py() for v in values[tmp_collection + "/s1"]]
    assert [None, 2.0] == [
        None if isnan(v.as_py()) else v.as_py() for v in values[tmp_collection + "/s2"]
    ]
    assert [1.0, 2.0] == [v.as_py() for v in values[tmp_collection + "/s3"]]


def test_timesnap_forward_restricts_range(conn, tmp_collection):
    sec = 10**9
    tv = [
        [1 * sec, 1.0],
        [2 * sec, 2.0],
        [int(2.75 * sec), 2.75],
    ]
    s = conn.create(new_uuid(), tmp_collection, tags={"name": "s"})
    s.insert(tv)
    ss = btrdb.stream.StreamSet([s]).filter(start=1 * sec, sampling_frequency=1)
    values = ss.filter(end=int(3.0 * sec)).arrow_values()
    assert [1 * sec, 2 * sec] == [t.value for t in values["time"]]
    assert [1.0, 2.0] == [v.as_py() for v in values[tmp_collection + "/s"]]
    # Same result if skipping past end instead of to end.
    assert values == ss.filter(end=int(2.9 * sec)).arrow_values()
