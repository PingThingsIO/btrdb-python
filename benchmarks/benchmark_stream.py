import uuid
from time import perf_counter
from typing import Dict, Union

import btrdb


def time_single_stream_raw_values(
    stream: btrdb.stream.Stream, start: int, end: int, version: int = 0
) -> Dict[str, Union[int, str]]:
    """Return the elapsed time for the stream raw values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return raw values.
    start : int, required
        The start time (in nanoseconds) to return raw values (inclusive).
    end : int, required
        The end time (in nanoseconds) to return raw values (exclusive)
    version : int, optional, default : 0
        The version of the stream to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict[str, int]
        The performance results of the stream method
    """
    expected_count = stream.count(start, end) - 1
    tic = perf_counter()
    vals = stream.values(start, end, version=version)
    toc = perf_counter()
    # minus 1 to account for the exclusive end time
    queried_points = len(vals)
    assert queried_points == expected_count
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(stream.uuid, point_count=queried_points, total_time=run_time)
    return results


def time_single_stream_arrow_raw_values(
    stream: btrdb.stream.Stream, start: int, end: int, version: int = 0
) -> Dict[str, Union[str, int, float]]:
    """Return the elapsed time for the stream arrow raw values query

    Parameters
    ----------
    stream : btrdb.Stream, required
        The data stream to return the raw data as an arrow table.
    start : int, required
        The start time (in nanoseconds) to return raw values (inclusive).
    end : int, required
        The end time (in nanoseconds) to return raw values (exclusive)
    version : int, optional, default : 0
        The version of the stream to query for points.

    Notes
    -----
    The data points returned will be [start, end)

    Returns
    -------
    results : dict[str, int]
        The performance results of the stream method
    """
    # minus 1 to account for the exclusive end time for the values query
    expected_count = stream.count(start, end) - 1
    tic = perf_counter()
    vals = stream.arrow_values(start, end, version=version)
    toc = perf_counter()
    # num of rows
    queried_points = vals.shape[0]
    assert queried_points == expected_count
    # time in seconds to run
    run_time = toc - tic
    results = _create_stream_result_dict(stream.uuid, point_count=queried_points, total_time=run_time)
    return results


def _create_stream_result_dict(
    uu: uuid.UUID, point_count: int, total_time: float
) -> Dict[str, Union[str, int, float]]:
    return {
        "uuid": str(uu),
        "total_points": point_count,
        "total_time_seconds": total_time,
    }
