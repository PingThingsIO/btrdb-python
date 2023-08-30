import btrdb
import btrdb.exceptions
import dask
import dask.distributed
import dask.dataframe
import pyarrow
import pandas
import random

# This process local connection variable is initialized in all
# dask worker processes by the configure function.
_btrdb_conns = []


def get_btrdb():
    """
    Retrieve the BtrDB connection object.

    This function returns a cached btrdb object that is local to
    the current dask node. Call this function from within your
    dask tasks instead of directly connecting to btrdb.

    Returns:
        object: The BtrDB connection object if established.

    Raises:
        btrdb.exceptions.ConnectionError: If the BtrDB connection has not been configured.

    Note:
        Ensure that the `configure` function is called to set up the
        BtrDB credentials before calling this function.
    """
    conns = _btrdb_conns
    if len(conns) == 0:
        raise btrdb.exceptions.ConnectionError(
            "call configure to configure btrdb credentials for the cluster"
        )
    return random.choice(conns)


class BtrdbConnectionPlugin(dask.distributed.WorkerPlugin):
    """
    A Dask Worker Plugin to establish a connection with BtrDB.

    This plugin, when added to a Dask worker, initializes a connection
    to BtrDB using the provided endpoints and API key. This connection
    is then globally available for use within the worker's tasks.

    Notes:
        This plugin should not be used directly, and instead be used via `configure`.
    """

    def __init__(self, connections=None, endpoints=None, apikey=None):
        self._connections = connections
        self._endpoints = endpoints
        self._apikey = apikey

    def setup(self, worker):
        global _btrdb_conns
        _btrdb_conns = [
            btrdb._connect(endpoints=self._endpoints, apikey=self._apikey)
            for i in range(self._connections)
        ]


def configure(client=None, conn_str=None, apikey=None, profile=None, connections=1):
    """
    Configure a btrdb connection on all worker nodes in the dask cluster.
    """
    if client is None:
        try:
            # Look for a default client.
            client = dask.distributed.get_client()
        except ValueError:
            pass
    if client is None:
        # We have a threaded scheduler.
        global _btrdb_conns
        _btrdb_conns = [
            btrdb.connect(conn_str=conn_str, apikey=apikey, profile=profile)
            for i in range(connections)
        ]
    else:
        if profile is not None:
            creds = btrdb.credentials_by_profile(profile)
        else:
            creds = btrdb.credentials(conn_str, apikey)

        if "endpoints" not in creds:
            raise btrdb.exceptions.ConnectionError(
                "Could not determine credentials to use."
            )

        # Configure the distributed scheduler.
        plugin = BtrdbConnectionPlugin(connections=connections, **creds)
        client.register_worker_plugin(plugin, name="btrdb_connection")


@dask.delayed
def _stream_as_dataframe_part(uuid, start, end, snap_period, data_column, version):
    db = get_btrdb()
    # For now we use multi values because it implements both timesnapping and does
    # not return duplicate values for a single timestamp. Both of these are useful
    # properties for how dask dataframe partitions are supposed to behave.
    values = db.ep.arrowMultiValues([uuid], start, end, [version], snap_period)
    values = [v for v in values]
    if len(values) != 0:
        values = pyarrow.concat_tables([v for v in values])
    else:
        schema = pa.schema(
            [
                pyarrow.field(
                    "time", pyarrow.timestamp("ns", tz="UTC"), nullable=False
                ),
                pyarrow.field(str(uuid), pyarrow.float64(), nullable=False),
            ]
        )
        values = pa.Table.from_arrays([pa.array([]), pa.array([])], schema=schema)
    # XXX ensure this is zero copy.
    values = values.rename_columns(["time", data_column])
    values = values.to_pandas()
    # XXX Can we do this from to_dataframe?
    values.set_index("time", inplace=True)
    return values


def stream_as_dataframe(
    stream,
    start=None,
    end=None,
    partitions=1,
    snap_period=0,
    data_column=None,
    version=0,
):
    """
    Converts a btrdb stream to a lazy Dask DataFrame.

    Parameters:
    ----------
    stream : btrdb.stream.Stream
        The stream containing the data.

    start : datetime-like, optional
        The start time for the data from the stream. Defaults to the earliest time in the stream.

    end : datetime-like, optional
        The end time for the data from the stream. Defaults to the latest time in the stream.

    partitions : int, optional
        Number of partitions for the dask dataframe. Default is 1.

    snap_period : int, optional
        The period for data time snapping.
        Defaults to 0, which means no snapping.

    data_column : str or callable, optional
        The name of the data column. If None, it defaults to the collection and name of the stream.
        If callable, the function is applied to the stream object to determine the data column name.

    version : int, optional
        The stream version to be used. Defaults to 0.

    Returns:
    --------
    Dask DataFrame
        The Dask DataFrame containing the stream data.
    """
    if data_column is None:
        data_column = stream.collection + "/" + stream.name
    else:
        if type(data_column) != str:
            # assume callable.
            data_column = data_column(s)
    if start is None:
        start = stream.earliest()[0].time
    if end is None:
        end = stream.latest()[0].time
    duration = end - start
    if partitions >= duration:
        partitions = 1
    part_duration = duration // partitions
    if snap_period != 0:
        # N.B. Due to the way the server does time snapping, we need to ensure that our partitions are aligned to the
        # timesnapping period. The reason for this is we don't want values to snapped into both partitions by accident.
        remainder = part_duration % snap_period
        if remainder != 0:
            part_duration += snap_period - remainder
    parts = []
    divisions = []
    part_start = start
    while part_start < end:
        part_end = min(part_start + part_duration, end)
        part = _stream_as_dataframe_part(
            stream.uuid, part_start, part_end, snap_period, data_column, version
        )
        parts.append(part)
        divisions.append(part_start)
        part_start += part_duration
    divisions.append(end)
    meta = pandas.DataFrame(
        index=pandas.DatetimeIndex([], tz="UTC"),
        columns=[data_column],
        dtype="float64",
    )
    return dask.dataframe.from_delayed(
        parts, meta=meta, divisions=divisions, verify_meta=False
    )
