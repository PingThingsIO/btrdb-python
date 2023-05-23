import dask
import dask.distributed
import dask.dataframe
import pyarrow as pa
import pandas as pd

from btrdb.conn import Connection
from btrdb.endpoint import Endpoint
from btrdb.exceptions import ConnectionError
from btrdb.stream import Stream
from btrdb.utils.credentials import credentials_by_profile, credentials

_ep = None

def _get_ep():
    ep = _ep
    if ep is None:
        raise Exception(
            "you must call btrdb.dask.configure_cluster after starting your dask client"
        )
    return ep

def _connect(endpoints=None, apikey=None):
    global _ep
    _ep = Endpoint(Connection(endpoints, apikey=apikey).channel)

def _configure_local(endpoints=None, apikey=None):
    global _ep
    _ep = Endpoint(Connection(endpoints, apikey=apikey).channel)

class _BtrdbConnectionPlugin(dask.distributed.WorkerPlugin):
    
    def __init__(self, endpoints=None, apikey=None):
        self.endpoints = endpoints
        self.apikey = apikey
    
    def setup(self, worker):
        _configure_local(endpoints=self.endpoints, apikey=self.apikey)

def configure_cluster(dask_client=None, conn_str=None, apikey=None, profile=None):
    """
    Connect to a BTrDB server across the dask cluster, sharing the local
    credentials with each dask worker.

    Parameters
    ----------
    dask_client: dask.distributed.Client, default=None
        The dask client to configure the connection credentials for.
        If set to None, will use the default dask client.
    conn_str: str, default=None
        The address and port of the cluster to connect to, e.g. `192.168.1.1:4411`.
        If set to None, will look in the environment variable `$BTRDB_ENDPOINTS`
        (recommended).
    apikey: str, default=None
        The API key used to authenticate requests (optional). If None, the key
        is looked up from the environment variable `$BTRDB_API_KEY`.
    profile: str, default=None
        The name of a profile containing the required connection information as
        found in the user's predictive grid credentials file
        `~/.predictivegrid/credentials.yaml`.

    Returns
    -------
    None
    """
    if conn_str and profile:
        raise ValueError("Received both conn_str and profile arguments.")
    creds = None
    if profile != None:
        creds = credentials_by_profile(profile)
    else:
        creds = credentials(conn_str, apikey)
    if dask_client is None:
        try:
            dask_client = dask.distributed.get_client()
        except ValueError:
            pass
    if dask_client is None:
        _configure_local(endpoints=creds.get("endpoints"), apikey=creds.get("apikey"))
    else:
        # Configure the distributed scheduler.
        plugin = _BtrdbConnectionPlugin(endpoints=creds.get("endpoints"), apikey=creds.get("apikey"))
        dask_client.register_worker_plugin(plugin, name="btrdb_connection")

def _arrow_raw_values(uuid, start, end, version):
    ep = _get_ep()
    arr_bytes = ep.arrowRawValues(
        uu=uuid, start=start, end=end, version=version
    )
    table_list = []
    for b, _ in  arr_bytes:
        with pa.ipc.open_stream(b) as reader:
            schema = reader.schema
            table_list.append(reader.read_all())
    return pa.concat_tables(table_list)

@dask.delayed(traverse=False)
def _values_as_delayed(uuid, start, end, version, value_col):
    df = _arrow_raw_values(uuid, start, end, version).to_pandas()
    if value_col != "value":
        df.rename(columns={"value":value_col}, inplace=True)
    df.set_index('time', inplace=True)
    df = df.loc[~df.index.duplicated()]
    return df

@dask.delayed(traverse=False)
def _snapped_values_as_delayed(uuid, start, end, period, version, value_col):
    extended_start = max(start-period, 0)
    extended_end = end+period
    df = _arrow_raw_values(uuid, extended_start, extended_end, version).to_pandas()
    if value_col != "value":
        df.rename(columns={"value":value_col}, inplace=True)
    df.set_index('time', inplace=True)
    df = df.loc[~df.index.duplicated()]
    aligned_index = pd.date_range(start=start, end=end-1, freq=pd.to_timedelta(period, unit='ns'), tz=df.index.tz, unit='ns')
    joined_index = aligned_index.union(df.index)
    sorted_index = joined_index.sort_values() # XXX union(...,sort=True) was not working?
    df = df.reindex(joined_index)
    df.interpolate(method='nearest', limit=2, inplace=True)
    df = df.loc[aligned_index]
    return df

def values(
        stream, start, end, version=0,
        snap_freq=None, snap_period=None,
        partitions=1, value_col='value'
    ):
    if snap_freq != None:
        snap_period = int((1 / snap_freq) * 1e9)
    partition_size = (end-start)//partitions
    if snap_period != None and (partition_size % snap_period != 0):
        # If we are snapping, we must ensure the partition_size is a multiple of snap_period.
        partition_size += snap_period - (partition_size % snap_period)
    parts = []
    divisions = []
    for part_start in range(start, end, partition_size):
        part_end = min(end, part_start+partition_size)
        if snap_period != None:
            parts.append(_snapped_values_as_delayed(
                stream.uuid, part_start, part_end, snap_period, version, value_col
            ))
        else:
            parts.append(_values_as_delayed(
                stream.uuid, part_start, part_end, version, value_col
            ))
        divisions.append(pd.to_datetime(part_start, unit='ns', utc=True))
    divisions.append(pd.to_datetime(end-1, unit='ns', utc=True))
    meta=pd.DataFrame(
        {value_col: pd.Series([], dtype='float64')},
        index=pd.Index([], name='time', dtype='datetime64[ns]')
    )
    return dask.dataframe.from_delayed(
        parts,
        meta=meta,
        divisions=divisions,
    )

def multi_values(
        streams, start, end, version_mapper=lambda s : 0,
        snap_freq=None, snap_period=None,
        partitions=1, column_mapper=lambda s : s.name
    ):
    if len(streams) == 0:
        raise ValueError("empty list of streams")
    if snap_freq != None:
        snap_period = int((1 / snap_freq) * 1e9)
    to_join = [
        values(
            stream, start, end,
            version=version_mapper(stream),
            snap_period=snap_period,
            partitions=partitions,
            value_col=column_mapper(stream)
        )
        for stream in streams
    ]
    df = to_join[0]
    join_method='left'
    if snap_period is None:
        join_method = 'outer'
    for other in to_join[1:]:
        df = df.join(other, how=join_method)
    return df

def _arrow_windows(uuid, start, end, width, depth, version):
    ep = _get_ep()
    arr_bytes = ep.arrowWindows(
        uu=uuid, start=start, end=end, width=width, depth=depth, version=version
    )
    table_list = []
    for b, _ in  arr_bytes:
        with pa.ipc.open_stream(b) as reader:
            schema = reader.schema
            table_list.append(reader.read_all())
    return pa.concat_tables(table_list)

@dask.delayed(traverse=False)
def _windows_as_delayed(uuid, start, end, width, depth, version, col_prefix):
    df = _arrow_windows(uuid, start, end, width, depth, version).to_pandas()
    if col_prefix != "":
        remapped = {
            "mean":col_prefix+"mean",
            "min":col_prefix+"min",
            "max":col_prefix+"max",
            "count":col_prefix+"count",
            "stddev":col_prefix+"stddev",
        }
        df.rename(columns=remapped, inplace=True)
    df.set_index('time', inplace=True)
    return df

def windows(stream, start, end, width, depth=0, version=0, partitions=1, col_prefix=""):
    partition_size = (end-start)//partitions
    if (partition_size % width != 0):
        # Ensure paritions are a multiple of width
        partition_size += width - (partition_size % width)
    parts = []
    divisions = []
    for part_start in range(start, end, partition_size):
        part_end = min(end, part_start+partition_size)
        parts.append(_windows_as_delayed(
            stream.uuid, part_start, part_end, width, depth, version, col_prefix
        ))
        divisions.append(pd.to_datetime(part_start, unit='ns', utc=True))
    divisions.append(pd.to_datetime(end-1, unit='ns', utc=True))
    meta=pd.DataFrame(
        {col_prefix+"mean": pd.Series([], dtype='float64'),
         col_prefix+"min": pd.Series([], dtype='float64'),
         col_prefix+"max": pd.Series([], dtype='float64'),
         col_prefix+"count": pd.Series([], dtype='int64'),
         col_prefix+"stddev": pd.Series([], dtype='float64')},
        index=pd.Index([], name='time', dtype='datetime64[ns]')
    )
    return dask.dataframe.from_delayed(
        parts,
        meta=meta,
        divisions=divisions,
    )