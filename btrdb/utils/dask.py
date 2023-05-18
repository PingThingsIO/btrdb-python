import btrdb
import dask
import pandas as pd
import pyarrow as pa
from dask.distributed import Client, WorkerPlugin, get_client

# Should this be a pool?
_cluster_connection = None

def btrdb_connection():
    conn = _cluster_connection
    if conn is None:
        raise Exception("call btrdb.utils.dask.configure_cluster")
    return conn

class BtrdbConnectionPlugin(WorkerPlugin):
    def __init__(self, conn_str=None, apikey=None, profile=None):
        self.conn_str = conn_str
        self.apikey = apikey
        self.profile = profile
    def setup(self, worker):
        global _cluster_connection
        _cluster_connection = btrdb.connect(conn_str=self.conn_str, apikey=None, profile=None)

def configure_cluster(client=None, conn_str=None, apikey=None, profile=None):
    if client is None:
        try:
            client = get_client()
        except ValueError:
            pass
    if client is None:
        # We have a threaded scheduler.
        global _cluster_connection
        _cluster_connection = btrdb.connect(conn_str=conn_str, apikey=apikey, profile=profile)
    else:
        # Configure the distributed scheduler.
        plugin = BtrdbConnectionPlugin(conn_str, apikey, profile)
        client.register_worker_plugin(plugin, name="btrdb_connection")

@dask.delayed()
def _values_as_delayed_pandas_frame(uuid, start, end, ver=0):
    conn = btrdb_connection()
    arr_bytes = conn.ep.arrowRawValues(
        uu=uuid, start=start, end=end, version=ver
    )
    table_list = []
    for b, _ in  arr_bytes:
        with pa.ipc.open_stream(b) as reader:
            schema = reader.schema
            table_list.append(reader.read_all())
    df = pa.concat_tables(table_list).to_pandas()
    df.set_index('time', inplace=True)
    return df


