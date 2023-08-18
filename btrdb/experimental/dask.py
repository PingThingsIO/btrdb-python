import btrdb
import btrdb.exceptions
import dask
import dask.distributed
import pandas as pd

# This process local connection variable is initialized in all
# dask worker processes by the configure function.
_btrdb_conn = None


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
    conn = _btrdb_conn
    if conn is None:
        raise btrdb.exceptions.ConnectionError(
            "call configure to configure btrdb credentials for the cluster"
        )
    return conn


class BtrdbConnectionPlugin(dask.distributed.WorkerPlugin):
    """
    A Dask Worker Plugin to establish a connection with BtrDB.

    This plugin, when added to a Dask worker, initializes a connection
    to BtrDB using the provided endpoints and API key. This connection
    is then globally available for use within the worker's tasks.

    Notes:
        This plugin should not be used directly, and instead be used via `configure`.
    """

    def __init__(self, endpoints=None, apikey=None):
        self._endpoints = endpoints
        self._apikey = apikey

    def setup(self, worker):
        global _btrdb_conn
        _btrdb_conn = btrdb._connect(endpoints=self._endpoints, apikey=self._apikey)


def configure(client=None, conn_str=None, apikey=None, profile=None):
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
        global _btrdb_conn
        _btrdb_conn = btrdb.connect(conn_str=conn_str, apikey=apikey, profile=profile)
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
        plugin = BtrdbConnectionPlugin(**creds)
        client.register_worker_plugin(plugin, name="btrdb_connection")
