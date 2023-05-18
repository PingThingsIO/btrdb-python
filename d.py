import dask
import dask.distributed
import btrdb
import btrdb.utils.dask
import time


if __name__ == '__main__':
	cluster = dask.distributed.LocalCluster(n_workers=16,threads_per_worker=1)
	client = dask.distributed.Client(cluster)
	btrdb.utils.dask.configure_cluster(client)
	conn = btrdb.connect()
	streams = conn.streams_in_collection("andy", tags={'name':'ms0370'})
	stream = streams[0]
	start = stream.earliest()[0].time
	end = stream.latest()[0].time-1
	vals = stream.dask_values(start=start, end=end, partitions=16)
	t1=time.time()
	print(vals.shape[0].compute())
	t2=time.time()
	print(t2-t1)
