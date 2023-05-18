import dask
import dask.distributed
import btrdb
import btrdb.utils.dask
import time


if __name__ == '__main__':
	cluster = dask.distributed.LocalCluster(n_workers=32,threads_per_worker=1)
	client = dask.distributed.Client(cluster)
	btrdb.utils.dask.configure_cluster(client)
	conn = btrdb.connect()
	streams = conn.streams_in_collection("andy")[:10]
	start = streams[0].earliest()[0].time
	end = start + 14*24*60*60*(10**9) # streams[0].latest()[0].time-1
	t1=time.time()
	sub_sums = []
	for stream in streams:
		v = stream.dask_values(start=start, end=end, partitions=2)
		sub_sums.append(v.shape[0])
	total = dask.delayed(sum)(sub_sums)
	#total.visualize(filename="sum.svg", optimize_graph=True)
	print(total.compute())
	t2=time.time()
	print(t2-t1)
	client.shutdown()
	cluster.close()
