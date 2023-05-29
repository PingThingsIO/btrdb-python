import dask
import dask.distributed
import btrdb
import btrdb.stream
import btrdb.parload
import btrdb.dask
import btrdb.utils.credentials
import pandas as pd
import time

COLLECTION="andy"
N_STREAMS=50
DURATION=7*24*60*60*(10**9)

def do_dask():
    import btrdb
    #cluster = dask.distributed.LocalCluster()
    #client = dask.distributed.Client(cluster)
    client = dask.distributed.Client("localhost:8786")
    btrdb.dask.configure_cluster()
    #print(client.dashboard_link)
    conn = btrdb.connect()
    streams = sorted(conn.streams_in_collection(COLLECTION), key=lambda s : s.name)
    streams = streams[:N_STREAMS]
    start = streams[0].earliest()[0].time
    end = start + DURATION
    df = btrdb.dask.multi_values(
        streams, start, end, partitions=3
    )
    delayed_npoints = dask.delayed(sum)([df[column].count() for column in df.columns])
    t1=time.time()
    npoints = delayed_npoints.compute()
    t2=time.time()
    elapsed=t2-t1
    print(f"dask: {npoints} in {elapsed} ({npoints/elapsed} PPS)")
    #client.shutdown()
    #cluster.close()

def do_threaded():
    conn = btrdb.connect()
    streams = sorted(conn.streams_in_collection(COLLECTION), key=lambda s : s.name)
    streams = streams[:N_STREAMS]
    streams = btrdb.stream.StreamSet(streams)
    start = streams[0].earliest()[0].time
    end = start + DURATION
    streams = streams.filter(start=start, end=end)
    t1=time.time()
    arrow = streams.values()
    t2=time.time()
    elapsed=t2-t1
    npoints=arrow.num_rows*arrow.num_columns
    print(f"threaded: {npoints} in {elapsed} ({npoints/elapsed} PPS)")

def do_parload():
    conn = btrdb.connect()
    streams = sorted(conn.streams_in_collection(COLLECTION), key=lambda s : s.name)
    streams = streams[:N_STREAMS]
    uuids = [str(stream.uuid) for stream in streams]
    start = streams[0].earliest()[0].time
    end = start + DURATION
    creds=btrdb.utils.credentials.credentials()
    pl = btrdb.parload.Parload(
        endpoint=creds.get('endpoints'),
        apikey=creds.get('apikey'),
        parload_path='./parload',
        num_workers=32,
    )
    t1 = time.time()
    values = pl.values_raw(uuids, start, end)
    t2 = time.time()
    elapsed = t2-t1
    npoints = 0
    for row in values[1]:
        npoints += len(row[0])
    print(f"parload: {npoints} in {elapsed} seconds ({npoints/elapsed} PPS)")


if __name__ == '__main__':
   #do_parload()
   #do_threaded()
   do_dask()
