import btrdb
import btrdb.stream
import uuid
import time
import math
import string

def stream_idx_to_name(n):
	charset='abcdefghijklmnopqrstuvwxyz'
	s = ''
	while True:
		s += charset[n%len(charset)]
		if n == 0:
			break
		n = n//len(charset)
	return s

test_collection = 'async-multistream-perf-test'
n_streams = 1000
n_datapoints_per_stream = 900 # larger than btrdbs batch limit
# assert(stream.INSERT_BATCH_SIZE > n_datapoints_per_stream) # Ensure we are testing batching.
batch_interval_ns = 60*60*(10**9) # 1 hour.

timestep_ns = batch_interval_ns//n_datapoints_per_stream

t_start = 1500000000000000000
t_end = t_start + batch_interval_ns

conn = btrdb.connect()

# XXX Obliterating streams slow for now...
#rows = conn.query('select * from streams where collection = $1;', [test_collection])
#existing_streams = conn.streams(*[uuid.UUID(row['uuid']) for row in rows])
#if len(existing_streams) > 0:
#	print("obliterating old test streams...")
#	obliterate_start_t = time.perf_counter()
#	existing_streams.obliterate()
#	obliterate_end_t = time.perf_counter()
#	obliterate_duration = obliterate_end_t - obliterate_start_t 
#	print(f"obliterated {len(rows)} streams in {obliterate_duration} seconds ({len(rows)/obliterate_duration} ops)")

stream_names = [stream_idx_to_name(i) for i in range(n_streams)]

print("querying which streams already exist...")
query_start_t = time.perf_counter()
query_results = conn.batch_query([
	btrdb.BatchSQLQueryArgs(
		stmt='select * from streams where collection=$1 and name=$2;',
		params=[test_collection, name],
	) for name in stream_names
])
exists_by_name = {}
for rows in query_results:
	for row in rows:
		exists_by_name[row['name']] = True 
query_end_t = time.perf_counter()
query_duration = query_end_t - query_start_t
print(f"queried {len(exists_by_name)} streams in {query_duration} seconds ({n_streams/query_duration} qps)")

create_args = [
	btrdb.BatchCreateArgs(
		uuid.uuid4(), test_collection, tags={"name":name},
	) for name in stream_names if not exists_by_name.get(name)
]
if len(create_args) > 0:
	print("creating missing streams...")
	create_start_t = time.perf_counter()
	conn.batch_create(create_args)
	create_end_t = time.perf_counter()
	create_duration = create_end_t - create_start_t
	print(f"created {len(create_args)} streams in {create_duration} seconds ({len(create_args)/create_duration} cps)")
else:
	print("no streams needed creating")

print("querying uuids for streams...")
uuid_rows = conn.batch_query([
	btrdb.BatchSQLQueryArgs(
		stmt='select uuid from streams where collection=$1 and name=$2;',
		params=[test_collection, name],
	) for name in stream_names
])
streams = conn.streams(*[uuid.UUID(rows[0]['uuid']) for rows in uuid_rows])

# Slow for now, but less of an issue.
print("deleting old test data from streams...")
delete_start_t = time.perf_counter()
streams.delete(t_start, t_end)
delete_end_t = time.perf_counter()
delete_duration = delete_end_t - delete_start_t
total_points=n_streams*n_datapoints_per_stream
print(f"deleted test data from {len(streams)} streams in {delete_duration} seconds ({len(streams)/delete_duration} dps)")

print("generating test data...")
all_data = {}
for s in streams:
	data = []
	for p in range(n_datapoints_per_stream):
		t = t_start + p*timestep_ns
		v = math.sin(t)
		data.append([t_start + p*timestep_ns, v])
	all_data[s.uuid] = data

print("inserting into streams...")
insert_start_t = time.perf_counter()
streams.insert(all_data)
insert_end_t = time.perf_counter()
insert_duration = insert_end_t - insert_start_t
total_points=n_streams*n_datapoints_per_stream
print(f"inserted {total_points} points in {insert_duration} seconds ({total_points/insert_duration} pps)")

print("pinning versions...")
streams.pin_versions()

print("querying stream data...") 
query_start_t = time.perf_counter()
returned_data = streams.filter(start=t_start, end=t_end).values()
query_end_t = time.perf_counter()
query_duration = query_end_t - query_start_t 
print(f"queried {total_points} points in {query_duration} seconds ({total_points/query_duration} pps)")

print("verifying downloaded data matches uploaded data...")
if len(returned_data) != len(all_data):
	raise Exception("number of streams in data differs")

for (d1, s) in zip(returned_data, streams):
	d2 = all_data[s.uuid]
	if len(d1) != len(d2):
		print("t_start, t_end, timestep -", t_start, t_end, timestep_ns)
		print("got data - idx t v expected:")
		for (i, p) in enumerate(d1):
			print(i, p.time, p.value, math.sin(p.time))
		print("expected data - idx t v expected:")
		for (i, p) in enumerate(d2):
			print(i, p[0], p[1], math.sin(p[0]))
		raise Exception(f"data len differs {len(d1)} != {len(d2)}")
	for (p1, p2) in zip(d1, d2):
		if p1.time != p2[0]:
			raise Exception(f"times differ {p1.time} != {p2[0]}")
		if p1.value != p2[1]:
			raise Exception(f"points differ {p1.values} != {p2[1]}")


#print("flushing streams...")
#flush_start_t = time.perf_counter()
#versions = streams.flush()
#flush_end_t = time.perf_counter()
#flush_duration = flush_end_t - flush_start_t
#print(f"flushed {n_streams} streams in {flush_duration} seconds ({n_streams/flush_duration} fps)")

