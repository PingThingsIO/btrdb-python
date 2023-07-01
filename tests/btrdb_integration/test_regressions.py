import pytest
from uuid import uuid4 as new_uuid

@pytest.mark.xfail
def test_create_count_obliterate_concurrency_bug(conn, tmp_collection):
    # This mark this test as failed, but don't run it as it
    # currently crashes btrdb, it needs to be resolved and
    # enabled.
    assert False
    from concurrent.futures import ThreadPoolExecutor
    n_streams = 10
    def create_stream(i):
        return conn.create(new_uuid(), tmp_collection, tags={"name":f"s{i}"})
    def points_in_stream(s):
        return s.count()
    def obliterate_stream(s):
        s.obliterate()
    with ThreadPoolExecutor() as executor:
        for i in range(2):
            pmap = lambda f, it : list(executor.map(f, it, timeout=30))
            streams = pmap(create_stream, range(n_streams))
            assert sum(pmap(points_in_stream, streams)) == 0
            pmap(obliterate_stream, streams)
