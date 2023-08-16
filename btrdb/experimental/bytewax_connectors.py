import time
import queue
import threading
import weakref
import pyarrow as pa

from bytewax.inputs import DynamicInput, StatelessSource

import btrdb

_empty_values = pa.Table.from_arrays(
    [pa.array([]), pa.array([])],
    schema=pa.schema(
        [
            pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("value", pa.float64(), nullable=False),
        ]
    ),
)


class InsertSubscription(DynamicInput):
    def __init__(
        self,
        selector_fn,
        selector_refresh_interval=60 * 60 * 6,
        heartbeat_interval=None,
        profile=None,
        conn_str=None,
        apikey=None,
    ):
        self._selector_fn = selector_fn
        self._conn_str = conn_str
        self._apikey = apikey
        self._profile = profile
        self._selector_refresh_interval = selector_refresh_interval
        self._heartbeat_interval = heartbeat_interval

    class Source(StatelessSource):
        def __init__(
            self,
            db,
            selector_fn,
            selector_refresh_interval,
            heartbeat_interval,
            worker_index,
            worker_count,
        ):
            self._db = db
            self._selector_fn = selector_fn
            self._worker_index = worker_index
            self._worker_count = worker_count
            self._selector_refresh_interval = selector_refresh_interval
            self._heartbeat_interval = heartbeat_interval
            self._read_worker_exception = None
            self._background_worker_exception = None
            self._del_event = threading.Event()
            self._update_queue = queue.Queue(1)
            self._data_queue = queue.Queue(512)

            # self is wrapped in a weakref with the worker threads so
            # that the worker threads keep self alive.
            def read_worker(self, data):
                try:
                    # Avoid exessive weakref lookups
                    # by doing the lookup upfront initially.
                    del_event = self._del_event
                    data_queue = self._data_queue
                    for dat in data:
                        if del_event.is_set():
                            return
                        data_queue.put(dat)
                except Exception as e:
                    self._read_worker_exception = e

            # Self is a weakref, same as above.
            def background_worker(self):
                try:
                    # Avoid exessive weakref lookups
                    # by doing the lookup upfront initially.
                    db = self._db
                    del_event = self._del_event
                    selector_fn = self._selector_fn
                    worker_index = self._worker_index
                    worker_count = self._worker_count
                    data_queue = self._data_queue
                    heartbeat_interval = self._heartbeat_interval
                    last_heartbeat = time.monotonic()
                    selector_refresh_interval = self._selector_refresh_interval
                    last_selector_refresh = time.monotonic() - selector_refresh_interval
                    current_uuids = set()
                    while True:
                        now = time.monotonic()
                        if (now - last_selector_refresh) >= selector_refresh_interval:
                            last_selector_refresh = now
                            next_uuids = {
                                uuid
                                for uuid in selector_fn(db)
                                if uuid.int % worker_count == worker_index
                            }
                            added_uuids = next_uuids - current_uuids
                            removed_uuids = current_uuids - next_uuids
                            if len(added_uuids) != 0 or len(removed_uuids) != 0:
                                self._update_queue.put([added_uuids, removed_uuids])
                            current_uuids = next_uuids
                        if (
                            heartbeat_interval is not None
                            and (now - last_heartbeat) >= heartbeat_interval
                        ):
                            last_heartbeat = now
                            for uuid in current_uuids:
                                while not del_event.is_set():
                                    try:
                                        data_queue.put((uuid, _empty_values), 0.1)
                                        break
                                    except queue.Full:
                                        pass
                        if del_event.wait(1.0):
                            return
                except Exception as e:
                    self._background_worker_exception = e

            weakself = weakref.proxy(self)
            data = db.ep.subscribe(self._update_queue)
            self._background_worker = threading.Thread(
                target=background_worker, args=[weakself], daemon=True
            )
            self._read_worker = threading.Thread(
                target=read_worker, args=[weakself, data], daemon=True
            )
            self._background_worker.start()
            self._read_worker.start()

        def next(self):
            # Check if the selector thread has died.
            background_worker_exception = self._background_worker_exception
            if background_worker_exception is not None:
                raise background_worker_exception
            try:
                return self._data_queue.get_nowait()
            except queue.Empty:
                # Check if the reason no data arrived is because
                # the reader thead has died.
                read_worker_exception = self._read_worker_exception
                if read_worker_exception is not None:
                    raise read_worker_exception
                return None

        def __del__(self):
            # Signal workers to exit.
            self._del_event.set()
            # Signal the end of the subscription.
            self._update_queue.put(None)

    def build(self, worker_index, worker_count):
        db = btrdb.connect(
            profile=self._profile,
            conn_str=self._conn_str,
            apikey=self._apikey,
        )
        return InsertSubscription.Source(
            db,
            self._selector_fn,
            self._selector_refresh_interval,
            self._heartbeat_interval,
            worker_index,
            worker_count,
        )
