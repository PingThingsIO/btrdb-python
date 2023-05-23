import os
import uuid
import json
import shutil
import struct
import threading
import traceback
import subprocess

import btrdb
import numpy as np
from tqdm import tqdm

class ConcurrentLoadingError(Exception):
    """
    The primary exception for Parload errors.
    """
    pass

class Parload():
    """
    Interface for `parload`, the parallel stream data loading tool.
    """
    def __init__(self, apikey=None, endpoint=None, num_workers=None, parload_path='parload'):
        """
        Initialize this interface & check that the parload binary is available.

        Parameters
        ----------
        apikey : str, optional
            API key to connect to BTrDB. If `None`, parload's default will be used (likely $BTRDB_API_KEY).
        endpoint : str, optional
            The endpoint to connect to BTrDB. If `None`, parload's default will be used (likely $BTRDB_ENDPOINTS).
        num_workers : int, optional
            Number of workers to use for parallel querying. If `None`, parload's default will be used.
        parload_path : str, optional
            File system path to the parload executable.
        """
        self.apikey = apikey or os.environ.get('BTRDB_API_KEY')
        self.endpoint = endpoint or os.environ.get('BTRDB_ENDPOINTS')
        self.num_workers = num_workers
        self.parload_path = parload_path

        # the following checks are done as an alternative to handling differnt kinds of output from the
        # Go binary related to such errors at query time
        self._verify_parload_path()
        self._verify_connection()

    def _verify_connection(self):
        btrdb.connect(conn_str=self.endpoint, apikey=self.apikey).info()  # will raise an exception if connection fails

    def _verify_parload_path(self):
        if not shutil.which(self.parload_path):
            raise ConcurrentLoadingError('parload binary not found')

    def _run_parload(self, parload_command, uuids, num_bytes_per_val,
                     num_points_per_stream=None, return_native_dtypes=False, show_progressbar=False):
        """
        Execute a `parload` command on the UUIDs submitted.

        Parameters
        ----------
        parload_command : list of str
            The `parload` execution command submitted to stdin
        uuids : list of str
            List of uuids for streams that will be queried for.
        num_bytes_per_val : int
            Number of bytes expected in the stdout stream for each point's value.
        num_points_per_stream : int, optional
            Number of points of data expected per stream. Used when called by `earliest` and `latest`.
        return_native_dtypes : bool, optional
            Return the data as native Python lists rather than Numpy arrays.
        show_progressbar: bool, optional
            Display a progressbar.

        Yields
        ------
        str
            The UUID for the stream whose data is yielded at this iteration.
        numpy.ndarray (1-dimensional) or list of ints
            The times (either timestamps or time indexes) for the values associated with the data returned for this stream.
        numpy.ndarray (1-dimensional) or list of floats
            The values associated with the times returned for this stream.
        """
        assert isinstance(uuids, list) and len(uuids) > 0, '`uuids` must be a list with > 0 elements'
        assert num_bytes_per_val in {4,8}, 'num bytes must be either 4 or 8'
        dtype_vals = np.float64 if num_bytes_per_val == 8 else np.float32
        dtype_times = np.int64

        handle = subprocess.Popen(parload_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        handle.stdin.write(bytes(json.dumps(uuids), 'utf8'))
        handle.stdin.close()
        output = handle.stdout

        # create a thread to read stderr output in the background of this Python process
        err_output = []
        t = threading.Thread(target=lambda fd: err_output.append(fd.read()), args=(handle.stderr,))
        t.start()

        disable_progressbar = not show_progressbar
        num_streams = len(uuids)
        for stream_idx in tqdm(range(num_streams), disable=disable_progressbar, total=num_streams):

            try:
                if num_points_per_stream:
                    num_points = num_points_per_stream
                    uu = str(uuid.UUID(bytes=output.read(16)))
                else:
                    buf = output.read(24)
                    num_points, = struct.unpack('<q', buf[:8])
                    uu = str(uuid.UUID(bytes=buf[8:24]))

                times_buffer = output.read(num_points*8)
                vals_buffer = output.read(num_points*num_bytes_per_val)

            except (struct.error, IndexError, ValueError):
                # the above 3 parsing errors can occur when the parload binary returns an error
                t.join()
                err = ''.join(e.decode("utf-8") for e in err_output) if err_output else None
                handle.terminate()
                if err:
                    raise ConcurrentLoadingError(err)
                else:
                    raise ConcurrentLoadingError("Unspecified Parload error; consider checking memory limits")

            if return_native_dtypes:
                times = struct.unpack(f'<{num_points}q', times_buffer)
                vals = struct.unpack(f'<{num_points}d', vals_buffer)
            else:
                times = np.frombuffer(times_buffer, dtype=dtype_times)
                vals = np.frombuffer(vals_buffer, dtype=dtype_vals)

            yield uu, times, vals

        t.join()

    def values_raw(self, uuids, start, end, return_native_dtypes=False, downcast_values=False, show_progressbar=False):
        """
        Use `parload` to load data from `start` to `end` for a list of streams, returned as a
        list of 2-element lists with time & value arrays for each stream.

        Parameters
        ----------
        uuids : list of str
            List of uuids for streams that will be queried for.
        start : int
            The start time in nanoseconds for the range to be queried.
        end : int
            The end time in nanoseconds for the range to be queried.
        return_native_dtypes : bool, optional
            Return the data as native Python lists rather than Numpy arrays.
        downcast_values : bool, optional
            Have parload downcast values from 64-bit to 32-bit before streaming to Python.
        show_progressbar: bool, optional
            Display a progressbar.

        Returns
        -------
        list of str
            The UUIDs of the streams as they returned from the database. This will also be
            the order of the columns in the resulting list.
        list of [numpy.ndarray, numpy.ndarray] or list of [list of RawPoints]
            The values data returned from the database. Each element of the list contains one
            stream's data as a pair of containers for times & values, respectively.
        """
        assert len(uuids) > 0, 'At least 1 stream UUID must be passed in'
        assert start < end, '`start` timestamp must be before `end` timestamp'

        parload_command = [self.parload_path, '--apiKey', self.apikey, '--endpoint', self.endpoint]
        if self.num_workers:
            parload_command.extend(['--num-workers', str(self.num_workers)])
        parload_command.extend(['query', '--start', str(start), '--end', str(end)])

        if downcast_values:
            parload_command.extend(['--downcast-floats'])
            num_bytes_per_val = 4
        else:
            num_bytes_per_val = 8

        uuid_order = []
        data = []
        for uu, times, vals in self._run_parload(parload_command, uuids, num_bytes_per_val,
                                                 return_native_dtypes=return_native_dtypes, show_progressbar=show_progressbar):
            if return_native_dtypes:
                data.append([btrdb.point.RawPoint(time, val) for time, val in zip(times, vals)])
            else:
                data.append([times, vals])

            uuid_order.append(uu)

        return uuid_order, data

    def values_aligned(self, uuids, start, end, sample_rate, downcast_values=False, return_timestamps=False, show_progressbar=False):
        """
        Use `parload` to query data for a list of streams between the `start` and `end` time.
        Data is returned as a time-aligned 2d numpy array.

        "Time-aligned" means data across streams will be snapped to a common time grid defined
        by the `start`, `end`, and `sample_rate`. It is recommended that the sample
        rate & start time are chosen to match the underlying sampling of the raw data.

        Parameters
        ----------
        uuids : list of str
            List of uuids for streams that will be queried for.
        start : int
            The start time in nanoseconds for the range to be queried.
        end : int
            The end time in nanoseconds for the range to be queried.
        sample_rate : int
            The sample rate in Hz that raw values will be "time-aligned" to. The first timestamp
            at this sample rate will be `start`.
        downcast_values : bool, optional
            Have parload downcast values from 64-bit to 32-bit before streaming to Python.
        return_timestamps : bool, optional
            Have parload return the timestamps used to calculate for time alignment.
        show_progressbar: bool, optional
            Display a progressbar.

        Returns
        -------
        list of str
            The UUIDs of the streams as they returned from the database. This will also be
            the order of the columns in the resulting array.
        numpy.ndarray
            Timestamps being used for time alignment.
        numpy.ndarray
            The values data returned from the database. The shape of this array will be
            (`num_streams`, `num_timestamps`), where num_timestamps is defined by the `sample_rate`
            from `start` to `end`.
        """
        assert len(uuids) > 0, 'At least 1 stream UUID must be passed in'
        assert start < end, '`start` timestamp must be before `end` timestamp'
        assert isinstance(sample_rate, int) and sample_rate > 0, '`sample_rate` must be a positive integer'
        time_divisor = int(1/sample_rate*1e9)
        num_streams = len(uuids)
        num_times = (end-start)//time_divisor + 1

        data = np.empty((num_times, num_streams))
        data[:] = np.NaN

        parload_command = [self.parload_path, '--apiKey', self.apikey, '--endpoint', self.endpoint]
        if self.num_workers:
            parload_command.extend(['--num-workers', str(self.num_workers)])
        parload_command.extend(['query', '--frequency', str(sample_rate), '--start', str(start), '--end', str(end)])

        if downcast_values:
            parload_command.extend(['--downcast-floats'])
            data = np.full((num_times, num_streams), np.nan, dtype=np.float32)
            num_bytes_per_val = 4
        else:
            data = np.full((num_times, num_streams), np.nan, dtype=np.float64)
            num_bytes_per_val = 8

        uuid_order = []

        for stream_idx, (uu, timestamps, vals) in enumerate(self._run_parload(parload_command, uuids, num_bytes_per_val,
                                                                 return_native_dtypes=False, show_progressbar=show_progressbar)):
            data[:, stream_idx] = vals
            uuid_order.append(uu)

        if return_timestamps:
            return uuid_order, timestamps, data
        else:
            return uuid_order, data

    def _earliest_latest(self, parload_command, uuids):
        """
        Private method to return either the "earliest" or "latest" point in a stream, optionally with
        respect to some timestamp.

        `parload_command` is expected to be a correctly-formed command for either an earliest or latest query.

        Parameters
        ----------
        parload_command : list of str
            A list of program arguments that form a correct `parload` command. Submitted internally to stdin via
            subprocess.Popen.
        uuids : list of str
            List of uuids for streams that will be queried for.

        Returns
        -------
        list of str
            The UUIDs of the streams as they returned from the database. This will also be
            the order of the columns in the resulting list.
        list of (RawPoint, int) tuples
            The points for each stream returned from the database.
        """
        num_bytes_per_val = 8

        uuid_order = []
        data = []
        for uu, (time,), (val,) in self._run_parload(parload_command, uuids, num_bytes_per_val,
                                                     num_points_per_stream=1, return_native_dtypes=True):
            data.append(btrdb.point.RawPoint(time, val))
            uuid_order.append(uu)

        return uuid_order, data

    def earliest(self, uuids, after=None):
        """
        Return the "earliest" point in a stream, optionally with respect to some timestamp.

        Parameters
        ----------
        uuids : list of str
            List of uuids for streams that will be queried for.
        after : int, optional
            The "earliest" point returned will be the nearest point in the future from this timestamp.

        Returns
        -------
        list of str
            The UUIDs of the streams as they returned from the database. This will also be
            the order of the columns in the resulting list.
        list of (RawPoint, int) tuples
            The points for each stream returned from the database.
        """
        parload_command = [self.parload_path, '--apiKey', self.apikey, '--endpoint', self.endpoint]
        if self.num_workers:
            parload_command.extend(['--num-workers', str(self.num_workers)])
        parload_command.extend(['earliest'])
        if after:
            parload_command.extend(['-a', str(after)])

        return self._earliest_latest(parload_command, uuids)

    def latest(self, uuids, before=None):
        """
        Return the "latest" point in a stream, optionally with respect to some timestamp.

        Parameters
        ----------
        uuids : list of str
            List of uuids for streams that will be queried for.
        before : int, optional
            The "latest" point returned will be the nearest point in the past from this timestamp.

        Returns
        -------
        list of str
            The UUIDs of the streams as they returned from the database. This will also be
            the order of the columns in the resulting list.
        list of (RawPoint, int) tuples
            The points for each stream returned from the database.
        """
        parload_command = [self.parload_path, '--apiKey', self.apikey, '--endpoint', self.endpoint]
        if self.num_workers:
            parload_command.extend(['--num-workers', str(self.num_workers)])
        parload_command.extend(['latest'])
        if before:
            parload_command.extend(['-b', str(before)])

        return self._earliest_latest(parload_command, uuids)
