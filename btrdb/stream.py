# btrdb.stream
# Module for Stream and related classes
#
# Author:   PingThings
# Created:  Fri Dec 21 14:57:30 2018 -0500
#
# For license information, see LICENSE.txt
# ID: stream.py [] allen@pingthings.io $

"""
Module for Stream and related classes
"""

##########################################################################
## Imports
##########################################################################

import uuid as uuidlib
from copy import deepcopy

from btrdb.point import RawPoint, StatPoint
from btrdb.transformers import StreamSetTransformer
from btrdb.utils.buffer import PointBuffer
from btrdb.utils.timez import currently_as_ns, to_nanoseconds
from btrdb.exceptions import BTrDBError, InvalidOperation


##########################################################################
## Module Variables
##########################################################################

INSERT_BATCH_SIZE = 5000


##########################################################################
## Stream Classes
##########################################################################

class Stream(object):
    """
    An object that represents a specific time series stream in the BTrDB database.

    Parameters
    ----------
        btrdb : BTrDB
            A reference to the BTrDB object connecting this stream back to the physical server.
        uuid : UUID
            The unique UUID identifier for this stream.
        db_values : kwargs
            Framework only initialization arguments.  Not for developer use.

    """

    def __init__(self, btrdb, uuid, **db_values):
        db_args = ('known_to_exist', 'collection', 'tags', 'annotations', 'property_version')
        for key in db_args:
            value = db_values.pop(key, None)
            setattr(self, "_{}".format(key), value)
        if db_values:
            bad_keys = ", ".join(db_values.keys())
            raise TypeError("got unexpected db_values argument(s) '{}'".format(bad_keys))

        self._btrdb = btrdb
        self._uuid = uuid


    def refresh_metadata(self):
        # type: () -> ()
        """
        Refreshes the locally cached meta data for a stream

        Queries the BTrDB server for all stream metadata including collection,
        annotation, and tags. This method requires a round trip to the server.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        ep = self._btrdb.ep
        self._collection, self._property_version, self._tags, self._annotations, _ = ep.streamInfo(self._uuid, False, True)
        self._known_to_exist = True

    def exists(self):
        # type: () -> bool
        """
        Check if stream exists

        Exists returns true if the stream exists. This is essential after using
        StreamFromUUID as the stream may not exist, causing a 404 error on
        later stream operations. Any operation that returns a stream from
        collection and tags will have ensured the stream exists already.

        Parameters
        ----------
        None

        Returns
        -------
        bool
            Indicates whether stream exists.
        """

        if self._known_to_exist:
            return True

        try:
            self.refresh_metadata()
            return True
        except BTrDBError as bte:
            if bte.code == 404:
                return False
            raise bte

    @property
    def btrdb(self):
        """
        Returns the stream's BTrDB object.

        Parameters
        ----------
        None

        Returns
        -------
        BTrDB
            The BTrDB database object.

        """
        return self._btrdb

    @property
    def uuid(self):
        """
        Returns the stream's UUID. The stream may nor may not exist
        yet, depending on how the stream object was obtained.

        Returns
        -------
        uuid : uuid.UUID
            The unique identifier of the stream.


        See Also
        --------
        stream.exists()

        """
        return self._uuid

    @property
    def name(self):
        """
        Returns the stream's name which is parsed from the stream tags.  This
        may require a round trip to the server depending on how the stream was
        acquired.

        Returns
        -------
        name : str
            The name of the stream.

        """
        return self.tags()["name"]

    @property
    def collection(self):
        """
        Returns the collection of the stream. It may require a round trip to the
        server depending on how the stream was acquired.

        Parameters
        ----------
        None

        Returns
        -------
        str
            the collection of the stream

        """
        if self._collection is not None:
            return self._collection

        self.refresh_metadata()
        return self._collection

    def earliest(self, version=0):
        """
        Returns the first point of data in the stream.  Returns None if error
        encountered during lookup or no values in stream.

        Parameters
        ----------
        None

        Returns
        -------
        RawPoint
            The first data point in the stream
        int
            The version of the stream for the RawPoint supplied

        """
        earliest = None
        start = 0

        try:
            earliest = self.nearest(start, version=version, backward=False)
        except Exception:
            # TODO: figure out proper exception type
            pass

        return earliest


    def latest(self, version=0):
        """
        Returns last point of data in the stream. Note that this method will
        return None if no point can be found that is less than the current
        date/time.

        Parameters
        ----------
        None

        Returns
        -------
        RawPoint, int
            The last data point in the stream and the version of the stream
            the value was retrieved at.

        """
        latest = None
        start = currently_as_ns()

        try:
            latest = self.nearest(start, version=version, backward=True)
        except Exception:
            pass

        return latest



    def tags(self, refresh=False):
        """
        Returns the stream's tags.

        Tags returns the tags of the stream. It may require a round trip to the
        server depending on how the stream was acquired.

        Parameters
        ----------
        refresh: bool
            Indicates whether a round trip to the server should be implemented
            regardless of whether there is a local copy.

        Returns
        -------
        Dict[str, str]
            A dictionary containing the tags.

        """
        if refresh or self._tags is None:
            self.refresh_metadata()

        return deepcopy(self._tags)

    def annotations(self, refresh=False):
        """

        Returns a stream's annotations

        Annotations returns the annotations of the stream (and the annotation
        version). It will always require a round trip to the server. If you are
        ok with stale data and want a higher performance version, use
        Stream.CachedAnnotations().

        Do not modify the resulting map.


        Parameters
        ----------
        refresh: bool
            Indicates whether a round trip to the server should be implemented
            regardless of whether there is a local copy.

        Returns
        -------
        Tuple[Dict[str, str], int]
            A tuple containing a dictionary of annotations and an integer representing
            the version of the metadata.

        """
        if refresh or self._annotations is None:
            self.refresh_metadata()

        return deepcopy(self._annotations), deepcopy(self._property_version)

    def version(self):
        # type: () -> int
        """
        Returns the current data version of the stream.

        Version returns the current data version of the stream. This is not
        cached, it queries each time. Take care that you do not intorduce races
        in your code by assuming this function will always return the same vaue

        Parameters
        ----------
        None

        Returns
        -------
        int
            The version of the stream.

        """
        return self._btrdb.ep.streamInfo(self._uuid, True, False)[4]

    def insert(self, data):
        """
        Insert new data in the form (time, value) into the series.

        Inserts a list of new (time, value) tuples into the series. The tuples
        in the list need not be sorted by time. If the arrays are larger than
        appropriate, this function will automatically chunk the inserts. As a
        consequence, the insert is not necessarily atomic, but can be used with
        a very large array.

        Parameters
        ----------
        data: List[Tuple[int, float]]
            A list of tuples in which each tuple contains a time (int) and
            value (float) for insertion to the database

        Returns
        -------
        version : int
            The version of the stream after inserting new points.

        """
        i = 0
        version = 0
        while i < len(data):
            thisBatch = data[i:i + INSERT_BATCH_SIZE]
            version = self._btrdb.ep.insert(self._uuid, thisBatch)
            i += INSERT_BATCH_SIZE
        return version

    def _update_tags_collection(self, tags, collection):
        tags = self.tags() if tags is None else tags
        collection = self.collection if collection is None else collection
        if collection is None:
            raise ValueError("collection must be provided to update tags or collection")

        self._btrdb.ep.setStreamTags(
            uu=self.uuid,
            expected=self._property_version,
            tags=tags,
            collection=collection
        )

    def _update_annotations(self, annotations):
        self._btrdb.ep.setStreamAnnotations(
            uu=self.uuid,
            expected=self._property_version,
            changes=annotations
        )

    def update(self, tags=None, annotations=None, collection=None):
        """
        Updates metadata including tags, annotations, and collection.

        Parameters
        ----------
        tags: dict
            Dict of tag information for the stream.
        annotations: dict
            Dict of annotation information for the stream.
        collection: str
            The collection prefix for a stream

        Returns
        -------
        property_version : int
            The version of the metadata (separate from the version of the data)

        """
        if tags is None and annotations is None and collection is None:
            raise ValueError("you must supply a tags, annotations, or collection argument")

        if tags is not None and isinstance(tags, dict) is False:
            raise TypeError("tags must be of type dict")

        if annotations is not None and isinstance(annotations, dict) is False:
            raise TypeError("annotations must be of type dict")

        if collection is not None and isinstance(collection, str) is False:
            raise TypeError("collection must be of type string")

        if tags is not None or collection is not None:
            self._update_tags_collection(tags, collection)
            self.refresh_metadata()

        if annotations is not None:
            self._update_annotations(annotations)
            self.refresh_metadata()

        return self._property_version

    def delete(self, start, end):
        """
        "Delete" all points between [`start`, `end`)

        "Delete" all points between `start` (inclusive) and `end` (exclusive),
        both in nanoseconds. As BTrDB has persistent multiversioning, the
        deleted points will still exist as part of an older version of the
        stream.

        Parameters
        ----------
        start : int
            The start time in nanoseconds for the range to be deleted
        end : int
            The end time in nanoseconds for the range to be deleted

        Returns
        -------
        int
            The version of the new stream created

        """
        return self._btrdb.ep.deleteRange(self._uuid, start, end)

    def values(self, start, end, version=0):
        # type: (int, int, int) -> Tuple[RawPoint, int]
        """
        Read raw values from BTrDB between time [a, b) in nanoseconds.

        RawValues queries BTrDB for the raw time series data points between
        `start` and `end` time, both in nanoseconds since the Epoch for the
        specified stream `version`.

        Parameters
        ----------
        start: int
            The start time in nanoseconds for the range to be retrieved
        end : int
            The end time in nanoseconds for the range to be deleted
        version: int
            The version of the stream to be queried

        Yields
        ------
        (RawPoint, int)
            Returns a tuple containing a RawPoint and the stream version


        Notes
        -----
        Note that the raw data points are the original values at the sensor's
        native sampling rate (assuming the time series represents measurements
        from a sensor). This is the lowest level of data with the finest time
        granularity. In the tree data structure of BTrDB, this data is stored in
        the vector nodes.

        """
        materialized = []
        start = to_nanoseconds(start)
        end = to_nanoseconds(end)

        point_windows = self._btrdb.ep.rawValues(self._uuid, start, end, version)
        for point_list, version in point_windows:
            for point in point_list:
                materialized.append((RawPoint.from_proto(point), version))
        return materialized

    def aligned_windows(self, start, end, pointwidth, version=0):
        # type: (int, int, int, int) -> Tuple[StatPoint, int]

        """
        Read statistical aggregates of windows of data from BTrDB.

        Query BTrDB for aggregates (or roll ups or windows) of the time series
        with `version` between time `start` (inclusive) and `end` (exclusive) in
        nanoseconds. Each point returned is a statistical aggregate of all the
        raw data within a window of width 2**`pointwidth` nanoseconds. These
        statistical aggregates currently include the mean, minimum, and maximum
        of the data and the count of data points composing the window.

        Note that `start` is inclusive, but `end` is exclusive. That is, results
        will be returned for all windows that start in the interval [start, end).
        If end < start+2^pointwidth you will not get any results. If start and
        end are not powers of two, the bottom pointwidth bits will be cleared.
        Each window will contain statistical summaries of the window.
        Statistical points with count == 0 will be omitted.

        Parameters
        ----------
        start : int
            The start time in nanoseconds for the range to be queried
        end : int
            The end time in nanoseconds for the range to be queried
        pointwidth : int
            Specify the number of ns between data points (2**pointwidth)
        version : int
            Version of the stream to query

        Returns
        -------
        tuple(tuple(StatPoint, int), ...)
            Returns a tuple containing windows of data.  Each window is a tuple
            containing data tuples.  Each data tuple contains a StatPoint and
            the stream version.

        Notes
        -----
        As the window-width is a power-of-two, it aligns with BTrDB internal
        tree data structure and is faster to execute than `windows()`.
        """
        materialized = []
        start = to_nanoseconds(start)
        end = to_nanoseconds(end)

        windows = self._btrdb.ep.alignedWindows(self._uuid, start, end, pointwidth, version)
        for stat_points, version in windows:
            for point in stat_points:
                materialized.append((StatPoint.from_proto(point), version))

        return tuple(materialized)

    def windows(self, start, end, width, depth=0, version=0):
        # type: (int, int, int, int, int) -> Tuple[StatPoint, int]

        """
        Read arbitrarily-sized windows of data from BTrDB.

        Parameters
        ----------
        start : int
            The start time in nanoseconds for the range to be queried.
        end : int
            The end time in nanoseconds for the range to be queried.
        width : int
            The number of nanoseconds in each window, subject to the depth
            parameter.
        depth : int
            The precision of the window duration as a power of 2 in nanoseconds.
            E.g 30 would make the window duration accurate to roughly 1 second
        version : int
            The version of the stream to query.

        Returns
        -------
        tuple(tuple(StatPoint, int), ...)
            Returns a tuple containing windows of data.  Each window is a tuple
            containing data tuples.  Each data tuple contains a StatPoint and
            the stream version.

        Notes
        -----
        Windows returns arbitrary precision windows from BTrDB. It is slower
        than AlignedWindows, but still significantly faster than RawValues. Each
        returned window will be `width` nanoseconds long. `start` is inclusive,
        but `end` is exclusive (e.g if end < start+width you will get no
        results). That is, results will be returned for all windows that start
        at a time less than the end timestamp. If (`end` - `start`) is not a
        multiple of width, then end will be decreased to the greatest value less
        than end such that (end - start) is a multiple of `width` (i.e., we set
        end = start + width * floordiv(end - start, width). The `depth`
        parameter is an optimization that can be used to speed up queries on
        fast queries. Each window will be accurate to 2^depth nanoseconds. If
        depth is zero, the results are accurate to the nanosecond. On a dense
        stream for large windows, this accuracy may not be required. For example
        for a window of a day, +- one second may be appropriate, so a depth of
        30 can be specified. This is much faster to execute on the database
        side.

        """
        materialized = []
        start = to_nanoseconds(start)
        end = to_nanoseconds(end)

        windows = self._btrdb.ep.windows(self._uuid, start, end, width, depth, version)
        for stat_points, version in windows:
            for point in stat_points:
                materialized.append((StatPoint.from_proto(point), version))

        return tuple(materialized)

    def nearest(self, time, version, backward):
        # type: (int, int, bool) -> Tuple[RawPoint, int]

        """
        Finds the closest point in the stream to a specified time.

        Return the point nearest to the specified `time` in nanoseconds since
        Epoch in the stream with `version` while specifying whether to search
        forward or backward in time. If `backward` is false, the returned point
        will be >= `time`. If backward is true, the returned point will be <
        `time`. The version of the stream used to satisfy the query is returned.

        Parameters
        ----------
        time : int
            The time (in nanoseconds since Epoch) to search near
        version : int
            Version of the stream to use in search
        backward : boolean
            True to search backwards from time, else false for forward

        Returns
        -------
        RawPoint
            The point closest to time
        int
            Version of the stream used to satisfy the query


        Raises
        ------
        BTrDBError [401] no such point
            No point satisfies the query in the direction specified

        """

        rp, version = self._btrdb.ep.nearest(self._uuid, time, version, backward)
        return RawPoint.from_proto(rp), version

    def flush(self):
        # type: () -> None
        """
        Flush writes the stream buffers out to persistent storage.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        self._btrdb.ep.flush(self._uuid)


##########################################################################
## StreamSet  Classes
##########################################################################

class StreamSetBase(object):
    """
    A lighweight wrapper around a list of stream objects
    """

    def __init__(self, streams):
        self._streams = streams
        self._pinned_versions = None

        self.filters = []
        self.pointwidth = None
        self.width = None
        self.depth = None

    @property
    def allow_window(self):
        return not bool(self.pointwidth or (self.width and self.depth))

    def _latest_versions(self):
        return {s.uuid: s.version() for s in self._streams}


    def pin_versions(self, versions=None):
        """
        Saves the stream versions that future materializations should use.  If
        no pin is requested then the first materialization will automatically
        pin the return versions.  Versions can also be supplied through a dict
        object with key:UUID, value:stream.version().

        Parameters
        ----------
        versions : Dict[UUID: int]
            A dict containing the stream UUID and version ints as key/values

        Returns
        -------
        StreamSet
            Returns self

        """
        if versions is not None:
            if not isinstance(versions, dict):
                raise TypeError("`versions` argument must be dict")

            for key in versions.keys():
                if not isinstance(key, uuidlib.UUID):
                    raise TypeError("version keys must be type UUID")


        self._pinned_versions = self._latest_versions() if not versions else versions
        return self

    def versions(self):
        """
        Returns a dict of the stream versions.  These versions are the pinned
        values if previously pinned or the latest stream versions if not
        pinned.


        Parameters
        ----------
        None

        Returns
        -------
        Dict[UUID: int]
            A dict containing the stream UUID and version ints as key/values

        """
        return self._pinned_versions if self._pinned_versions else self._latest_versions()

    def earliest(self):
        """
        Returns earliest points of data in streams using available
        filters.

        Parameters
        ----------
        None

        Returns
        -------
        tuple(RawPoint)
            The earliest points of data found among all streams

        """
        earliest = []
        params = self._params_from_filters()
        start = params.get("start", 0)

        for s in self._streams:
            version = self.versions()[s.uuid]
            try:
                point, _ = s.nearest(start, version=version, backward=False)
            except Exception:
                # TODO: figure out proper exception type
                earliest.append(None)

            earliest.append(point)


        return tuple(earliest)

    def latest(self):
        """
        Returns latest points of data in the streams using available
        filters.  Note that this method will return None if no
        end filter is provided and point cannot be found that is less than the
        current date/time.

        Parameters
        ----------
        None

        Returns
        -------
        tuple(RawPoint)
            The latest points of data found among all streams

        """
        latest = []
        params = self._params_from_filters()
        start = params.get("end", currently_as_ns())

        for s in self._streams:
            version = self.versions()[s.uuid]
            try:
                point, _ = s.nearest(start, version=version, backward=True)
            except Exception:
                latest.append(None)

            latest.append(point)

        return tuple(latest)

    def filter(self, start=None, end=None):
        """
        Stores filtering attributes for queries to be eventually materialized
        from the database.  This method will return a new StreamSet instance.

        Parameters
        ----------
        start : int
            A int indicating the inclusive start of the query
        end : int
            A int indicating the exclusive end of the query

        Returns
        -------
        StreamSet
            Returns a new copy of the instance

        """

        if start is None and end is None:
            raise ValueError("A valid `start` or `end` must be supplied")

        obj = self.clone()
        obj.filters.append(StreamFilter(start, end))
        return obj

    def clone(self):
        """
        Returns a deep copy of the object.  Attributes that cannot be copied
        will be referenced to both objects.

        Parameters
        ----------
        None

        Returns
        -------
        StreamSet
            Returns a new copy of the instance

        """
        protected = ('_streams', )
        clone = self.__class__(self._streams)
        for attr, val in self.__dict__.items():
            if attr not in protected:
                setattr(clone, attr, deepcopy(val))
        return clone

    def windows(self, width, depth):
        """
        Stores the request for a windowing operation when the query is
        eventually materialized.

        Parameters
        ----------
        width : int
            The number of nanoseconds to use for each window size.
        depth : int
            The requested accuracy of the data up to 2^depth nanoseconds.  A
            depth of 0 is accurate to the nanosecond.

        Returns
        -------
        StreamSet
            Returns self


        Notes
        -----
        Windows returns arbitrary precision windows from BTrDB. It is slower
        than aligned_windows, but still significantly faster than values. Each
        returned window will be width nanoseconds long. start is inclusive, but
        end is exclusive (e.g if end < start+width you will get no results).
        That is, results will be returned for all windows that start at a time
        less than the end timestamp. If (end - start) is not a multiple of
        width, then end will be decreased to the greatest value less than end
        such that (end - start) is a multiple of width (i.e., we set end = start
        + width * floordiv(end - start, width). The depth parameter is an
        optimization that can be used to speed up queries on fast queries. Each
        window will be accurate to 2^depth nanoseconds. If depth is zero, the
        results are accurate to the nanosecond. On a dense stream for large
        windows, this accuracy may not be required. For example for a window of
        a day, +- one second may be appropriate, so a depth of 30 can be
        specified. This is much faster to execute on the database side.

        """
        if not self.allow_window:
            raise InvalidOperation("A window operation is already requested")

        # TODO: refactor keeping in mind how exception is raised
        self.width = int(width)
        self.depth = int(depth)
        return self

    def aligned_windows(self, pointwidth):
        """
        Stores the request for an aligned windowing operation when the query is
        eventually materialized.

        Parameters
        ----------
        pointwidth : int
            The length of each returned window as computed by 2^pointwidth.

        Returns
        -------
        StreamSet
            Returns self

        Notes
        -----
        `aligned_windows` reads power-of-two aligned windows from BTrDB. It is
        faster than Windows(). Each returned window will be 2^pointwidth
        nanoseconds long, starting at start. Note that start is inclusive, but
        end is exclusive. That is, results will be returned for all windows that
        start in the interval [start, end). If end < start+2^pointwidth you will
        not get any results. If start and end are not powers of two, the bottom
        pointwidth bits will be cleared. Each window will contain statistical
        summaries of the window. Statistical points with count == 0 will be
        omitted.

        """
        if not self.allow_window:
            raise InvalidOperation("A window operation is already requested")

        self.pointwidth = int(pointwidth)
        return self

    def rows(self):
        """
        Returns a materialized list of tuples where each tuple contains the
        points from each stream at a unique time.  If a stream has no value for that
        time than None is provided instead of a point object.

        Parameters
        ----------
        None

        Returns
        -------
        list(tuple(RawPoint, int))
            A list of tuples containing a RawPoint (or StatPoint) and the stream
            version.

        """
        result = []
        params = self._params_from_filters()
        result_iterables = [iter(s.values(**params)) for s in self._streams]
        buffer = PointBuffer(len(self._streams))

        while True:
            streams_empty = True

            # add next values from streams into buffer
            for stream_idx, data in enumerate(result_iterables):
                if buffer.active[stream_idx]:
                    try:
                        point, _ = next(data)
                        buffer.add_point(stream_idx, point)
                        streams_empty = False
                    except StopIteration:
                        buffer.deactivate(stream_idx)
                        continue

            key = buffer.next_key_ready()
            if key:
                result.append(tuple(buffer.pop(key)))

            if streams_empty and len(buffer.keys()) == 0:
                break

        return result

    def _params_from_filters(self):
        params = {}
        for filter in self.filters:
            if filter.start is not None:
                params["start"] = filter.start
            if filter.end is not None:
                params["end"] = filter.end
        return params

    def values_iter(self):
        """
        Must return context object which would then close server cursor on __exit__
        """
        raise NotImplementedError()

    def values(self):
        """
        Returns a fully materialized list of lists for the stream values/points
        """
        result = []
        params = self._params_from_filters()
        stream_output_iterables = [s.values(**params) for s in self._streams]

        for stream_output in stream_output_iterables:
            result.append([point[0] for point in stream_output])

        return result

    def __iter__(self):
        for stream in self._streams:
            yield stream

    def __repr__(self):
        return "<{}({} streams)>".format(self.__class__.__name__, len(self._streams))

    def __str__(self):
        return "{} with {} streams".format(self.__class__.__name__, len(self._streams))


class StreamSet(StreamSetBase, StreamSetTransformer):
    """
    Public class for a collection of streams
    """
    pass


##########################################################################
## Utility Classes
##########################################################################

class StreamFilter(object):
    """
    Object for storing requested filtering options
    """
    def __init__(self, start=None, end=None):
        self.start = to_nanoseconds(start) if start else None
        self.end = to_nanoseconds(end) if end else None

        if self.start is None and self.end is None:
            raise ValueError("A valid `start` or `end` must be supplied")

        if self.start is not None and self.end is not None and self.start >= self.end:
            raise ValueError("`start` must be strictly less than `end` argument")