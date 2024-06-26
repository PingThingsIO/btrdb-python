# tests.test_conn
# Testing package for the btrdb connection module
#
# Author:   PingThings
# Created:  Wed Jan 02 19:26:20 2019 -0500
#
# For license information, see LICENSE.txt
# ID: test_conn.py [] allen@pingthings.io $

"""
Testing package for the btrdb connection module
"""

##########################################################################
## Imports
##########################################################################

import uuid as uuidlib
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest

from btrdb.conn import BTrDB, Connection
from btrdb.endpoint import Endpoint
from btrdb.exceptions import *
from btrdb.grpcinterface import btrdb_pb2
from btrdb.stream import Stream

##########################################################################
## Fixtures
##########################################################################


@pytest.fixture
def stream1():
    uu = uuidlib.UUID("0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a")
    stream = Mock(Stream)
    stream.version = Mock(return_value=11)
    stream.uuid = Mock(return_value=uu)
    type(stream).collection = PropertyMock(return_value="fruits/apple")
    type(stream).name = PropertyMock(return_value="gala")
    stream.tags = Mock(return_value={"name": "gala", "unit": "volts"})
    stream.annotations = Mock(return_value=({"owner": "ABC", "color": "red"}, 11))
    stream._btrdb = MagicMock()
    return stream


@pytest.fixture
def stream2():
    uu = uuidlib.UUID("17dbe387-89ea-42b6-864b-f505cdb483f5")
    stream = Mock(Stream)
    stream.version = Mock(return_value=22)
    stream.uuid = Mock(return_value=uu)
    type(stream).collection = PropertyMock(return_value="fruits/orange")
    type(stream).name = PropertyMock(return_value="blood")
    stream.tags = Mock(return_value={"name": "blood", "unit": "amps"})
    stream.annotations = Mock(return_value=({"owner": "ABC", "color": "orange"}, 22))
    stream._btrdb = MagicMock()
    return stream


@pytest.fixture
def stream3():
    uu = uuidlib.UUID("17dbe387-89ea-42b6-864b-e2ef0d22a53b")
    stream = Mock(Stream)
    stream.version = Mock(return_value=33)
    stream.uuid = Mock(return_value=uu)
    type(stream).collection = PropertyMock(return_value="fruits/banana")
    type(stream).name = PropertyMock(return_value="yellow")
    stream.tags = Mock(return_value={"name": "yellow", "unit": "watts"})
    stream.annotations = Mock(return_value=({"owner": "ABC", "color": "yellow"}, 33))
    stream._btrdb = MagicMock()
    return stream


##########################################################################
## Connection Tests
##########################################################################


class TestConnection(object):
    def test_raises_err_invalid_address(self):
        """
        Assert ValueError is raised if address:port is invalidly formatted
        """
        address = "127.0.0.1::4410"
        with pytest.raises(ValueError) as exc:
            conn = Connection(address)
        assert "expecting address:port" in str(exc)


##########################################################################
## BTrDB Tests
##########################################################################


class TestBTrDB(object):
    ##########################################################################
    ## .streams tests
    ##########################################################################
    # TODO: remove this when removing future warnings from `streams_in_collection`
    pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

    def test_streams_raises_err_if_version_not_list(self):
        """
        Assert streams raises TypeError if versions is not list
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            with pytest.raises(TypeError) as exc:
                db.streams("0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a", versions="2,2")

            assert "versions argument must be of type list" in str(exc)

    def test_streams_raises_err_if_version_argument_mismatch(self):
        """
        Assert streams raises ValueError if len(identifiers) doesnt match length of versions
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            with pytest.raises(ValueError) as exc:
                db.streams("0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a", versions=[2, 2])

            assert "versions does not match identifiers" in str(exc)

    def test_streams_stores_versions(self):
        """
        Assert streams correctly stores supplied version info
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            uuid1 = uuidlib.UUID("0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a")
            uuid2 = uuidlib.UUID("17dbe387-89ea-42b6-864b-f505cdb483f5")
            versions = [22, 44]
            expected = dict(zip([uuid1, uuid2], versions))

            streams = db.streams(uuid1, uuid2, versions=versions)
            assert streams._pinned_versions == expected

    @patch("btrdb.conn.BTrDB.stream_from_uuid")
    def test_streams_recognizes_uuid(self, mock_func):
        """
        Assert streams recognizes uuid strings
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            uuid1 = uuidlib.UUID("0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a")
            mock_func.return_value = Stream(db, uuid1)
            db.streams(uuid1)

            mock_func.assert_called_once()
            assert mock_func.call_args[0][0] == uuid1

    @patch("btrdb.conn.BTrDB.stream_from_uuid")
    def test_streams_recognizes_uuid_string(self, mock_func):
        """
        Assert streams recognizes uuid strings
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            uuid1 = "0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a"
            mock_func.return_value = Stream(db, uuid1)
            db.streams(uuid1)

            mock_func.assert_called_once()
            assert mock_func.call_args[0][0] == uuid1

    @patch("btrdb.conn.BTrDB.streams_in_collection")
    def test_streams_handles_path(self, mock_func):
        """
        Assert streams calls streams_in_collection for collection/name paths
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            ident = "zoo/animal/dog"
            mock_func.return_value = [
                Stream(db, "0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a"),
            ]
            db.streams(ident, "0d22a53b-e2ef-4e0a-ab89-b2d48fb2592a")

            mock_func.assert_called_once()
            assert mock_func.call_args[0][0] == "zoo/animal"
            assert mock_func.call_args[1] == {
                "is_collection_prefix": False,
                "tags": {"name": "dog"},
            }

    @patch("btrdb.conn.BTrDB.streams_in_collection")
    def test_streams_raises_err(self, mock_func):
        """
        Assert streams raises StreamNotFoundError
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            ident = "zoo/animal/dog"

            mock_func.return_value = []
            with pytest.raises(StreamNotFoundError) as exc:
                db.streams(ident)

            # check that does not raise if one returned
            mock_func.return_value = [
                Stream(db, ident),
            ]
            db.streams(ident)

    def test_streams_raises_valueerror(self):
        """
        Assert streams raises ValueError if not uuid, uuid str, or path
        """
        with patch("btrdb.endpoint.Endpoint", return_value=MagicMock()) as ep:
            db = BTrDB(ep)
            with pytest.raises(ValueError) as exc:
                db.streams(11)

    ##########################################################################
    ## other tests
    ##########################################################################

    def test_info(self):
        """
        Assert info method returns a dict
        """
        serialized = b"\x18\x05*\x055.0.02\x10\n\x0elocalhost:4410"
        info = btrdb_pb2.InfoResponse.FromString(serialized)

        endpoint = Mock(Endpoint)
        endpoint.info = Mock(return_value=info)
        conn = BTrDB(endpoint)

        truth = {
            "majorVersion": 5,
            "minorVersion": 0,
            "build": "5.0.0",
            "proxy": {
                "proxyEndpoints": ["localhost:4410"],
            },
        }
        info = conn.info()
        assert info == truth

        # verify RepeatedScalarContainer is converted to list
        assert info["proxy"]["proxyEndpoints"].__class__ == list

    def test_list_collections(self):
        """
        Assert list_collections method works
        """
        endpoint = Mock(Endpoint)
        endpoint.listCollections = Mock(
            side_effect=[iter([["allen/automated"], ["allen/bindings"]])]
        )
        conn = BTrDB(endpoint)

        truth = ["allen/automated", "allen/bindings"]
        assert conn.list_collections() == truth

    @patch("btrdb.conn.unpack_stream_descriptor")
    def test_streams_in_collections_args(self, mock_util):
        """
        Assert streams_in_collections correctly sends *collection, tags, annotations
        to the endpoint method
        """
        descriptor = Mock()
        type(descriptor).uuid = PropertyMock(return_value=uuidlib.uuid4().bytes)
        type(descriptor).collection = PropertyMock(return_value="fruit/apple")
        type(descriptor).propertyVersion = PropertyMock(return_value=22)
        mock_util.side_effect = [({"name": "gala"}, {}), ({"name": "fuji"}, {})]

        endpoint = Mock(Endpoint)
        endpoint.lookupStreams = Mock(side_effect=[[[descriptor]], [[descriptor]]])

        conn = BTrDB(endpoint)
        tags = {"unit": "volts"}
        annotations = {"size": "large"}
        streams = conn.streams_in_collection(
            "a", "b", is_collection_prefix=False, tags=tags, annotations=annotations
        )

        assert streams[0].name == "gala"
        assert streams[1].name == "fuji"

        expected = [
            call("a", False, tags, annotations),
            call("b", False, tags, annotations),
        ]
        assert endpoint.lookupStreams.call_args_list == expected

    def test_streams_in_collections_no_arg(self):
        """
        Assert streams_in_collections calls lookupStreams if collections not sent
        """
        endpoint = Mock(Endpoint)
        endpoint.lookupStreams.return_value = []

        conn = BTrDB(endpoint)
        annotations = {"size": "large"}
        streams = conn.streams_in_collection(annotations=annotations)
        assert endpoint.lookupStreams.called
