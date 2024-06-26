# tests.test_base
# Testing package for the btrdb library.
#
# Author:   PingThings
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# For license information, see LICENSE.txt
# ID: test_base.py [] allen@pingthings.io $

"""
Testing package for the btrdb database library.
"""

##########################################################################
## Imports
##########################################################################

import os
from unittest.mock import MagicMock, Mock, patch

import pytest

from btrdb import BTRDB_API_KEY, BTRDB_ENDPOINTS, Endpoint, __version__, connect
from btrdb.exceptions import ConnectionError

##########################################################################
## Initialization Tests
##########################################################################


class TestConnect(object):
    def setup_method(self):
        for env in ["BTRDB_ENDPOINTS", "BTRDB_PROFILE", "BTRDB_API_KEY"]:
            try:
                del os.environ[env]
            except KeyError:
                pass

    @patch("btrdb.credentials")
    def test_raises_err_with_no_inputs(self, mock_credentials):
        """
        Assert ConnectionError raised if no connection arg, ENV, profile
        """
        mock_credentials.return_value = {}
        msg = "Could not determine credentials to use."
        with pytest.raises(ConnectionError, match=msg):
            connect()

    def test_raises_err_if_both_profile_and_credentials(self):
        """
        Assert error is raised if both profile and credentials are sent
        """
        with pytest.raises(ValueError):
            connect("192.168.1.100:4410", None, "default")

    @patch("btrdb.utils.credentials.credentials_by_profile")
    @patch("btrdb.utils.credentials.credentials_by_env")
    @patch("btrdb._connect")
    def test_uses_args_over_env(
        self, mock_connect, mock_credentials_by_env, mock_credentials_by_profile
    ):
        """
        Assert uses connect args over env
        """
        mock_credentials_by_profile.return_value = {}
        mock_credentials_by_env.return_value = {"endpoints": "a", "apikey": "b"}
        connect("cat", "dog")
        mock_connect.assert_called_once_with(endpoints="cat", apikey="dog")

    @patch("btrdb.utils.credentials.credentials_by_profile")
    @patch("btrdb.utils.credentials.credentials_by_env")
    @patch("btrdb._connect")
    def test_uses_env_over_profile(
        self, mock_connect, mock_credentials_by_env, mock_credentials_by_profile
    ):
        """
        Assert connect uses env over profile info
        """
        mock_credentials_by_profile.return_value = {"endpoints": "a", "apikey": "b"}
        mock_credentials_by_env.return_value = {"endpoints": "c", "apikey": "d"}
        connect()
        mock_connect.assert_called_once_with(endpoints="c", apikey="d")

    # @patch("btrdb.utils.credentials.credentials_by_env")
    @patch("btrdb.utils.credentials.credentials_by_env")
    @patch("btrdb.utils.credentials.credentials_by_profile")
    @patch("btrdb.Connection")
    def test_connect_with_env(
        self, mock_conn, mock_credentials_by_profile, mock_credentials_by_env
    ):
        """
        Assert connect uses ENV variables
        """
        with patch(
            "btrdb.endpoint.Endpoint.info", return_value=Mock(Endpoint)
        ) as mock_ep_info:
            mock_ep_info.return_value = MagicMock()
            mock_credentials_by_profile.return_value = {}
            address = "127.0.0.1:4410"
            mock_credentials_by_env.return_value = {"endpoints": address}
            os.environ[BTRDB_ENDPOINTS] = address

            btrdb = connect()
            mock_conn.assert_called_once_with(address, apikey=None)
            mock_conn.reset_mock()

            apikey = "abcd"
            os.environ[BTRDB_API_KEY] = apikey
            mock_credentials_by_env.return_value = {
                "endpoints": address,
                "apikey": apikey,
            }
            btrdb = connect()
            mock_conn.assert_called_once_with(address, apikey=apikey)
