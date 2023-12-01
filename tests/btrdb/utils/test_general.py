# tests.test_general
# Testing for the btrdb.utils.general module
#
# Author:   PingThings
# Created:  Wed Jun 12 11:57:06 2019 -0400
#
# For license information, see LICENSE.txt
# ID: test_collection.py [] benjamin@pingthings.io $

"""
Testing for the btrdb.utils.general module
"""

from collections import namedtuple
from datetime import timedelta
from itertools import zip_longest

import pytest

from btrdb.utils.general import batched, pointwidth
from btrdb.utils.timez import ns_delta

BatchedExample = namedtuple(
    "BatchedExample", ["input", "n", "expected_output", "description"]
)
batched_examples = [
    BatchedExample((1, 2, 3, 4, 5, 6), 4, ((1, 2, 3, 4), (5, 6)), "Normal examples"),
    BatchedExample(
        (1, 2, 3, 4, 5, 6), 10, [(1, 2, 3, 4, 5, 6)], "Batch size larger than input"
    ),
    BatchedExample(
        (1, "2", int, 4, 5, 6), 2, [(1, "2"), (int, 4), (5, 6)], "Test many data types"
    ),
    BatchedExample((_ for _ in range(4)), 3, [(0, 1, 2), (3,)], "Test generator input"),
    BatchedExample([], 3, [], "Empty input"),
]

ids = [_.description for _ in batched_examples]


@pytest.mark.parametrize("test_example", batched_examples, ids=ids)
def test_batched(test_example: BatchedExample):
    actual_output = batched(test_example.input, test_example.n)
    for actual_elm, expected_el in zip_longest(
        actual_output, test_example.expected_output
    ):
        assert actual_elm == expected_el


def test_batched_bad_batch():
    with pytest.raises(ValueError):
        next(batched("", 0))


class TestPointwidth(object):
    @pytest.mark.parametrize(
        "delta, expected",
        [
            (timedelta(days=365), 54),
            (timedelta(days=30), 51),
            (timedelta(days=7), 49),
            (timedelta(days=1), 46),
            (timedelta(hours=4), 43),
            (timedelta(minutes=15), 39),
            (timedelta(seconds=30), 34),
        ],
    )
    def test_from_timedelta(self, delta, expected):
        """
        Test getting the closest point width from a timedelta
        """
        assert pointwidth.from_timedelta(delta) == expected

    @pytest.mark.parametrize(
        "nsec, expected",
        [
            (ns_delta(days=365), 54),
            (ns_delta(days=30), 51),
            (ns_delta(days=7), 49),
            (ns_delta(days=1), 46),
            (ns_delta(hours=12), 45),
            (ns_delta(minutes=30), 40),
            (ns_delta(seconds=1), 29),
        ],
    )
    def test_from_nanoseconds(self, nsec, expected):
        """
        Test getting the closest point width from nanoseconds
        """
        assert pointwidth.from_nanoseconds(nsec) == expected

    def test_time_conversions(self):
        """
        Test standard pointwidth time conversions
        """
        assert pointwidth(62).years == pytest.approx(146.2171)
        assert pointwidth(56).years == pytest.approx(2.2846415)
        assert pointwidth(54).months == pytest.approx(6.854793)
        assert pointwidth(50).weeks == pytest.approx(1.861606)
        assert pointwidth(48).days == pytest.approx(3.2578122)
        assert pointwidth(44).hours == pytest.approx(4.886718)
        assert pointwidth(38).minutes == pytest.approx(4.581298)
        assert pointwidth(32).seconds == pytest.approx(4.294967)
        assert pointwidth(26).milliseconds == pytest.approx(67.108864)
        assert pointwidth(14).microseconds == pytest.approx(16.384)
        assert pointwidth(8).nanoseconds == pytest.approx(256)

    def test_int_conversion(self):
        """
        Test converting a pointwidth into an int and back
        """
        assert int(pointwidth(42)) == 42
        assert isinstance(int(pointwidth(32)), int)
        assert isinstance(pointwidth(pointwidth(32)), pointwidth)

    def test_equality(self):
        """
        Test equality comparision of pointwidth
        """
        assert pointwidth(32) == pointwidth(32)
        assert 32 == pointwidth(32)
        assert pointwidth(32) == 32.0000

    def test_strings(self):
        """
        Test the string representation of pointwidth
        """
        assert "years" in str(pointwidth(55))
        assert "months" in str(pointwidth(52))
        assert "weeks" in str(pointwidth(50))
        assert "days" in str(pointwidth(47))
        assert "hours" in str(pointwidth(42))
        assert "minutes" in str(pointwidth(36))
        assert "seconds" in str(pointwidth(30))
        assert "milliseconds" in str(pointwidth(20))
        assert "microseconds" in str(pointwidth(10))
        assert "nanoseconds" in str(pointwidth(5))

    def test_decr(self):
        """
        Test decrementing a pointwidth
        """
        assert pointwidth(23).decr() == 22

    def test_incr(self):
        """
        Test incrementing a pointwidth
        """
        assert pointwidth(23).incr() == 24
