"""Unit tests for the helpers that decide whether a card matches the dashboard."""

import json
from pathlib import Path

import pytest

from src.dashboard_queries import CARDS
from tests.test_dashboard_reference import fields_outside_tolerance, find_row

ROWS = [{"orgId": "A", "critical": 100, "high": 50}, {"orgId": "B", "critical": 0, "high": 3}]


def test_find_row_picks_the_matching_row():
    assert find_row(ROWS, {"orgId": "B"})["high"] == 3


@pytest.mark.parametrize("match", [{"orgId": "missing"}, {}])
def test_find_row_refuses_zero_or_several_matches(match):
    with pytest.raises(AssertionError, match="exactly one row"):
        find_row(ROWS, match)


@pytest.mark.parametrize(
    ("expected", "tolerance_pct", "problem_count"),
    [
        ({"critical": 100, "high": 50}, 0, 0),
        ({"critical": 101}, 0, 1),
        ({"critical": 101}, 1, 0),
        ({"critical": 102}, 1, 1),
        ({"low": 5}, 0, 1),
    ],
)
def test_fields_outside_tolerance(expected, tolerance_pct, problem_count):
    assert len(fields_outside_tolerance(ROWS[0], expected, tolerance_pct)) == problem_count


def test_a_zero_expected_value_allows_no_difference():
    assert fields_outside_tolerance(ROWS[1], {"critical": 0}, 50) == []
    assert fields_outside_tolerance(ROWS[1], {"high": 0}, 50) != []


def test_the_example_reference_names_real_cards_and_queries():
    example = json.loads(Path("tests/fixtures/dashboard_reference.example.json").read_text())

    for check in example["checks"]:
        assert check["query"] in {name for name, _ in CARDS[check["card"]].queries}
