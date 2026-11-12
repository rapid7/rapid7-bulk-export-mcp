"""Tests for the inline SVG charts in the dashboard report."""

import re
from decimal import Decimal

import pytest

from src.report_charts import bar_chart, dot_plot, line_chart, nice_max


def _fmt(value):
    return f"{float(value):,.0f}"


COLOURS = {"ORG-A": "#77b8ec", "ORG-B": "#ff3d82"}
HOSTILE = '<b x="1">&'
SHOWN = "&lt;b x=&quot;1&quot;&gt;&amp;"


@pytest.mark.parametrize(
    ("value", "top"),
    [(0, 1), (-5, 1), (1, 1), (7, 10), (11, 20), (21, 25), (26, 50), (51, 100), (1_400_000, 2_000_000)],
)
def test_nice_max_rounds_up_to_a_readable_axis_top(value, top):
    assert nice_max(value) == top


def test_a_line_chart_draws_one_line_per_org_and_counts_a_missing_quarter_as_zero():
    rows = [
        {"quarter": "2026-01", "orgId": "ORG-A", "n": 10},
        {"quarter": "2026-04", "orgId": "ORG-A", "n": 20},
        {"quarter": "2026-04", "orgId": "ORG-B", "n": Decimal("5")},
    ]

    svg = line_chart(rows, "quarter", "n", "orgId", COLOURS, _fmt)

    lines = re.findall(r'<polyline points="([^"]+)" fill="none" stroke="(#[0-9a-f]{6})"', svg)
    assert [colour for _, colour in lines] == ["#77b8ec", "#ff3d82"]
    org_b_first_y = float(lines[1][0].split()[0].split(",")[1])
    assert org_b_first_y == 180.0
    assert "<title>ORG-B 2026-01: 0</title>" in svg


def test_dot_plot_handles_one_org_and_draws_no_cross_org_marker():
    svg = dot_plot([{"orgId": "ORG-A", "mean_risk": 617381.3}], "mean_risk", "orgId", COLOURS, _fmt)

    assert svg.count("<circle") == 1
    assert "median" not in svg and "highest" not in svg


def test_bar_chart_draws_one_bar_per_row():
    rows = [{"solution_type": "PATCH", "findings": 10}, {"solution_type": "ROLLUP", "findings": 0}]

    svg = bar_chart(rows, "solution_type", "findings", _fmt)

    assert svg.count("<rect") == 2
    assert 'height="0.00"' in svg


@pytest.mark.parametrize("chart", ["line", "dot", "bar"])
def test_every_chart_label_is_escaped(chart):
    rows = [{"quarter": HOSTILE, "orgId": HOSTILE, "n": 3, "mean_risk": 3, "solution_type": HOSTILE, "findings": 3}]
    svg = {
        "line": lambda: line_chart(rows, "quarter", "n", "orgId", {}, _fmt),
        "dot": lambda: dot_plot(rows, "mean_risk", "orgId", {}, _fmt),
        "bar": lambda: bar_chart(rows, "solution_type", "findings", _fmt),
    }[chart]()

    assert "<b " not in svg
    assert SHOWN in svg


@pytest.mark.parametrize("chart", ["line", "dot", "bar"])
def test_no_rows_draws_nothing(chart):
    svg = {
        "line": lambda: line_chart([], "quarter", "n", "orgId", COLOURS, _fmt),
        "dot": lambda: dot_plot([], "mean_risk", "orgId", COLOURS, _fmt),
        "bar": lambda: bar_chart([], "solution_type", "findings", _fmt),
    }[chart]()

    assert svg == ""


def test_a_non_finite_value_is_drawn_as_zero_rather_than_breaking_the_chart():
    rows = [
        {"quarter": "2026-01", "orgId": "ORG-A", "n": float("nan")},
        {"quarter": "2026-04", "orgId": "ORG-A", "n": 4},
    ]

    svg = line_chart(rows, "quarter", "n", "orgId", COLOURS, _fmt)

    assert "nan" not in svg.lower()
    assert "<title>ORG-A 2026-01: 0</title>" in svg
    first_y = float(re.search(r'<polyline points="([^"]+)"', svg).group(1).split()[0].split(",")[1])
    assert first_y == 180.0


def test_a_gap_between_quarters_keeps_its_width_and_a_missing_quarter_is_dropped():
    rows = [
        {"quarter": "2024-01", "orgId": "ORG-A", "n": 1},
        {"quarter": "2024-10", "orgId": "ORG-A", "n": 2},
        {"quarter": None, "orgId": "ORG-A", "n": 9},
    ]

    svg = line_chart(rows, "quarter", "n", "orgId", COLOURS, _fmt)

    points = re.search(r'<polyline points="([^"]+)"', svg).group(1).split()
    assert len(points) == 4
    assert "None" not in svg
    assert "<title>ORG-A 2024-04: 0</title>" in svg


def test_an_org_with_no_risk_score_is_left_off_the_dot_plot():
    rows = [{"orgId": "ORG-A", "mean_risk": 100}, {"orgId": "ORG-B", "mean_risk": None}]

    svg = dot_plot(rows, "mean_risk", "orgId", COLOURS, _fmt)

    assert svg.count("<circle") == 1
    assert "ORG-B" not in svg


def test_negative_and_huge_values_still_give_valid_charts():
    bars = bar_chart([{"t": "a", "n": -5}, {"t": "b", "n": 1.7e308}], "t", "n", _fmt)
    line = line_chart([{"quarter": "2026-01", "orgId": "ORG-A", "n": -3}], "quarter", "n", "orgId", COLOURS, _fmt)

    assert 'height="-' not in bars
    assert "inf" not in bars.lower() and "nan" not in bars.lower()
    assert float(re.search(r'points="([^"]+)"', line).group(1).split(",")[1]) <= 180.0


def test_axis_title_and_ticks_are_escaped():
    svg = bar_chart([{"t": "a", HOSTILE: 3}], "t", HOSTILE, _fmt)

    assert "<b " not in svg
