"""Tests for the inline SVG charts in the dashboard report."""

import re
from decimal import Decimal

import pytest

from src.report_charts import _MUTED_TIP, dot_plot, line_chart, nice_max


def _fmt(value):
    return f"{float(value):,.0f}"


COLOURS = {"ORG-A": "#77b8ec", "ORG-B": "#ff3d82"}
BASELINE = 230.0
"""The y of a zero value in a line chart."""
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

    lines = dict(
        (series, (points, colour))
        for series, points, colour in re.findall(
            r'data-series="([^"]+)"[^>]*><polyline points="([^"]+)" fill="none" stroke="(#[0-9a-f]{6})"', svg
        )
    )
    assert {series: colour for series, (_, colour) in lines.items()} == {"ORG-A": "#77b8ec", "ORG-B": "#ff3d82"}
    org_b_first_y = float(lines["ORG-B"][0].split()[0].split(",")[1])
    assert org_b_first_y == BASELINE
    assert "<title>ORG-B 2026-01: 0</title>" in svg


def test_dot_plot_handles_one_org_and_draws_no_cross_org_marker():
    svg = dot_plot([{"orgId": "ORG-A", "mean_risk": 617381.3}], "mean_risk", "orgId", COLOURS, _fmt)

    assert svg.count("<circle") == 1
    assert "median" not in svg and "highest" not in svg


@pytest.mark.parametrize("chart", ["line", "dot"])
def test_every_chart_label_is_escaped(chart):
    rows = [{"quarter": HOSTILE, "orgId": HOSTILE, "n": 3, "mean_risk": 3, "solution_type": HOSTILE, "findings": 3}]
    svg = {
        "line": lambda: line_chart(rows, "quarter", "n", "orgId", {}, _fmt),
        "dot": lambda: dot_plot(rows, "mean_risk", "orgId", {}, _fmt),
    }[chart]()

    assert "<b " not in svg
    assert SHOWN in svg


@pytest.mark.parametrize("chart", ["line", "dot"])
def test_no_rows_draws_nothing(chart):
    svg = {
        "line": lambda: line_chart([], "quarter", "n", "orgId", COLOURS, _fmt),
        "dot": lambda: dot_plot([], "mean_risk", "orgId", COLOURS, _fmt),
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
    assert first_y == BASELINE


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
    rows = [
        {"quarter": "2026-01", "orgId": "ORG-A", "n": -3},
        {"quarter": "2026-04", "orgId": "ORG-A", "n": 1.7e308},
    ]

    svg = line_chart(rows, "quarter", "n", "orgId", COLOURS, _fmt)

    assert "inf" not in svg.lower() and "nan" not in svg.lower()
    assert float(re.search(r'points="([^"]+)"', svg).group(1).split()[0].split(",")[1]) <= BASELINE


def test_axis_title_and_ticks_are_escaped():
    svg = line_chart(
        [{"quarter": "2026-01", "orgId": "ORG-A", "n": 3}], "quarter", "n", "orgId", {}, _fmt, y_title=HOSTILE
    )

    assert "<b " not in svg
    assert SHOWN in svg


def test_an_org_with_no_rows_still_gets_a_flat_line_and_a_legend_entry():
    rows = [{"quarter": "2026-01", "orgId": "ORG-A", "n": 4}]

    svg = line_chart(rows, "quarter", "n", "orgId", COLOURS, _fmt, series=["ORG-A", "ORG-B"])

    assert svg.count("<polyline") == 2
    assert "<title>ORG-B 2026-01: 0</title>" in svg
    assert "</i>ORG-B</span>" in svg


def test_axis_ticks_carry_no_float_error():
    rows = [{"orgId": "ORG-A", "mean_risk": 139837.4}, {"orgId": "ORG-B", "mean_risk": 617381.3}]

    svg = dot_plot(rows, "mean_risk", "orgId", COLOURS, lambda v: repr(float(v)))

    assert ">378609.35<" in svg


def test_weekly_gaps_keep_their_width():
    rows = [
        {"week": "2026-08-31", "orgId": "ORG-A", "n": 1},
        {"week": "2026-09-21", "orgId": "ORG-A", "n": 2},
    ]

    svg = line_chart(rows, "week", "n", "orgId", COLOURS, _fmt)

    assert len(re.search(r'<polyline points="([^"]+)"', svg).group(1).split()) == 4
    assert "<title>ORG-A 2026-09-07: 0</title>" in svg and "<title>ORG-A 2026-09-14: 0</title>" in svg


def test_dates_that_are_not_weekly_are_left_as_given():
    rows = [{"week": "2026-08-31", "orgId": "ORG-A", "n": 1}, {"week": "2026-09-02", "orgId": "ORG-A", "n": 2}]

    svg = line_chart(rows, "week", "n", "orgId", COLOURS, _fmt)

    assert len(re.search(r'<polyline points="([^"]+)"', svg).group(1).split()) == 2


def test_the_dot_plot_draws_a_median_only_when_given_one():
    rows = [{"orgId": "ORG-A", "mean_risk": 100}, {"orgId": "ORG-B", "mean_risk": 300}]

    assert "median" not in dot_plot(rows, "mean_risk", "orgId", COLOURS, _fmt)
    assert ">median 200<" in dot_plot(rows, "mean_risk", "orgId", COLOURS, _fmt, median=200)


@pytest.mark.parametrize(
    ("x_key", "first", "last"),
    [("week", "1969-12-29", "2026-11-09"), ("quarter", "1970-01", "2026-10")],
)
def test_a_stray_old_date_never_cuts_off_the_latest_period(x_key, first, last):
    rows = [{x_key: first, "orgId": "ORG-A", "n": 1}, {x_key: last, "orgId": "ORG-A", "n": 500}]

    svg = line_chart(rows, x_key, "n", "orgId", COLOURS, _fmt)

    assert f"<title>ORG-A {last}: 500</title>" in svg


def test_only_the_three_largest_series_lead_as_the_prototype_draws_them():
    rows = [{"week": "2026-08-31", "orgId": f"ORG-{n}", "n": n} for n in range(1, 6)]
    colours = {f"ORG-{n}": "#77b8ec" for n in range(1, 6)}

    svg = line_chart(rows, "week", "n", "orgId", colours, _fmt)

    leads = re.findall(r'<g class="series lead[^"]*" data-series="([^"]+)"', svg)
    assert sorted(leads) == ["ORG-3", "ORG-4", "ORG-5"]
    assert svg.count('data-colour="#9aa5aa"') == 2, "the tooltip dot uses the prototype's muted swatch"


def test_each_hover_target_carries_its_tooltip_parts_escaped():
    rows = [{"week": "2026-08-31", "orgId": "ORG-A", "n": 3}]

    svg = line_chart(rows, "week", "n", "orgId", COLOURS, _fmt, y_title=HOSTILE)

    assert 'class="pt" data-heading="Week of 2026-08-31"' in svg
    assert f'data-metric="{SHOWN}" data-value="3"' in svg


def test_a_long_range_hides_markers_and_draws_the_largest_series_on_top():
    rows = [{"week": "2026-01-05", "orgId": "BIG", "n": 9}, {"week": "2026-12-28", "orgId": "BIG", "n": 9}]

    svg = line_chart(rows, "week", "n", "orgId", {"BIG": "#77b8ec"}, _fmt, series=["ZERO"])

    assert "marked" not in svg, "more than 26 weeks, so no series shows markers"
    assert svg.index('data-series="ZERO"') < svg.index('data-series="BIG"')


def test_a_short_range_marks_the_leading_series():
    rows = [{"week": "2026-08-31", "orgId": "ORG-A", "n": 3}]

    assert 'class="series lead marked"' in line_chart(rows, "week", "n", "orgId", COLOURS, _fmt)


def test_only_the_largest_series_keep_their_colour_and_the_rest_draw_grey():
    rows = [{"week": "2026-08-31", "orgId": f"ORG-{n}", "n": n} for n in range(1, 6)]
    colours = {f"ORG-{n}": "#77b8ec" for n in range(1, 6)}

    svg = line_chart(rows, "week", "n", "orgId", colours, _fmt)

    assert svg.count('stroke="rgba(248,250,250,0.38)" stroke-width="1.6"') == 2
    assert svg.count('stroke="#77b8ec" stroke-width="2.4"') == 3
    assert svg.count('<i style="background:rgba(248,250,250,0.38)"></i>') == 2, "the legend matches the line"


def test_line_charts_have_no_y_axis_title():
    svg = line_chart(
        [{"week": "2026-08-31", "orgId": "ORG-A", "n": 1}], "week", "n", "orgId", COLOURS, _fmt, y_title="Count"
    )

    assert "rotate(-90" not in svg
    assert 'data-metric="Count"' in svg, "the tooltip still names the metric"


def test_the_dot_plot_labels_the_highest_only_when_asked():
    rows = [{"orgId": "ORG-A", "mean_risk": 100}, {"orgId": "ORG-B", "mean_risk": 300}]

    assert "highest" not in dot_plot(rows, "mean_risk", "orgId", COLOURS, _fmt)
    assert ">highest 300<" in dot_plot(rows, "mean_risk", "orgId", COLOURS, _fmt, mark_highest=True)


def test_muted_dots_draw_tooltip_and_legend_in_one_grey():
    rows = [{"orgId": f"ORG-{n}", "mean_risk": n} for n in range(1, 6)]
    colours = {f"ORG-{n}": "#77b8ec" for n in range(1, 6)}

    svg = dot_plot(rows, "mean_risk", "orgId", colours, _fmt)

    assert svg.count('fill="rgba(248,250,250,0.55)"') == 2
    assert svg.count('data-colour="#9aa5aa"') == 2
    assert svg.count('<i style="background:rgba(248,250,250,0.38)"></i>') == 2


def test_the_dot_plot_fills_the_card_width():
    svg = dot_plot(
        [{"orgId": "ORG-A", "mean_risk": 1}, {"orgId": "ORG-B", "mean_risk": 9}], "mean_risk", "orgId", COLOURS, _fmt
    )

    assert 'viewBox="0 0 1000 160"' in svg and "max-width" not in svg


def test_equal_scores_stack_and_keep_the_coloured_dots_visible():
    rows = [{"orgId": f"ORG-{i}", "mean_risk": 100} for i in range(6)] + [{"orgId": "ORG-LOW", "mean_risk": 1}]

    svg = dot_plot(rows, "mean_risk", "orgId", COLOURS, _fmt)

    heights = sorted({int(y) for y in re.findall(r'<circle cx="960.00" cy="(\d+)"', svg)})
    assert len(heights) == 5, "six equal scores spread over the five lanes"
    last_three = re.findall(r'data-colour="([^"]+)"', svg)[-3:]
    assert _MUTED_TIP not in last_three, "the coloured dots are drawn last, on top"


@pytest.mark.parametrize(("peak", "top"), [(0, 4), (3, 4), (6, 8), (880, 1000), (16521, 20000)])
def test_a_count_axis_has_whole_number_ticks(peak, top):
    from src.report_charts import count_max

    assert count_max(peak) == top
    assert all(float(top * i / 4).is_integer() for i in range(5))


def test_an_all_zero_count_chart_has_no_fractional_ticks():
    rows = [{"week": "2026-11-02", "n": 0, "org": "A"}, {"week": "2026-11-09", "n": 0, "org": "A"}]

    svg = line_chart(rows, "week", "n", "org", COLOURS, _fmt)

    assert ">0.25<" not in svg and ">0.5<" not in svg and ">4<" in svg


def test_a_line_chart_carries_what_the_script_needs_to_rescale_it():
    rows = [{"week": "2026-11-02", "n": 1234567, "org": "A"}, {"week": "2026-11-09", "n": 3, "org": "B"}]

    svg = line_chart(rows, "week", "n", "org", COLOURS, _fmt)

    assert 'data-top="2000000" data-y0="20" data-y1="230" data-counts="1"' in svg
    assert 'data-y="1234567"' in svg, "raw values keep full precision"
    assert svg.count('class="ytick"') == 5
