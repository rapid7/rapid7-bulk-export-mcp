"""Tests for rendering the dashboard cards as one HTML document."""

import re
from datetime import datetime
from decimal import Decimal

import pytest

from src.dashboard_queries import CARDS, run_card
from src.dashboard_report import _label, render_report
from tests.test_dashboard_queries import _load

AS_OF = datetime(2026, 9, 23)


@pytest.fixture
def cards(tmp_path):
    db = _load(tmp_path)
    try:
        yield [run_card(db, name, as_of=AS_OF) for name in CARDS]
    finally:
        db.close()


def _html(cards, complete=True, message=None):
    return render_report(cards, "2026-09-23T09:00:00+00:00", coverage_complete=complete, coverage_message=message)


def test_every_card_appears_with_its_definition_and_data_date(cards):
    html = _html(cards)

    for card in cards:
        assert f'id="{card["card"]}"' in html
        assert card["title"] in html
        assert card["definition"].split(".")[0] in html
    assert "Data date: 2026-09-23 00:00 UTC" in html


def test_the_document_is_self_contained(cards):
    html = _html(cards)
    without_fonts = re.sub(r"url\(data:font/woff2;base64,[A-Za-z0-9+/=]+\)", "", html)
    without_svg_namespace = without_fonts.replace('xmlns="http://www.w3.org/2000/svg"', "")

    assert html.count("url(data:font/woff2;base64,") == 2
    for marker in ("<script", "<link", "<iframe", "<img", "<object", "<embed", "@import", "http", "//"):
        assert marker not in without_svg_namespace.lower(), marker
    assert set(re.findall(r"url\(([^)]*)\)", without_svg_namespace)) <= {"#r7logo"}


def test_the_report_uses_the_callisto_dark_theme_and_the_logo(cards):
    html = _html(cards)

    assert '<html lang="en" data-rds-brand="callisto" data-rds-mode="dark">' in html
    assert 'aria-label="Rapid7"' in html
    assert "--rds-bg-default: #181d1f" in html


def test_portfolio_totals_show_as_tiles(cards):
    portfolio = next(c for c in cards if c["card"] == "portfolio_summary")["results"]["portfolio"][0]
    section = _section(_html(cards), "portfolio_summary")

    assert f'<div class="k">Active critical</div><div class="v">{portfolio["critical"]:,}</div>' in section


def test_age_bands_use_the_heat_ramp_from_warning_to_critical():
    rows = [{"band": "0-7", "critical": 1, "high": 2}, {"band": "91+", "critical": 50, "high": 9}]
    html = _html([_card({"by_band": rows}, card="unresolved_age_severity")])

    assert '<td style="background:#ffe100;color:#181d1f">1</td>' in html
    assert '<td style="background:#982c35;color:#ffffff">50</td>' in html


def test_each_org_keeps_one_colour_on_every_card():
    rows = [{"orgId": "ORG-B", "n": 1}, {"orgId": "ORG-A", "n": 2}]
    cards = [_card({"by_org": rows}, per_org=["by_org"], card=f"c{i}", orgs=["ORG-A", "ORG-B"]) for i in (1, 2)]
    html = _html(cards)

    assert html.count('<span class="swatch" style="background:#77b8ec"></span>ORG-A') == 2
    assert html.count('<span class="swatch" style="background:#ff3d82"></span>ORG-B') == 2


def _card(results, per_org=(), **overrides):
    card = {
        "card": "portfolio_summary",
        "title": "t",
        "definition": "d",
        "as_of": "2026-09-23T00:00:00+00:00",
        "orgs": ["ORG-A"],
        "per_org": list(per_org),
        "results": results,
    }
    card.update(overrides)
    return card


def test_hostile_text_is_escaped_on_every_path():
    hostile = '<b x="1">&'
    shown = "&lt;b x=&quot;1&quot;&gt;&amp;"
    card = _card(
        {hostile: [{"orgId": hostile, hostile: hostile}]},
        per_org=[hostile],
        card=hostile,
        title=hostile,
        definition=hostile,
        as_of=hostile,
        orgs=[hostile],
    )

    html = render_report([card], hostile, coverage_complete=False, coverage_message=hostile)

    assert "<b " not in html
    assert f'id="{shown}"' in html
    assert html.count(shown) >= 8


def _section(html, card):
    return html.split(f'id="{card}"', 1)[1].split("</section>", 1)[0]


def test_incomplete_coverage_hides_combined_results_and_keeps_per_org_ones(cards):
    complete = _html(cards)
    html = _html(cards, complete=False, message="ORG-C <missing>")

    assert "Coverage is incomplete." in html
    assert "ORG-C &lt;missing&gt;" in html
    for card in cards:
        section = _section(html, card["card"])
        for name in card["results"]:
            heading = f"<h3>{_label(name)}</h3>"
            after = section.split(heading, 1)[1].split("<h3>", 1)[0]
            if name in card["per_org"]:
                assert "<table" in after or "No rows." in after, f"{card['card']}.{name}"
            else:
                assert "Left out: this combines organisations" in after, f"{card['card']}.{name}"
                assert "<table" not in after, f"{card['card']}.{name}"
    portfolio = next(c for c in cards if c["card"] == "portfolio_summary")
    total = f"{portfolio['results']['portfolio'][0]['active_total']:,}"
    assert f'<div class="v">{total}</div>' in _section(complete, "portfolio_summary")
    assert "ORG-A</td>" in html


def test_the_cross_org_top_assets_ranking_is_hidden_when_coverage_is_incomplete(cards):
    section = _section(_html(cards, complete=False), "weaponised_critical")

    assert "Left out" in section.split("<h3>Top assets</h3>", 1)[1]


def test_an_empty_combined_result_is_hidden_rather_than_shown_as_zero():
    html = _html([_card({"top_solutions": []})], complete=False)

    assert "No rows." not in html
    assert "Left out" in html


def test_a_card_without_a_per_org_list_hides_everything_when_coverage_is_incomplete():
    card = _card({"by_org": [{"orgId": "ORG-A", "n": 1}]})
    del card["per_org"]

    assert "<table" not in _html([card], complete=False)


def test_complete_coverage_shows_the_cross_org_results(cards):
    html = _html(cards)

    assert "Coverage is incomplete." not in html
    assert "Left out" not in html


@pytest.mark.parametrize(
    ("value", "shown"),
    [
        (3549013, "3,549,013"),
        (Decimal("617381.30"), "617,381.3"),
        (Decimal("0.9422"), "0.9422"),
        (125.0, "125"),
        (-1234.5, "-1,234.5"),
        (-0.0, "0"),
        (Decimal("-0.00"), "0"),
        (float("nan"), ""),
        (float("inf"), ""),
    ],
)
def test_numbers_keep_their_precision_and_get_separators(value, shown):
    card = {
        "card": "portfolio_summary",
        "title": "t",
        "definition": "d",
        "as_of": "2026-09-23T00:00:00+00:00",
        "orgs": ["ORG-A"],
        "results": {"by_org": [{"orgId": "ORG-A", "figure": value}]},
    }

    assert f'<td class="n">{shown}</td>' in _html([card])


def test_different_org_data_dates_are_shown_as_a_range():
    card = {
        "card": "portfolio_summary",
        "title": "t",
        "definition": "d",
        "as_of": "2026-09-21T00:00:00+00:00",
        "as_of_by_org": {"ORG-A": "2026-09-21T00:00:00+00:00", "ORG-B": "2026-09-25T00:00:00+00:00"},
        "orgs": ["ORG-A", "ORG-B"],
        "results": {"by_org": []},
    }

    assert "from 2026-09-21 00:00 UTC to 2026-09-25 00:00 UTC, by organisation" in _html([card])


def test_a_single_combined_number_shows_as_a_tile():
    html = _html([_card({"portfolio_median": [{"median_of_org_means": Decimal("617381.30")}]})])

    assert '<div class="k">Median of org means</div><div class="v">617,381.3</div>' in html
    assert "<table" not in html


def test_tiles_come_before_the_per_org_table(cards):
    section = _section(_html(cards), "portfolio_summary")

    assert section.index('class="tiles"') < section.index("<table")
