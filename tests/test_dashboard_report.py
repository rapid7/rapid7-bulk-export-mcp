"""Tests for rendering the dashboard cards as one HTML document."""

import re
from datetime import datetime
from decimal import Decimal

import pytest

from src.dashboard_queries import CARDS, run_card
from src.dashboard_report import is_cross_org, render_report
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
    assert "Data date (UTC): 2026-09-23T00:00:00+00:00" in html


def test_the_document_is_self_contained(cards):
    html = _html(cards)

    assert "<script" not in html
    assert not re.search(r'(src|href)="https?://', html)


def test_hostile_text_in_the_data_is_escaped():
    hostile = '<script>alert("x")</script> & co'
    card = {
        "card": "weaponised_critical",
        "title": "Weaponised <critical>",
        "definition": "Rows & columns",
        "as_of": "2026-09-23T00:00:00+00:00",
        "orgs": ["ORG-A"],
        "results": {"top_assets": [{"orgId": "ORG-A", "hostName": hostile, "risk_score": 1}]},
    }

    html = _html([card])

    assert "<script>" not in html
    assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt; &amp; co" in html
    assert "Weaponised &lt;critical&gt;" in html


def test_incomplete_coverage_leaves_out_every_cross_org_result(cards):
    html = _html(cards, complete=False, message="ORG-C <missing>")

    assert "Coverage is incomplete." in html
    assert "ORG-C &lt;missing&gt;" in html
    for card in cards:
        for name, rows in card["results"].items():
            if rows and is_cross_org(rows):
                section = html.split(f'id="{card["card"]}"', 1)[1].split("</section>", 1)[0]
                assert "Left out: this combines organisations" in section, f"{card['card']}.{name}"
    assert "ORG-A" in html


def test_complete_coverage_shows_the_cross_org_results(cards):
    html = _html(cards)

    assert "Coverage is incomplete." not in html
    assert "Left out" not in html


@pytest.mark.parametrize(
    ("value", "shown"),
    [(3549013, "3,549,013"), (Decimal("617381.30"), "617,381.3"), (Decimal("0.9422"), "0.9422"), (125.0, "125")],
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

    assert f'<td class="number">{shown}</td>' in _html([card])


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

    assert "from 2026-09-21T00:00:00+00:00 to 2026-09-25T00:00:00+00:00, by organisation" in _html([card])
