"""Tests for rendering the dashboard cards as one HTML document."""

import re
from datetime import datetime
from decimal import Decimal

import pytest

from src.dashboard_queries import CARDS, run_card
from src.dashboard_report import HEAT_RAMP, _label, _number_text, render_report
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
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.S)
    rest = re.sub(r"<script>.*?</script>", "", html, flags=re.S)
    without_fonts = re.sub(r"url\(data:font/woff2;base64,[A-Za-z0-9+/=]+\)", "", rest)
    without_svg_namespace = without_fonts.replace('xmlns="http://www.w3.org/2000/svg"', "")

    assert html.count("url(data:font/woff2;base64,") == 2
    assert len(scripts) == 1 and html.count("<script>") == 1, "one inline script, with no src"
    for marker in ("<script", "<link", "<iframe", "<img", "<object", "<embed", "@import", "http", "//"):
        assert marker not in without_svg_namespace.lower(), marker
    assert set(re.findall(r"url\(([^)]*)\)", without_svg_namespace)) <= {"#r7logo"}


def test_the_hover_script_reaches_nothing_outside_the_page_and_never_parses_html(cards):
    [script] = re.findall(r"<script>(.*?)</script>", _html(cards), re.S)

    for forbidden in (
        "fetch",
        "XMLHttpRequest",
        "WebSocket",
        "sendBeacon",
        "import",
        "eval",
        "Function(",
        "innerHTML",
        "outerHTML",
        "insertAdjacentHTML",
        "document.write",
        "http",
        "//",
        "src",
        "href",
        "localStorage",
        "cookie",
        "postMessage",
        'setTimeout("',
        "location",
    ):
        assert forbidden not in script, forbidden
    assert "textContent" in script


def test_hostile_text_in_a_tooltip_stays_text(cards):
    hostile = '<img src=x onerror="1">'
    rows = [{"week": "2026-08-31", "orgId": hostile, "new_critical_high": 1}]
    html = _html([_card({"by_week": rows}, per_org=["by_week"], card="new_vulnerabilities", orgs=[hostile])])

    assert "<img" not in html.split("<script>", 1)[0]
    assert "<title>&lt;img src=x onerror=&quot;1&quot;&gt; 2026-08-31: 1</title>" in html
    assert 'data-series="&lt;img src=x onerror=&quot;1&quot;&gt;"' in html


def test_the_report_uses_the_callisto_dark_theme_and_the_logo(cards):
    html = _html(cards)

    assert '<html lang="en" data-rds-brand="callisto" data-rds-mode="dark">' in html
    assert 'aria-label="Rapid7"' in html
    assert "--rds-bg-default: #181d1f" in html


def test_portfolio_totals_show_as_tiles(cards):
    portfolio = next(c for c in cards if c["card"] == "portfolio_summary")["results"]["portfolio"][0]
    section = _section(_html(cards), "portfolio_summary")

    assert f'<div class="k">Total critical findings</div><div class="v">{portfolio["critical"]:,}</div>' in section
    assert "Stands in for findings in CISA KEV" in section


def _shades(values):
    rows = [{"band": str(i), "critical": v} for i, v in enumerate(values)]
    html = _html([_card({"by_band": rows}, card="unresolved_age_severity")])
    return re.findall(r'<td(?: style="background:(#[0-9a-f]{6});color:#[0-9a-f]{6}")?>', html.split("<tbody>")[1])


@pytest.mark.parametrize(
    ("values", "steps"),
    [
        (list(range(1, 11)), [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]),
        ([1, 5, 5], [0, 4, 4]),
        ([1, 1, 1, 1, 100], [1, 1, 1, 1, 4]),
        ([7], [4]),
        ([0, 0, 100], [None, None, 4]),
        ([Decimal("2"), 1, 3.0, float("nan"), 4, 5], [1, 0, 2, None, 3, 4]),
    ],
)
def test_age_bands_shade_by_quintile_with_the_largest_darkest_and_zero_unshaded(values, steps):
    ramp = [background for background, _ in HEAT_RAMP]
    expected = [ramp[step] if step is not None else "" for step in steps]
    assert _shades(values) == expected


def test_a_tile_falls_back_to_a_table_when_its_column_is_missing():
    html = _html([_card({"portfolio": [{"active_total": 5}]})], complete=True)

    assert '<div class="k">Active findings</div>' not in html
    assert '<div class="k">Active total</div><div class="v">5</div>' in html


def test_hostile_text_is_escaped_in_tiles_and_the_heat_table():
    hostile = '<b x="1">&'
    cards = [
        _card({"portfolio": [{"critical": 1, "high": 1, "with_exploits": 1}]}),
        _card({"by_band": [{"band": hostile, "critical": 3}]}, card="unresolved_age_severity"),
        _card({"by_org": [{"orgId": hostile, "n": 1}]}, per_org=["by_org"], card="org_comparison", orgs=[hostile]),
    ]

    html = _html(cards)

    assert "<b " not in html
    assert '<td class="rl">&lt;b x=&quot;1&quot;&gt;&amp; days</td>' in html
    assert "</span>&lt;b x=&quot;1&quot;&gt;&amp;</td>" in html


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
        description=hostile,
        definition=hostile,
        as_of=hostile,
        orgs=[hostile],
    )

    html = render_report([card], hostile, coverage_complete=False, coverage_message=hostile)

    assert "<b " not in html
    assert f'id="{shown}"' in html
    assert html.count(shown) >= 9
    assert f'<p class="hint">{shown}</p>' in html


def _section(html, card):
    return html.split(f'id="{card}"', 1)[1].split("</section>", 1)[0]


def test_incomplete_coverage_hides_combined_results_and_keeps_per_org_ones(cards):
    html = _html(cards, complete=False, message="ORG-C <missing>")

    assert "Coverage is incomplete." in html
    assert "ORG-C &lt;missing&gt;" in html
    for card in cards:
        section = _section(html, card["card"])
        for name in card["results"]:
            if name in card["per_org"]:
                continue
            what = "Median" if card["card"] == "org_risk_distribution" else _label(name)
            assert f"<strong>{what}.</strong> Left out: this combines organisations" in section, (
                f"{card['card']}.{name}"
            )
    assert 'class="tiles"' not in _section(html, "portfolio_summary")
    assert '<table class="mx">' not in _section(html, "unresolved_age_severity")
    assert ">median " not in _section(html, "org_risk_distribution")
    assert "ORG-A</td>" in html


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


@pytest.mark.parametrize("card", ["new_vulnerabilities", "remediated_vulnerabilities"])
def test_weekly_results_show_as_line_charts_with_their_figures(cards, card):
    section = _section(_html(cards), card)

    assert section.count("<polyline") == 2
    assert section.count("<summary>Show the figures</summary>") == 1


def test_complete_coverage_draws_the_median_on_the_risk_chart(cards):
    median = next(c for c in cards if c["card"] == "org_risk_distribution")["results"]["portfolio_median"][0]

    section = _section(_html(cards), "org_risk_distribution")

    assert f">median {_number_text(median['median_of_org_means'])}<" in section


PROTOTYPE_COPY = {
    "portfolio_summary": ("Portfolio summary", "Totals across all selected organisations."),
    "unresolved_age_severity": (
        "Unresolved findings by days active and severity",
        "All selected organisations. Days active measured from discovery date.",
    ),
    "org_comparison": ("Organisation comparison", "Every selected organisation. Default sorted by risk score."),
    "new_vulnerabilities": (
        "New critical and high vulnerabilities",
        "Count of critical and high vulnerabilities first detected or reintroduced in each week.",
    ),
    "remediated_vulnerabilities": ("Remediated critical and high vulnerabilities", None),
    "org_risk_distribution": ("Organisation risk distribution", None),
}
"""Copy taken from the UX phase 1 prototype. None means the data cannot support the prototype's wording."""


def test_the_report_carries_the_ux_titles_and_copy(cards):
    html = _html(cards)

    assert "<h1>Multi-Org Exposure Dashboard</h1>" in html
    for card, (title, description) in PROTOTYPE_COPY.items():
        section = _section(html, card)
        assert f'<p class="sec">{title}</p>' in section, card
        if description:
            assert description in section, card
    age = _section(html, "unresolved_age_severity")
    assert '<p class="headline"><span class="lab">91+ days</span> <strong>' in age
    assert '<th class="rh"></th>' in age, "no Days active header, as in the prototype"
    assert 'Critical<span class="range">CVSS 9.0-10.0</span>' in age
    assert "light amber (lowest) to dark red (highest)" in age
    comparison = _section(html, "org_comparison")
    assert "Remediated Critical and High Vulnerabilities (reintroduced stands in)" in comparison
    assert "Total CISA KEV findings (known exploits stand in)" in comparison


@pytest.mark.parametrize(("complete", "opened"), [(True, False), (False, True)])
def test_the_by_organisation_table_opens_when_coverage_is_incomplete(cards, complete, opened):
    section = _section(_html(cards, complete=complete), "portfolio_summary")

    assert ("<details open><summary>By organisation</summary>" in section) is opened
    assert "<summary>By organisation</summary>" in section


def test_more_than_three_orgs_each_get_their_own_colour():
    orgs = [f"ORG-{i}" for i in range(10)]
    rows = [{"orgId": org, "n": 1} for org in orgs]
    html = _html([_card({"by_org": rows}, per_org=["by_org"], orgs=orgs)])

    swatches = re.findall(r'<span class="swatch" style="background:(#[0-9a-f]{6})"></span>', html)
    assert len(swatches) == 10 == len(set(swatches))


def test_incomplete_coverage_still_draws_the_per_org_charts(cards):
    section = _section(_html(cards, complete=False), "new_vulnerabilities")

    assert section.count("<polyline") == 2


def _weekly_card(rows, as_of="2026-11-13T10:48:00+00:00"):
    return _card({"by_week": rows}, per_org=["by_week"], card="new_vulnerabilities", as_of=as_of, as_of_by_org={})


def test_weekly_charts_plot_the_last_26_weeks_and_keep_every_week_in_the_figures():
    rows = [
        {"week": "2023-06-12", "orgId": "ORG-A", "new_critical_high": 1400000},
        {"week": "2026-11-02", "orgId": "ORG-A", "new_critical_high": 50},
    ]

    section = _section(_html([_weekly_card(rows)]), "new_vulnerabilities")
    chart, figures = section.split("<summary>Show the figures</summary>", 1)

    assert len(re.findall(r'class="pt"', chart)) == 26
    assert 'data-heading="Week of 2026-11-09"' in chart, "the window ends at the week of the data date"
    assert 'data-heading="Week of 2026-05-25"' in chart and "2023-06-12" not in chart
    assert "2023-06-12" in figures and "1,400,000" in figures
    assert "Last 26 weeks to the data date" in section
    assert 'class="series lead marked"' in chart, "26 points leave room for the prototype's markers"


def test_an_unreadable_data_date_plots_every_week():
    rows = [{"week": "2026-08-31", "orgId": "ORG-A", "new_critical_high": 1}]

    section = _section(_html([_weekly_card(rows, as_of="not a date")]), "new_vulnerabilities")

    assert section.count('class="pt"') == 1
    assert "Last 26 weeks" not in section


KEV = {"version": "2026.11.13", "released": "2026-11-13T20:24:18Z", "sha256": "4028ec03" + "0" * 56, "entries": 1781}


def _kev_cards(kev):
    portfolio = _card(
        {"portfolio": [{"critical": 5, "high": 7, "with_exploits": 9, "kev_findings": 3 if kev else None}]},
        kev_catalogue=kev,
    )
    comparison = _card(
        {
            "by_org": [
                {
                    "orgId": "ORG-A",
                    "risk_score": 10.0,
                    "total_known_exploit_findings": 9,
                    "total_kev_findings": 3 if kev else None,
                    "kev_per_assessed_asset": 1.5 if kev else None,
                }
            ]
        },
        per_org=["by_org"],
        card="org_comparison",
        kev_catalogue=kev,
    )
    return [portfolio, comparison]


def test_a_stored_catalogue_gives_real_kev_figures_and_names_its_source():
    html = _html(_kev_cards(KEV))

    portfolio = _section(html, "portfolio_summary")
    comparison = _section(html, "org_comparison")
    assert '<div class="k">Total findings in CISA KEV</div><div class="v">3</div>' in portfolio
    assert "CISA KEV catalogue 2026.11.13, released 2026-11-13 20:24 UTC" in portfolio
    assert "Total CISA KEV findings</th>" in comparison and "CISA KEV per assessed asset</th>" in comparison
    assert "known exploits stand in" not in comparison
    assert "1,781 entries" in html
    assert KEV["sha256"] in html


def test_no_catalogue_keeps_the_stand_ins_and_says_so():
    html = _html(_kev_cards(None))

    assert '<div class="k">Total findings with a known exploit</div><div class="v">9</div>' in html
    assert "No CISA KEV catalogue is stored yet" in html
    comparison = _section(html, "org_comparison")
    assert "Total CISA KEV findings (known exploits stand in)" in comparison
    assert "CISA KEV per assessed asset" not in comparison


def test_hostile_catalogue_details_are_escaped():
    hostile = dict(KEV, version='<b x="1">', sha256="<script>x</script>")

    html = _html(_kev_cards(hostile))

    assert "<b x=" not in html and "<script>x" not in html
    assert "&lt;script&gt;x&lt;/script&gt;" in html


def test_a_stored_catalogue_with_no_cves_column_gives_the_right_reason():
    cards = _kev_cards(None)
    for card in cards:
        card["kev_unavailable"] = "no_cves_column"

    html = _html(cards)

    assert "has no list of CVE IDs per finding" in html
    assert "No CISA KEV catalogue is stored yet" not in html


def test_risk_scores_show_as_whole_numbers():
    rows = [{"orgId": "ORG-A", "risk_score": 645050.8, "total_assessed_assets": 2650}]

    html = _html([_card({"by_org": rows}, per_org=["by_org"], card="org_comparison")])

    assert '<td class="n">645,051</td>' in html
    assert "645,050.8" not in html


@pytest.mark.parametrize(("complete", "marked"), [(True, True), (False, False)])
def test_the_highest_risk_label_appears_only_with_complete_coverage(cards, complete, marked):
    section = _section(_html(cards, complete=complete), "org_risk_distribution")

    assert (">highest " in section) is marked
