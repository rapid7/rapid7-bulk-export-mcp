"""Tests for the prebaked dashboard card queries, against a hand-countable dataset.

Two organisations. ORG-A has two excepted findings: V4 (medium) and V6 (critical,
exploitable and reintroduced, so it would reach every count if the active rule were
dropped). It also has one reintroduced finding (V3). ORG-B has an Unknown severity
finding (V5) and one critical finding with a known exploit.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.dashboard_queries import CARDS, run_card
from src.duckdb_loader import VulnerabilityDatabase

AS_OF = datetime(2026, 9, 23)

VULNS = [
    # orgId, assetId, vulnId, severity, exploits, firstFound, reintroduced, vector, epss
    ("ORG-A", "A1", "V1", "Critical", True, datetime(2026, 9, 20), None, "Network", 0.9),
    ("ORG-A", "A1", "V2", "High", False, datetime(2026, 6, 1), None, "Local", 0.1),
    ("ORG-A", "A2", "V3", "Critical", True, datetime(2026, 1, 10), datetime(2026, 9, 1), "Network", 0.6),
    ("ORG-A", "A2", "V4", "Medium", False, datetime(2026, 8, 1), None, "Network", 0.2),
    ("ORG-B", "B1", "V1", "Critical", True, datetime(2026, 7, 1), None, "Network", 0.4),
    ("ORG-B", "B1", "V5", "Unknown", False, datetime(2026, 9, 22), None, "Network", 0.0),
    ("ORG-A", "A1", "V6", "Critical", True, datetime(2026, 2, 1), datetime(2026, 9, 10), "Network", 0.95),
]
ASSETS = [("ORG-A", "A1", 100.0), ("ORG-A", "A2", 300.0), ("ORG-B", "B1", 50.0)]
EXCEPTIONS = [("ORG-A", "A2", "V4"), ("ORG-A", "A1", "V6")]


def _utc(when):
    return when.replace(tzinfo=timezone.utc).isoformat()


def _write(path, columns):
    pq.write_table(pa.table(columns), path)
    return str(path)


def _load(tmp_path, with_exceptions=True, rows=VULNS, assets=ASSETS):
    vulns = _write(
        tmp_path / "vulns.parquet",
        {
            "orgId": [v[0] for v in rows],
            "assetId": [v[1] for v in rows],
            "vulnId": [v[2] for v in rows],
            "cvssV3Severity": pa.array([v[3] for v in rows], pa.string()),
            "hasExploits": [v[4] for v in rows],
            "firstFoundTimestamp": pa.array([v[5] for v in rows], pa.timestamp("us")),
            "reintroducedTimestamp": pa.array([v[6] for v in rows], pa.timestamp("us")),
            "cvssV3AttackVector": [v[7] for v in rows],
            "epssscore": [v[8] for v in rows],
            "cvssV3Score": [9.0] * len(rows),
            "title": [f"Vuln {v[2]}" for v in rows],
            "bestSolutionSummary": [f"Fix {v[2]}" for v in rows],
            "bestSolutionType": ["patch"] * len(rows),
        },
    )
    assets = _write(
        tmp_path / "assets.parquet",
        {
            "orgId": [a[0] for a in assets],
            "assetId": [a[1] for a in assets],
            "riskScore": [a[2] for a in assets],
            "hostName": [f"host-{a[1]}" for a in assets],
            "osProduct": ["Linux"] * len(assets),
        },
    )
    prefixes = {"asset_vulnerability": [vulns], "asset": [assets]}
    if with_exceptions:
        prefixes["vulnerability_exception"] = [
            _write(
                tmp_path / "exceptions.parquet",
                {
                    "orgId": [e[0] for e in EXCEPTIONS],
                    "assetId": [e[1] for e in EXCEPTIONS],
                    "vulnId": [e[2] for e in EXCEPTIONS],
                },
            )
        ]
    db = VulnerabilityDatabase(str(Path(tmp_path) / "dashboard.db"))
    db.load_parquet_files_by_prefix(prefixes)
    return db


@pytest.fixture
def db(tmp_path):
    database = _load(tmp_path)
    yield database
    database.close()


def _by_org(rows):
    return {row["orgId"]: row for row in rows}


def test_portfolio_summary_excludes_the_excepted_finding(db):
    result = run_card(db, "portfolio_summary", as_of=AS_OF)

    by_org = _by_org(result["results"]["by_org"])
    assert by_org["ORG-A"]["active_total"] == 3
    assert by_org["ORG-A"]["medium"] == 0
    assert by_org["ORG-B"]["unscored"] == 1
    assert result["results"]["portfolio"] == [
        {
            "active_total": 5,
            "assets_with_findings": 3,
            "critical": 3,
            "high": 1,
            "medium": 0,
            "low": 0,
            "with_exploits": 3,
        }
    ]


def test_org_risk_distribution_median_counts_each_org_once(db):
    result = run_card(db, "org_risk_distribution", as_of=AS_OF)

    by_org = _by_org(result["results"]["by_org"])
    assert (by_org["ORG-A"]["mean_risk"], by_org["ORG-A"]["max_risk"]) == (200.0, 300.0)
    assert by_org["ORG-B"]["mean_risk"] == 50.0
    assert result["results"]["portfolio_median"] == [{"median_of_org_means": 125.0}]


def test_new_vulnerabilities_use_the_week_a_finding_became_active(db):
    result = run_card(db, "new_vulnerabilities", as_of=AS_OF)

    new = {(r["week"], r["orgId"]): r["new_critical_high"] for r in result["results"]["by_week"]}
    assert new == {
        ("2026-06-01", "ORG-A"): 1,
        ("2026-06-29", "ORG-B"): 1,
        ("2026-08-31", "ORG-A"): 1,
        ("2026-09-14", "ORG-A"): 1,
    }


def test_remediated_vulnerabilities_count_reintroductions_by_week(db):
    result = run_card(db, "remediated_vulnerabilities", as_of=AS_OF)

    assert result["results"]["by_week"] == [{"week": "2026-08-31", "orgId": "ORG-A", "reintroduced_critical_high": 1}]


def test_unresolved_age_severity_counts_days_from_discovery(db):
    """V3 was reintroduced 22 days before AS_OF but discovered 256 days before, so it is 91+."""
    result = run_card(db, "unresolved_age_severity", as_of=AS_OF)

    bands = {row["band"]: row for row in result["results"]["by_band"]}
    assert list(bands) == ["0-7", "61-90", "91+"]
    assert bands["0-7"]["critical"] == 1
    assert bands["61-90"]["critical"] == 1
    assert (bands["91+"]["critical"], bands["91+"]["high"]) == (1, 1)


def test_org_comparison_gives_each_ux_column_per_org(db):
    result = run_card(db, "org_comparison", as_of=AS_OF)

    by_org = _by_org(result["results"]["by_org"])
    assert by_org["ORG-A"] == {
        "orgId": "ORG-A",
        "risk_score": 200.0,
        "total_assessed_assets": 2,
        "total_critical_vulnerabilities": 2,
        "average_vulnerabilities_per_asset": 1.5,
        "average_known_exploits_per_asset": 1.0,
        "reintroduced_critical_high": 1,
    }
    assert by_org["ORG-B"]["average_vulnerabilities_per_asset"] == 2.0
    assert [row["orgId"] for row in result["results"]["by_org"]] == ["ORG-A", "ORG-B"]


def test_the_cards_follow_the_ux_phase_1_prototype():
    assert [card.title for card in CARDS.values()] == [
        "Portfolio summary",
        "Organisation risk distribution",
        "New critical and high vulnerabilities",
        "Remediated critical and high vulnerabilities",
        "Unresolved findings by days active and severity",
        "Organisation comparison",
    ]
    assert CARDS["portfolio_summary"].description == "Totals across all selected organisations."
    assert CARDS["unresolved_age_severity"].description == (
        "All selected organisations. Days active measured from discovery date."
    )
    assert CARDS["org_comparison"].description == "Every selected organisation. Default sorted by risk score."
    assert "weaponised_critical" not in CARDS


@pytest.mark.parametrize("card", list(CARDS))
def test_every_card_states_its_orgs_data_date_and_definition(db, card):
    result = run_card(db, card, as_of=AS_OF)

    assert result["orgs"] == ["ORG-A", "ORG-B"]
    assert result["as_of"] == _utc(AS_OF)
    assert result["definition"] and result["description"]
    assert set(result["results"]) == {name for name, _ in CARDS[card].queries}


def test_as_of_defaults_to_when_the_vulnerability_data_was_loaded(db):
    loaded_at = db.get_load_metadata()["vulnerabilities"]

    result = run_card(db, "unresolved_age_severity")

    assert result["as_of"] == _utc(loaded_at)


@pytest.mark.parametrize("card", list(CARDS))
def test_every_card_runs_when_no_exceptions_were_exported(tmp_path, card):
    database = _load(tmp_path, with_exceptions=False)
    try:
        result = run_card(database, card, as_of=AS_OF)
    finally:
        database.close()

    assert any(result["results"].values()), f"{card} returned no rows at all"


@pytest.mark.parametrize(
    ("days", "band"),
    [
        (0, "0-7"),
        (7, "0-7"),
        (8, "8-30"),
        (30, "8-30"),
        (31, "31-60"),
        (60, "31-60"),
        (61, "61-90"),
        (90, "61-90"),
        (91, "91+"),
    ],
)
def test_age_band_edges(tmp_path, days, band):
    found = AS_OF - timedelta(days=days)
    rows = [("ORG-A", "A1", "V1", "High", False, found, None, "Local", 0.1)]
    database = _load(tmp_path, with_exceptions=False, rows=rows)
    try:
        bands = run_card(database, "unresolved_age_severity", as_of=AS_OF)["results"]["by_band"]
    finally:
        database.close()

    assert bands == [{"band": band, "critical": 0, "high": 1, "medium": 0, "low": 0}]


def test_a_finding_with_no_date_is_counted_in_91_plus_as_the_dashboard_does(tmp_path):
    rows = [("ORG-A", "A1", "V1", "High", False, None, None, "Local", 0.1)]
    database = _load(tmp_path, with_exceptions=False, rows=rows)
    try:
        bands = run_card(database, "unresolved_age_severity", as_of=AS_OF)["results"]["by_band"]
    finally:
        database.close()

    assert [row["band"] for row in bands] == ["91+"]


def test_missing_severity_counts_as_unscored(tmp_path):
    rows = [("ORG-A", "A1", "V1", None, False, AS_OF, None, "Local", 0.1)]
    database = _load(tmp_path, with_exceptions=False, rows=rows)
    try:
        org = run_card(database, "portfolio_summary", as_of=AS_OF)["results"]["by_org"][0]
    finally:
        database.close()

    assert (org["active_total"], org["unscored"]) == (1, 1)


def test_without_exceptions_the_formerly_excepted_finding_is_active(tmp_path):
    database = _load(tmp_path, with_exceptions=False)
    try:
        result = run_card(database, "portfolio_summary", as_of=AS_OF)
    finally:
        database.close()

    assert _by_org(result["results"]["by_org"])["ORG-A"]["medium"] == 1


def test_unknown_card_is_rejected_with_the_valid_names(db):
    with pytest.raises(ValueError, match="portfolio_summary"):
        run_card(db, "no_such_card", as_of=AS_OF)


def test_no_loaded_data_is_refused(tmp_path):
    database = VulnerabilityDatabase(str(Path(tmp_path) / "empty.db"))
    try:
        with pytest.raises(ValueError, match="No vulnerability data"):
            run_card(database, "portfolio_summary", as_of=AS_OF)
    finally:
        database.close()


@pytest.mark.parametrize(
    ("card", "phrases"),
    [
        ("portfolio_summary", ["with a known exploit (hasExploits) stand in for findings in CISA KEV"]),
        ("org_risk_distribution", ["no 0 to 1000 organisation risk score"]),
        (
            "new_vulnerabilities",
            ["the later of firstFoundTimestamp and reintroducedTimestamp", "Weeks start on Monday"],
        ),
        ("remediated_vulnerabilities", ["Remediated counts are not available per organisation"]),
        ("unresolved_age_severity", ["from firstFoundTimestamp, the discovery date"]),
        ("org_comparison", ["CISA KEV or exploited in the wild", "reintroduced ones", "not a clean record"]),
    ],
)
def test_every_substitute_and_scope_is_stated(card, phrases):
    """Each phrase pins a substitute or a scope that the card's UX title alone would not tell a reader."""
    text = CARDS[card].description + " " + CARDS[card].definition
    for phrase in phrases:
        assert phrase in text, phrase


def test_org_comparison_sorts_by_risk_score_not_by_org(tmp_path):
    database = _load(tmp_path, assets=[("ORG-A", "A1", 10.0), ("ORG-A", "A2", 20.0), ("ORG-B", "B1", 900.0)])
    try:
        rows = run_card(database, "org_comparison", as_of=AS_OF)["results"]["by_org"]
    finally:
        database.close()

    assert [row["orgId"] for row in rows] == ["ORG-B", "ORG-A"]


def test_an_org_with_findings_but_no_assets_has_no_averages(tmp_path):
    database = _load(tmp_path, assets=[("ORG-A", "A1", 100.0), ("ORG-A", "A2", 300.0)])
    try:
        org_b = _by_org(run_card(database, "org_comparison", as_of=AS_OF)["results"]["by_org"])["ORG-B"]
    finally:
        database.close()

    assert org_b["total_assessed_assets"] == 0
    assert org_b["total_critical_vulnerabilities"] == 1
    assert org_b["average_vulnerabilities_per_asset"] is None
    assert org_b["average_known_exploits_per_asset"] is None


def test_findings_with_no_org_stay_on_one_comparison_row(tmp_path):
    rows = [(None, "N1", "V1", "Critical", True, datetime(2026, 9, 1), None, "Network", 0.9)]
    database = _load(tmp_path, with_exceptions=False, rows=rows, assets=[(None, "N1", 15.0)])
    try:
        compared = run_card(database, "org_comparison", as_of=AS_OF)["results"]["by_org"]
    finally:
        database.close()

    assert compared == [
        {
            "orgId": None,
            "risk_score": 15.0,
            "total_assessed_assets": 1,
            "total_critical_vulnerabilities": 1,
            "average_vulnerabilities_per_asset": 1.0,
            "average_known_exploits_per_asset": 1.0,
            "reintroduced_critical_high": 0,
        }
    ]
