"""Multi-org loads store each org's configured label, so the report names orgs, not IDs."""

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.dashboard_queries import run_card
from src.dashboard_report import render_report
from src.duckdb_loader import VulnerabilityDatabase
from tests.test_org_load_metadata import _org_file

FOUND = datetime(2026, 9, 1)


@pytest.fixture
def db(tmp_path):
    database = VulnerabilityDatabase(str(tmp_path / "orgs.db"))
    yield database
    database.close()


def _assets(directory, org_id):
    path = Path(directory) / f"{org_id}-assets.parquet"
    pq.write_table(pa.table({"orgId": [org_id], "assetId": [f"{org_id}-asset"], "riskScore": [100.0]}), path)
    return str(path)


def _load(db, tmp_path, org_id, label=None, org_scoped=True):
    db.load_parquet_files_by_prefix(
        {"asset_vulnerability": [_org_file(tmp_path, org_id, FOUND)], "asset": [_assets(tmp_path, org_id)]},
        org_scoped=org_scoped,
        org_label=label,
    )


def test_a_multi_org_load_stores_each_orgs_label(db, tmp_path):
    _load(db, tmp_path, "ORG-A", "fogcutter")
    _load(db, tmp_path, "ORG-B", "eu")

    assert db.get_org_labels() == {"ORG-A": "fogcutter", "ORG-B": "eu"}


def test_a_label_names_one_org_at_a_time(db, tmp_path):
    """If a label's key now reaches another org, the old pairing must not keep the name."""
    _load(db, tmp_path, "ORG-A", "fogcutter")
    _load(db, tmp_path, "ORG-NEW", "fogcutter")

    assert db.get_org_labels() == {"ORG-NEW": "fogcutter"}


def test_a_single_org_load_stores_no_label(db, tmp_path):
    db.load_parquet_files_by_prefix({"asset_vulnerability": [_org_file(tmp_path, "ORG-A", FOUND)]})

    assert db.get_org_labels() == {}


def test_a_card_carries_the_labels_of_its_orgs_only(db, tmp_path):
    _load(db, tmp_path, "ORG-A", "fogcutter")
    _load(db, tmp_path, "ORG-B")

    assert run_card(db, "portfolio_summary")["org_labels"] == {"ORG-A": "fogcutter"}


def test_the_report_names_labelled_orgs_and_keeps_the_id_otherwise(db, tmp_path):
    _load(db, tmp_path, "ORG-A", "fogcutter")
    _load(db, tmp_path, "ORG-B")
    cards = [run_card(db, name) for name in ("portfolio_summary", "new_vulnerabilities", "org_comparison")]

    html = render_report(cards, "2026-11-17T17:00:00+00:00", coverage_complete=True)

    assert "ORG-A" not in html, "a labelled org never shows its ID"
    assert "fogcutter</td>" in html and 'data-series="fogcutter"' in html and "</i>fogcutter</span>" in html
    assert "ORG-B</td>" in html, "an org with no label keeps its ID"


def test_a_hostile_label_is_escaped(db, tmp_path):
    _load(db, tmp_path, "ORG-A", '<b x="1">')
    cards = [run_card(db, "org_comparison")]

    html = render_report(cards, "2026-11-17T17:00:00+00:00", coverage_complete=True)

    assert "<b x=" not in html
    assert "&lt;b x=&quot;1&quot;&gt;</td>" in html


def test_an_empty_export_keeps_the_label_with_the_orgs_rows(db, tmp_path):
    _load(db, tmp_path, "ORG-A", "fogcutter")
    empty = Path(tmp_path) / "empty.parquet"
    pq.write_table(pq.read_table(_org_file(tmp_path, "ORG-A", FOUND)).slice(0, 0), empty)

    db.load_parquet_files_by_prefix({"asset_vulnerability": [str(empty)]}, org_scoped=True, org_label="fogcutter")

    assert db.get_org_labels() == {"ORG-A": "fogcutter"}


def test_a_failed_load_does_not_change_the_label(db, tmp_path, monkeypatch):
    _load(db, tmp_path, "ORG-A", "first")

    def _fail(*args, **kwargs):
        raise RuntimeError("metadata write failed")

    monkeypatch.setattr(VulnerabilityDatabase, "_record_org_load_metadata", _fail)
    with pytest.raises(RuntimeError):
        _load(db, tmp_path, "ORG-A", "second")

    assert db.get_org_labels() == {"ORG-A": "first"}


def test_a_label_that_matches_another_orgs_id_does_not_merge_the_two(db, tmp_path):
    _load(db, tmp_path, "ORG-A", "ORG-B")
    _load(db, tmp_path, "ORG-B")
    cards = [run_card(db, name) for name in ("portfolio_summary", "org_comparison")]

    html = render_report(cards, "2026-11-17T17:00:00+00:00", coverage_complete=True)

    assert "2 organisation(s)" in html
    assert "ORG-B (ORG-A)</td>" in html and "ORG-B (ORG-B)</td>" in html


def test_an_org_with_assets_but_no_findings_shows_its_label(db, tmp_path):
    _load(db, tmp_path, "ORG-A", "fogcutter")
    db.load_parquet_files_by_prefix({"asset": [_assets(tmp_path, "ORG-C")]}, org_scoped=True, org_label="charlie")

    html = render_report([run_card(db, "org_comparison")], "2026-11-17T17:00:00+00:00", coverage_complete=True)

    assert "charlie</td>" in html and "ORG-C" not in html


def test_colours_follow_the_org_not_its_label(db, tmp_path):
    _load(db, tmp_path, "ORG-A", "zulu")
    _load(db, tmp_path, "ORG-B", "alpha")

    html = render_report([run_card(db, "org_comparison")], "2026-11-17T17:00:00+00:00", coverage_complete=True)

    assert '<span class="swatch" style="background:#77b8ec"></span>zulu' in html, (
        "ORG-A sorts first, so it keeps the first colour"
    )


def test_a_hostile_label_is_escaped_on_every_chart_path(db, tmp_path):
    _load(db, tmp_path, "ORG-A", '<b x="1">')
    cards = [run_card(db, name) for name in ("org_risk_distribution", "new_vulnerabilities")]

    html = render_report(cards, "2026-11-17T17:00:00+00:00", coverage_complete=True)
    shown = "&lt;b x=&quot;1&quot;&gt;"

    assert "<b x=" not in html
    assert f'data-series="{shown}"' in html and f"</i>{shown}</span>" in html and f"<title>{shown}" in html
