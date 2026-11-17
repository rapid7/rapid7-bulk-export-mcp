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
