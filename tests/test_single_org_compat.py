"""The original single-org flow, with no RAPID7_ORGS_FILE, must behave as it did before multi-org.

Every multi-org change to shared code (loader, orchestrator, config, existing tools) has to
leave this path unchanged. These tests pin it.
"""

import pytest

from src import mcp_server
from src.dashboard_queries import CARDS, run_card
from src.db_utils import duckdb_connection
from src.duckdb_loader import ORG_LOAD_METADATA_TABLE, VulnerabilityDatabase
from tests.test_dashboard_queries import VULNS, _load

SINGLE_ORG = [v for v in VULNS if v[0] == "ORG-A"]


@pytest.fixture
def no_org_file(monkeypatch):
    monkeypatch.delenv("RAPID7_ORGS_FILE", raising=False)


@pytest.fixture
def served(tmp_path, monkeypatch, no_org_file):
    database = _load(tmp_path, rows=SINGLE_ORG)
    monkeypatch.setattr(mcp_server, "db", database)
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", False)
    yield database
    database.close()


def _fail(*args, **kwargs):
    raise RuntimeError("per-org metadata write failed")


def test_a_snapshot_load_never_fails_because_of_per_org_load_times(tmp_path, monkeypatch, no_org_file):
    monkeypatch.setattr(VulnerabilityDatabase, "_record_org_load_metadata", _fail)

    database = _load(tmp_path, rows=SINGLE_ORG)
    try:
        assert database.query("SELECT COUNT(*) AS n FROM vulnerabilities")[0]["n"] == len(SINGLE_ORG)
        assert "vulnerabilities" in database.get_load_metadata()
        assert database.get_org_load_metadata("vulnerabilities") == {}
        for card in CARDS:
            run_card(database, card)
    finally:
        database.close()


def test_a_failed_per_org_write_clears_stale_times_so_cards_use_the_table_time(tmp_path, monkeypatch, no_org_file):
    database = _load(tmp_path, rows=SINGLE_ORG)
    database.close()
    assert VulnerabilityDatabase(str(tmp_path / "dashboard.db")).get_org_load_metadata("vulnerabilities")

    monkeypatch.setattr(VulnerabilityDatabase, "_record_org_load_metadata", _fail)
    database = _load(tmp_path, rows=SINGLE_ORG)
    try:
        assert database.get_org_load_metadata("vulnerabilities") == {}
        table_time = database.get_load_metadata()["vulnerabilities"]
        result = run_card(database, "unresolved_age_severity")
        assert result["as_of"].startswith(table_time.strftime("%Y-%m-%dT%H:%M"))
    finally:
        database.close()


def test_a_database_loaded_before_per_org_times_still_builds_every_card(served):
    with duckdb_connection(served.db_path) as conn:
        conn.execute(f"DROP TABLE {ORG_LOAD_METADATA_TABLE}")

    for card in CARDS:
        assert run_card(served, card)["orgs"] == ["ORG-A"]


def test_coverage_without_an_org_list_reports_the_loaded_org_and_no_warning(served):
    result = mcp_server.get_rapid7_org_coverage()

    assert "Organizations present in the loaded vulnerability data: 1" in result
    assert "ORG-A" in result
    assert "Configured organizations" not in result
    assert "⚠️" not in result


@pytest.mark.parametrize(("hosted", "noted"), [(False, False), (True, True)])
def test_the_data_age_note_appears_only_in_hosted_mode(served, monkeypatch, hosted, noted):
    monkeypatch.setattr(mcp_server, "storage_configured", lambda: hosted)

    note = mcp_server._freshness_note()

    assert ("Data last loaded" in note) is noted


def test_existing_read_tools_work_without_an_org_list(served):
    assert "ORG-A" in mcp_server.query_rapid7('SELECT DISTINCT "orgId" FROM vulnerabilities')
    assert "vulnerabilities" in mcp_server.get_rapid7_stats()
    assert "vulnerabilities" in mcp_server.get_rapid7_schema()
