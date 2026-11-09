"""Load times are UTC clock times, so the operator's time zone never changes a number."""

import json
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import duckdb
import pytest

from src import mcp_server
from src.dashboard_queries import run_card
from src.duckdb_loader import VulnerabilityDatabase, utc_now
from tests.test_dashboard_queries import _load


@pytest.fixture
def tokyo(monkeypatch):
    """Run as an operator in Japan, nine hours ahead of UTC with no summer time."""
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def test_utc_now_ignores_the_machines_time_zone(tokyo):
    assert datetime.now() - utc_now() > timedelta(hours=8)
    assert abs(utc_now() - datetime.now(timezone.utc).replace(tzinfo=None)) < timedelta(seconds=5)


def test_loads_are_stamped_in_utc_on_a_machine_in_another_zone(tokyo, tmp_path):
    db = _load(tmp_path)
    try:
        loaded_at = db.get_load_metadata()["vulnerabilities"]
        per_org = db.get_org_load_metadata("vulnerabilities")
    finally:
        db.close()

    true_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    assert abs(loaded_at - true_utc) < timedelta(minutes=1)
    assert set(per_org.values()) == {loaded_at}


def test_the_data_age_is_right_on_a_machine_in_another_zone(tokyo, tmp_path, monkeypatch):
    """Comparing a UTC load time with local now would report the data as nine hours old."""
    db = _load(tmp_path)
    monkeypatch.setattr(mcp_server, "db", db)
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", False)
    monkeypatch.setattr(mcp_server, "storage_configured", lambda: True)
    try:
        card = json.loads(mcp_server.get_rapid7_dashboard_card("portfolio_summary"))
        note = mcp_server._freshness_note()
    finally:
        db.close()

    assert card["data_loaded"] in ("less than a minute ago", "1 minute ago")
    assert "less than a minute ago" in note or "1 minute ago" in note


def test_a_zoned_as_of_is_counted_in_utc(tmp_path):
    db = _load(tmp_path)
    try:
        tokyo_morning = datetime(2026, 9, 23, 9, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        result = run_card(db, "unresolved_age_severity", as_of=tokyo_morning)
    finally:
        db.close()

    assert result["as_of"] == "2026-09-23T00:00:00+00:00"


def test_utc_now_is_naive_so_duckdb_never_converts_it():
    """DuckDB converts an aware datetime through its own session zone, which ignores TZ."""
    assert utc_now().tzinfo is None


def test_a_database_from_before_utc_stamping_reads_its_local_stamps_as_utc(tokyo, tmp_path):
    db = VulnerabilityDatabase(str(tmp_path / "legacy.db"))
    db.close()
    tokyo_evening = datetime(2026, 9, 21, 18, 0)
    with duckdb.connect(db.db_path) as conn:
        conn.execute("CREATE TABLE _load_metadata (table_name VARCHAR PRIMARY KEY, loaded_at TIMESTAMP NOT NULL)")
        conn.execute("INSERT INTO _load_metadata VALUES ('vulnerabilities', ?)", [tokyo_evening])
        conn.execute(
            "CREATE TABLE _org_load_metadata (table_name VARCHAR NOT NULL, org_id VARCHAR NOT NULL, "
            "loaded_at TIMESTAMP NOT NULL, PRIMARY KEY (table_name, org_id))"
        )
        conn.execute("INSERT INTO _org_load_metadata VALUES ('vulnerabilities', 'ORG-A', ?)", [tokyo_evening])

    reopened = VulnerabilityDatabase(db.db_path)
    try:
        assert reopened.get_load_metadata()["vulnerabilities"] == datetime(2026, 9, 21, 9, 0)
        assert reopened.get_org_load_metadata("vulnerabilities") == {"ORG-A": datetime(2026, 9, 21, 9, 0)}
    finally:
        reopened.close()
