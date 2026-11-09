"""Per-org load times: recorded by the loader, and used to age each org's findings."""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src import duckdb_loader
from src.dashboard_queries import run_card
from src.duckdb_loader import ORG_LOAD_METADATA_TABLE, VulnerabilityDatabase

MONDAY = datetime(2026, 9, 21, 9, 0)
FRIDAY = datetime(2026, 9, 25, 9, 0)


class _Clock:
    """Stands in for datetime in the loader so each load stamps a chosen time."""

    current = MONDAY

    @classmethod
    def now(cls):
        return cls.current


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr(duckdb_loader, "datetime", _Clock)
    _Clock.current = MONDAY
    return _Clock


def _org_file(directory, org_id, first_found):
    """One High finding for one org, first found at ``first_found``."""
    path = Path(directory) / f"{org_id}-{first_found:%Y%m%d}.parquet"
    pq.write_table(
        pa.table(
            {
                "orgId": [org_id],
                "assetId": [f"{org_id}-asset"],
                "vulnId": ["V1"],
                "cvssV3Severity": ["High"],
                "hasExploits": [False],
                "firstFoundTimestamp": pa.array([first_found], pa.timestamp("us")),
                "reintroducedTimestamp": pa.array([None], pa.timestamp("us")),
            }
        ),
        path,
    )
    return str(path)


@pytest.fixture
def db(tmp_path):
    database = VulnerabilityDatabase(str(tmp_path / "orgs.db"))
    yield database
    database.close()


def _load_org(db, tmp_path, org_id, first_found):
    db.load_parquet_files_by_prefix(
        {"asset_vulnerability": [_org_file(tmp_path, org_id, first_found)]}, org_scoped=True
    )


def test_each_org_keeps_its_own_load_time(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY - timedelta(days=5))
    clock.current = FRIDAY
    _load_org(db, tmp_path, "ORG-B", FRIDAY - timedelta(days=5))

    assert db.get_org_load_metadata("vulnerabilities") == {"ORG-A": MONDAY, "ORG-B": FRIDAY}
    assert db.get_load_metadata()["vulnerabilities"] == FRIDAY


def test_refreshing_one_org_leaves_the_others_time_alone(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY)
    _load_org(db, tmp_path, "ORG-B", MONDAY)
    clock.current = FRIDAY
    _load_org(db, tmp_path, "ORG-B", FRIDAY)

    assert db.get_org_load_metadata("vulnerabilities") == {"ORG-A": MONDAY, "ORG-B": FRIDAY}


def test_a_snapshot_load_drops_orgs_that_are_no_longer_present(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY)
    _load_org(db, tmp_path, "ORG-B", MONDAY)
    clock.current = FRIDAY
    db.load_parquet_files_by_prefix({"asset_vulnerability": [_org_file(tmp_path, "ORG-B", FRIDAY)]})

    assert db.get_org_load_metadata("vulnerabilities") == {"ORG-B": FRIDAY}


def test_no_per_org_times_before_anything_is_loaded(db):
    assert db.get_org_load_metadata("vulnerabilities") == {}


def test_metadata_table_is_not_reported_as_rapid7_data(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY)

    assert ORG_LOAD_METADATA_TABLE not in db.get_schema()


def test_each_orgs_findings_are_aged_to_its_own_load_time(db, tmp_path, clock):
    """Both findings are 5 days old when their org loaded. Ageing both to Friday would put A's at 9."""
    _load_org(db, tmp_path, "ORG-A", MONDAY - timedelta(days=5))
    clock.current = FRIDAY
    _load_org(db, tmp_path, "ORG-B", FRIDAY - timedelta(days=5))

    result = run_card(db, "unresolved_age_severity")

    assert result["results"]["by_band"] == [{"band": "0-7", "critical": 0, "high": 2, "medium": 0, "low": 0}]
    assert result["as_of_by_org"] == {"ORG-A": MONDAY.isoformat(), "ORG-B": FRIDAY.isoformat()}
    assert result["as_of"] == MONDAY.isoformat()


def test_an_org_without_its_own_time_falls_back_to_the_table_time(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY)
    with duckdb.connect(db.db_path) as conn:
        conn.execute(f"DELETE FROM {ORG_LOAD_METADATA_TABLE}")  # nosec B608

    assert run_card(db, "unresolved_age_severity")["as_of_by_org"] == {"ORG-A": MONDAY.isoformat()}


def test_an_org_with_no_time_at_all_is_refused(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY)
    with duckdb.connect(db.db_path) as conn:
        conn.execute(f"DELETE FROM {ORG_LOAD_METADATA_TABLE}")  # nosec B608
        conn.execute("DELETE FROM _load_metadata")

    with pytest.raises(ValueError, match="No recorded load time for org"):
        run_card(db, "unresolved_age_severity")
