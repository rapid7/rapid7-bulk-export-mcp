"""Per-org load times: recorded by the loader, and used to age each org's findings."""

from datetime import datetime, timedelta, timezone
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


def _utc(when):
    return when.replace(tzinfo=timezone.utc).isoformat()


class _Clock:
    """Stands in for datetime in the loader so each load stamps a chosen time."""

    current = MONDAY

    @classmethod
    def now(cls, tz=None):
        return cls.current.replace(tzinfo=tz)


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
    assert result["as_of_by_org"] == {"ORG-A": _utc(MONDAY), "ORG-B": _utc(FRIDAY)}
    assert result["as_of"] == _utc(MONDAY)


def test_an_org_without_its_own_time_falls_back_to_the_table_time(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY)
    with duckdb.connect(db.db_path) as conn:
        conn.execute(f"DELETE FROM {ORG_LOAD_METADATA_TABLE}")  # nosec B608

    assert run_card(db, "unresolved_age_severity")["as_of_by_org"] == {"ORG-A": _utc(MONDAY)}


def test_an_org_with_no_time_at_all_is_refused(db, tmp_path, clock):
    _load_org(db, tmp_path, "ORG-A", MONDAY)
    with duckdb.connect(db.db_path) as conn:
        conn.execute(f"DELETE FROM {ORG_LOAD_METADATA_TABLE}")  # nosec B608
        conn.execute("DELETE FROM _load_metadata")

    with pytest.raises(ValueError, match="No recorded load time for org"):
        run_card(db, "unresolved_age_severity")


def test_findings_with_no_org_are_still_counted(db, tmp_path, clock):
    path = tmp_path / "null-orgs.parquet"
    pq.write_table(
        pa.table(
            {
                "orgId": pa.array(["ORG-A", None, None], pa.string()),
                "assetId": ["a1", "a2", "a3"],
                "vulnId": ["V1", "V2", "V3"],
                "cvssV3Severity": ["High"] * 3,
                "hasExploits": [False] * 3,
                "firstFoundTimestamp": pa.array([MONDAY] * 3, pa.timestamp("us")),
                "reintroducedTimestamp": pa.array([None] * 3, pa.timestamp("us")),
            }
        ),
        path,
    )
    db.load_parquet_files_by_prefix({"asset_vulnerability": [str(path)]})

    bands = run_card(db, "unresolved_age_severity")["results"]["by_band"]

    assert sum(row["high"] for row in bands) == 3


def test_a_non_text_org_id_still_finds_its_own_time(db, tmp_path, clock):
    def _int_org_file(org_id, first_found):
        path = tmp_path / f"int-{org_id}.parquet"
        pq.write_table(
            pa.table(
                {
                    "orgId": pa.array([org_id], pa.int64()),
                    "assetId": [f"asset-{org_id}"],
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

    db.load_parquet_files_by_prefix({"asset_vulnerability": [_int_org_file(1, MONDAY)]}, org_scoped=True)
    clock.current = FRIDAY
    db.load_parquet_files_by_prefix({"asset_vulnerability": [_int_org_file(2, FRIDAY)]}, org_scoped=True)

    assert run_card(db, "unresolved_age_severity")["as_of_by_org"] == {1: _utc(MONDAY), 2: _utc(FRIDAY)}


def test_a_failed_time_write_rolls_back_the_org_refresh(db, tmp_path, clock, monkeypatch):
    _load_org(db, tmp_path, "ORG-A", MONDAY - timedelta(days=5))
    clock.current = FRIDAY

    def _fail(*args, **kwargs):
        raise RuntimeError("metadata write failed")

    monkeypatch.setattr(VulnerabilityDatabase, "_record_org_load_metadata", _fail)
    with pytest.raises(RuntimeError, match="metadata write failed"):
        _load_org(db, tmp_path, "ORG-A", FRIDAY - timedelta(days=1))
    monkeypatch.undo()

    first_found = db.query('SELECT "firstFoundTimestamp" AS f FROM vulnerabilities')
    assert first_found == [{"f": MONDAY - timedelta(days=5)}]
    assert db.get_org_load_metadata("vulnerabilities") == {"ORG-A": MONDAY}
    assert db.get_load_metadata()["vulnerabilities"] == MONDAY


def test_weaponised_days_open_use_each_orgs_own_time(db, tmp_path, clock):
    def _weaponised(org_id, first_found):
        path = tmp_path / f"weaponised-{org_id}.parquet"
        pq.write_table(
            pa.table(
                {
                    "orgId": [org_id],
                    "assetId": [f"{org_id}-asset"],
                    "vulnId": ["V1"],
                    "cvssV3Severity": ["Critical"],
                    "hasExploits": [True],
                    "cvssV3AttackVector": ["Network"],
                    "epssscore": [0.9],
                    "cvssV3Score": [9.8],
                    "title": ["Vuln V1"],
                    "bestSolutionSummary": ["Patch V1"],
                    "bestSolutionType": ["patch"],
                    "firstFoundTimestamp": pa.array([first_found], pa.timestamp("us")),
                    "reintroducedTimestamp": pa.array([None], pa.timestamp("us")),
                }
            ),
            path,
        )
        return str(path)

    def _asset(org_id):
        path = tmp_path / f"asset-{org_id}.parquet"
        pq.write_table(
            pa.table(
                {
                    "orgId": [org_id],
                    "assetId": [f"{org_id}-asset"],
                    "riskScore": [500.0],
                    "hostName": [f"host-{org_id}"],
                    "osProduct": ["Linux"],
                }
            ),
            path,
        )
        return str(path)

    db.load_parquet_files_by_prefix(
        {"asset_vulnerability": [_weaponised("ORG-A", MONDAY - timedelta(days=10))], "asset": [_asset("ORG-A")]},
        org_scoped=True,
    )
    clock.current = FRIDAY
    db.load_parquet_files_by_prefix(
        {"asset_vulnerability": [_weaponised("ORG-B", FRIDAY - timedelta(days=10))], "asset": [_asset("ORG-B")]},
        org_scoped=True,
    )

    headline = run_card(db, "weaponised_critical")["results"]["headline"][0]

    assert (headline["mean_days_open"], headline["median_days_open"]) == (10, 10)
