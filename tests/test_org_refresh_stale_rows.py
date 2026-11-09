"""An org-scoped refresh must replace every row of the refreshed org, even when a table comes back empty.

Each refresh here loads a full vulnerability export for one org: findings, assets, and
exceptions, any of which may be empty or missing, as a real export can be.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.duckdb_loader import VulnerabilityDatabase


def _parquet(directory, name, columns):
    path = Path(directory) / f"{name}.parquet"
    pq.write_table(pa.table(columns), path)
    return str(path)


def _export(directory, org_id, findings, assets, exceptions, tag):
    """``None`` for a dataset leaves its file out entirely."""
    files = {}
    if findings is not None:
        files["asset_vulnerability"] = [
            _parquet(
                directory,
                f"{tag}-vulns",
                {
                    "orgId": pa.array([org_id] * len(findings), pa.string()),
                    "assetId": pa.array([a for a, _ in findings], pa.string()),
                    "vulnId": pa.array([v for _, v in findings], pa.string()),
                },
            )
        ]
    if assets is not None:
        files["asset"] = [
            _parquet(
                directory,
                f"{tag}-assets",
                {"orgId": pa.array([org_id] * len(assets), pa.string()), "assetId": pa.array(assets, pa.string())},
            )
        ]
    if exceptions is not None:
        files["vulnerability_exception"] = [
            _parquet(
                directory,
                f"{tag}-exceptions",
                {
                    "orgId": pa.array([org_id] * len(exceptions), pa.string()),
                    "assetId": pa.array([a for a, _ in exceptions], pa.string()),
                    "vulnId": pa.array([v for _, v in exceptions], pa.string()),
                },
            )
        ]
    return files


@pytest.fixture
def db(tmp_path):
    database = VulnerabilityDatabase(str(tmp_path / "refresh.db"))
    yield database
    database.close()


def _refresh(db, tmp_path, org_id, label, findings, assets, exceptions, tag):
    files = _export(tmp_path, org_id, findings, assets, exceptions, tag)
    db.load_parquet_files_by_prefix(files, org_scoped=True, org_label=label)


def _count(db, table, org_id):
    return db.query(f'SELECT COUNT(*) AS c FROM {table} WHERE "orgId" = $o', {"o": org_id})[0]["c"]  # nosec B608


@pytest.fixture
def two_orgs(db, tmp_path):
    _refresh(db, tmp_path, "ORG-A", "alpha", [("A1", "V1")], ["A1"], [("A1", "V1")], "a-day1")
    _refresh(db, tmp_path, "ORG-B", "beta", [("B1", "V9")], ["B1"], [], "b-day1")
    return db


@pytest.mark.parametrize("exceptions", [[], None], ids=["empty exception file", "no exception file"])
def test_a_lifted_exception_does_not_survive_the_refresh(two_orgs, tmp_path, exceptions):
    _refresh(two_orgs, tmp_path, "ORG-A", "alpha", [("A1", "V1")], ["A1"], exceptions, "a-day2")

    assert _count(two_orgs, "vulnerability_exceptions", "ORG-A") == 0


@pytest.mark.parametrize("findings", [[], None], ids=["empty findings file", "no findings file"])
def test_findings_that_were_all_fixed_do_not_survive_the_refresh(two_orgs, tmp_path, findings):
    _refresh(two_orgs, tmp_path, "ORG-A", "alpha", findings, ["A1"], [], "a-day2")

    assert _count(two_orgs, "vulnerabilities", "ORG-A") == 0
    assert _count(two_orgs, "assets", "ORG-A") == 1


@pytest.mark.parametrize("label", ["alpha", None])
def test_an_export_with_no_rows_at_all_deletes_nothing(two_orgs, tmp_path, label):
    """Its files name no org, and guessing could delete another org's data if the key moved."""
    _refresh(two_orgs, tmp_path, "ORG-A", label, [], [], [], "a-day2")

    for table in ("vulnerabilities", "assets", "vulnerability_exceptions"):
        assert _count(two_orgs, table, "ORG-A") == 1, table
    assert _count(two_orgs, "vulnerabilities", "ORG-B") == 1


def test_refreshing_one_org_never_touches_another(two_orgs, tmp_path):
    _refresh(two_orgs, tmp_path, "ORG-A", "alpha", [], ["A1"], [], "a-day2")

    assert _count(two_orgs, "vulnerabilities", "ORG-B") == 1
    assert _count(two_orgs, "assets", "ORG-B") == 1


def test_a_label_that_now_reaches_another_org_does_not_delete_the_old_org(two_orgs, tmp_path):
    """The files win: if alpha's key now returns ORG-C, ORG-A's data stays and ORG-C is loaded."""
    _refresh(two_orgs, tmp_path, "ORG-C", "alpha", [("C1", "V5")], ["C1"], [], "c-day2")

    assert _count(two_orgs, "vulnerabilities", "ORG-A") == 1
    assert _count(two_orgs, "vulnerabilities", "ORG-C") == 1


def test_a_partial_call_treats_the_missing_tables_as_empty(two_orgs, tmp_path):
    """Callers must pass the full export: a table with no file is cleared for the org."""
    files = _export(tmp_path, "ORG-A", [("A1", "V1")], None, None, "a-partial")
    two_orgs.load_parquet_files_by_prefix(files, org_scoped=True, org_label="alpha")

    assert _count(two_orgs, "vulnerabilities", "ORG-A") == 1
    assert _count(two_orgs, "assets", "ORG-A") == 0


def test_both_policy_sources_are_replaced_without_duplicating(db, tmp_path):
    def _policies(name, assets):
        return _parquet(
            tmp_path,
            name,
            {"orgId": pa.array(["ORG-A"] * len(assets), pa.string()), "assetId": pa.array(assets, pa.string())},
        )

    for day in ("day1", "day2"):
        db.load_parquet_files_by_prefix(
            {
                "asset_policy": [_policies(f"agent-{day}", ["A1"])],
                "asset_scan_policy": [_policies(f"scan-{day}", ["A1", "A2"])],
            },
            org_scoped=True,
            org_label="alpha",
        )

    rows = db.query("SELECT source, COUNT(*) AS c FROM policies GROUP BY source ORDER BY source")
    assert rows == [{"source": "agent", "c": 1}, {"source": "scan", "c": 2}]
