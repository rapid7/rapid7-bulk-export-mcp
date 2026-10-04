"""
Tests for the DuckDB loader module.
"""

import tempfile
import threading
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.duckdb_loader import DEFAULT_QUERY_TIMEOUT_SECONDS, VulnerabilityDatabase, _resolve_query_timeout

# A query with no table dependency that runs long enough to blow a sub-second
# timeout: a self-join over range() forces a large intermediate the engine must
# grind through, and range() needs no external access so it works under lockdown.
_SLOW_QUERY = "SELECT COUNT(*) FROM range(1000000) t1, range(1000) t2 WHERE t1.range % 7 = t2.range % 7"


@pytest.fixture
def sample_remediation_parquet_file():
    """Create a sample remediation Parquet file for testing."""
    table = pa.table(
        {
            "assetId": ["ASSET-A", "ASSET-B"],
            "cveId": ["CVE-2024-0001", "CVE-2024-0002"],
            "cvssV3Severity": ["Critical", "High"],
            "title": ["Remote Code Execution", "Privilege Escalation"],
        }
    )
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        yield f.name
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def sample_parquet_file():
    """Create a sample Parquet file for testing."""
    # Create sample data using PyArrow
    table = pa.table(
        {
            "vulnId": ["VULN-001", "VULN-002", "VULN-003"],
            "assetId": ["ASSET-A", "ASSET-B", "ASSET-A"],
            "severity": ["Critical", "Moderate", "Severe"],
            "cvssV3Score": [9.8, 5.5, 7.2],
            "title": ["SQL Injection", "XSS", "Buffer Overflow"],
        }
    )

    # Write to temporary file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        yield f.name

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def sample_asset_parquet_file():
    """Create a sample asset Parquet file for testing."""
    table = pa.table(
        {
            "assetId": ["ASSET-A", "ASSET-B"],
            "hostName": ["host-a.example.com", "host-b.example.com"],
            "ip": ["10.0.0.1", "10.0.0.2"],
            "osFamily": ["Linux", "Windows"],
        }
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        yield f.name

    Path(f.name).unlink(missing_ok=True)


def test_database_initialization():
    """Test that database can be initialized."""
    db = VulnerabilityDatabase()
    assert db.db_path is not None
    db.close()


def test_load_parquet_files_by_prefix(sample_parquet_file):
    """Test loading Parquet files into database via prefix routing."""
    db = VulnerabilityDatabase()

    prefix_map = {"asset_vulnerability": [sample_parquet_file]}
    row_counts = db.load_parquet_files_by_prefix(prefix_map)

    assert row_counts["vulnerabilities"] == 3
    db.close()


def test_query_execution(sample_parquet_file):
    """Test executing SQL queries."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    # Query all rows
    results = db.query("SELECT * FROM vulnerabilities")
    assert len(results) == 3

    # Query with filter
    results = db.query("SELECT * FROM vulnerabilities WHERE severity = 'Critical'")
    assert len(results) == 1
    assert results[0]["vulnId"] == "VULN-001"

    db.close()


def test_get_schema(sample_parquet_file):
    """Test retrieving table schema."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    schema = db.get_schema()

    # Should return a dict keyed by table name
    assert isinstance(schema, dict)
    assert "vulnerabilities" in schema
    assert len(schema["vulnerabilities"]) == 5
    column_names = [col["column_name"] for col in schema["vulnerabilities"]]
    assert "vulnId" in column_names
    assert "severity" in column_names

    db.close()


def test_get_stats(sample_parquet_file):
    """Test retrieving statistics."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    stats = db.get_stats()

    # Stats are now keyed by table name
    assert "vulnerabilities" in stats
    vuln_stats = stats["vulnerabilities"]
    assert vuln_stats["total_rows"] == 3
    assert "severity_distribution" in vuln_stats
    assert vuln_stats["severity_distribution"]["Critical"] == 1

    db.close()


def test_context_manager(sample_parquet_file):
    """Test using database as context manager."""
    with VulnerabilityDatabase() as db:
        db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})
        results = db.query("SELECT COUNT(*) as count FROM vulnerabilities")
        assert results[0]["count"] == 3


def test_persistent_database(sample_parquet_file):
    """Test creating a persistent database file."""
    # Create a temp directory and use a proper path
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create and populate database
        db = VulnerabilityDatabase(str(db_path))
        db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})
        db.close()

        # Reopen and verify data persists
        db = VulnerabilityDatabase(str(db_path))
        results = db.query("SELECT COUNT(*) as count FROM vulnerabilities")
        assert results[0]["count"] == 3
        db.close()


def test_query_with_aggregation(sample_parquet_file):
    """Test queries with aggregation."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    results = db.query("""
        SELECT severity, COUNT(*) as count
        FROM vulnerabilities
        GROUP BY severity
        ORDER BY count DESC
    """)

    assert len(results) == 3
    assert all("severity" in row and "count" in row for row in results)

    db.close()


def test_empty_prefix_map():
    """Test that empty prefix map returns empty counts."""
    db = VulnerabilityDatabase()

    row_counts = db.load_parquet_files_by_prefix({})
    assert row_counts == {}

    db.close()


def test_invalid_query(sample_parquet_file):
    """Test that invalid queries raise errors."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    with pytest.raises(ValueError, match="Query execution failed"):
        db.query("SELECT * FROM nonexistent_table")

    db.close()


def test_lockdown_blocks_external_access(sample_parquet_file):
    """Test that after loading, external filesystem access is blocked."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    # After loading, the connection should have external access disabled
    with pytest.raises(ValueError, match="Query execution failed"):
        db.query("SELECT * FROM read_csv('/etc/passwd')")

    db.close()


def test_lockdown_allows_normal_queries(sample_parquet_file):
    """Test that after lockdown, normal SELECT queries still work."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    # Normal queries should still work fine
    results = db.query("SELECT COUNT(*) as cnt FROM vulnerabilities")
    assert results[0]["cnt"] == 3

    db.close()


def test_reload_after_lockdown(sample_parquet_file):
    """Test that loading more data after lockdown works (reopens connection)."""
    db = VulnerabilityDatabase()
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    # Connection is now locked down — loading again should reopen it
    row_counts = db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})
    assert row_counts["vulnerabilities"] == 3

    # And queries should still work after re-lockdown
    results = db.query("SELECT COUNT(*) as cnt FROM vulnerabilities")
    assert results[0]["cnt"] == 3

    db.close()


def test_multiple_prefixes(sample_parquet_file, sample_asset_parquet_file):
    """Test loading multiple prefixes into different tables."""
    db = VulnerabilityDatabase()
    prefix_map = {
        "asset": [sample_asset_parquet_file],
        "asset_vulnerability": [sample_parquet_file],
    }
    row_counts = db.load_parquet_files_by_prefix(prefix_map)

    assert row_counts["assets"] == 2
    assert row_counts["vulnerabilities"] == 3

    db.close()


def test_purge(sample_parquet_file):
    """Test that purge removes all data and reinitializes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_purge.db"
        db = VulnerabilityDatabase(str(db_path))
        db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

        # Verify data exists
        results = db.query("SELECT COUNT(*) as cnt FROM vulnerabilities")
        assert results[0]["cnt"] == 3

        # Purge
        db.purge()

        # After purge, table should not exist
        with pytest.raises(ValueError, match="Query execution failed"):
            db.query("SELECT * FROM vulnerabilities")

        db.close()


def test_VulnerabilityDatabase_RepeatedSnapshotLoadsDoNotGrow(sample_parquet_file):
    """Repeated snapshot loads must not cause unbounded file growth.

    Each load drops and recreates the targeted tables. This asserts the file
    size after N reloads is no larger than after the first load plus a generous
    tolerance.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "growth_test.db")
        db = VulnerabilityDatabase(db_path)

        prefix_map = {"asset_vulnerability": [sample_parquet_file]}
        db.load_parquet_files_by_prefix(prefix_map)
        size_after_first = Path(db_path).stat().st_size

        for _ in range(4):
            db.load_parquet_files_by_prefix(prefix_map)

        size_after_five = Path(db_path).stat().st_size
        db.close()

        # Allow a 1.5x headroom for DuckDB metadata overhead from repeated
        # drop/recreate cycles, but the file must not grow proportionally
        # to the number of reloads.
        assert size_after_five <= size_after_first * 1.5, (
            f"DB file grew from {size_after_first} bytes to {size_after_five} bytes "
            f"over 5 identical snapshot loads — targeted table replacement not working"
        )


def test_VulnerabilityDatabase_SnapshotLoadPreservesRemediation(sample_parquet_file, sample_remediation_parquet_file):
    """Remediation data survives a snapshot reload of vulnerability data.

    A vulnerability snapshot only drops the 'vulnerabilities' table (and 'assets'
    if present). The 'vulnerability_remediation' table is untouched because it is
    not targeted by the incoming prefix map.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "remediation_rescue.db")
        db = VulnerabilityDatabase(db_path)

        # Load remediation data first
        db.load_parquet_files_by_prefix({"vulnerability_remediation": [sample_remediation_parquet_file]})
        remediation_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_remediation")[0]["cnt"]
        assert remediation_rows == 2

        # Now snapshot-load vulnerability data — should rescue and restore remediation
        db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

        # Vulnerability data must be present
        vuln_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")[0]["cnt"]
        assert vuln_rows == 3

        # Remediation data must still be present
        remediation_rows_after = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_remediation")[0]["cnt"]
        assert remediation_rows_after == 2, (
            f"Remediation rows lost after snapshot reload: expected 2, got {remediation_rows_after}"
        )

        db.close()


def test_VulnerabilityDatabase_RemediationNotDuplicatedOnReload(sample_parquet_file, sample_remediation_parquet_file):
    """Remediation rows are not duplicated when the same snapshot is reloaded twice."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "no_dupes.db")
        db = VulnerabilityDatabase(db_path)

        db.load_parquet_files_by_prefix({"vulnerability_remediation": [sample_remediation_parquet_file]})

        # Two consecutive snapshot reloads
        db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})
        db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

        remediation_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_remediation")[0]["cnt"]
        assert remediation_rows == 2, f"Remediation rows duplicated: expected 2, got {remediation_rows}"

        db.close()


@pytest.fixture
def sample_policy_parquet_file():
    """Create a sample policy Parquet file for testing."""
    table = pa.table(
        {
            "assetId": ["ASSET-A", "ASSET-B"],
            "benchmarkNaturalId": ["CIS-1", "CIS-2"],
            "ruleTitle": ["Ensure SSH access is restricted", "Ensure MFA is enabled"],
            "finalStatus": ["pass", "fail"],
        }
    )
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        yield f.name
    Path(f.name).unlink(missing_ok=True)


def test_VulnerabilityDatabase_PolicyLoadPreservesVulnData(sample_parquet_file, sample_policy_parquet_file):
    """Loading policy data must not wipe previously loaded vulnerability data.

    This is the primary bug scenario: loading a policy export after a
    vulnerability export would previously delete the entire DB, losing
    the vulnerability and asset tables.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "policy_preserves_vuln.db")
        db = VulnerabilityDatabase(db_path)

        # Step 1: Load vulnerability data (creates assets + vulnerabilities tables)
        db.load_parquet_files_by_prefix(
            {
                "asset_vulnerability": [sample_parquet_file],
            }
        )
        vuln_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")[0]["cnt"]
        assert vuln_rows == 3

        # Step 2: Load policy data (should only touch policies table)
        db.load_parquet_files_by_prefix(
            {"asset_policy": [sample_policy_parquet_file]},
            skip_prefixes={"asset"},
        )

        # Policies must be loaded
        policy_rows = db.query("SELECT COUNT(*) AS cnt FROM policies")[0]["cnt"]
        assert policy_rows == 2

        # Vulnerability data must still be intact
        vuln_rows_after = db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")[0]["cnt"]
        assert vuln_rows_after == 3, f"Vulnerability rows lost after policy load: expected 3, got {vuln_rows_after}"

        db.close()


def test_VulnerabilityDatabase_VulnLoadPreservesPolicyData(
    sample_parquet_file, sample_asset_parquet_file, sample_policy_parquet_file
):
    """Loading vulnerability data must not wipe previously loaded policy data.

    The reverse scenario: a vulnerability snapshot should only replace
    assets + vulnerabilities tables, leaving policies intact.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "vuln_preserves_policy.db")
        db = VulnerabilityDatabase(db_path)

        # Step 1: Load policy data
        db.load_parquet_files_by_prefix({"asset_policy": [sample_policy_parquet_file]})
        policy_rows = db.query("SELECT COUNT(*) AS cnt FROM policies")[0]["cnt"]
        assert policy_rows == 2

        # Step 2: Load vulnerability data (replaces assets + vulnerabilities)
        db.load_parquet_files_by_prefix(
            {
                "asset": [sample_asset_parquet_file],
                "asset_vulnerability": [sample_parquet_file],
            }
        )

        # Vulnerability + asset data must be present
        vuln_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")[0]["cnt"]
        assert vuln_rows == 3
        asset_rows = db.query("SELECT COUNT(*) AS cnt FROM assets")[0]["cnt"]
        assert asset_rows == 2

        # Policy data must still be intact
        policy_rows_after = db.query("SELECT COUNT(*) AS cnt FROM policies")[0]["cnt"]
        assert policy_rows_after == 2, f"Policy rows lost after vulnerability load: expected 2, got {policy_rows_after}"

        db.close()


def test_append_returns_per_call_inserted_counts(sample_remediation_parquet_file, tmp_path):
    """Append mode must report rows inserted BY THIS CALL, not the table's
    cumulative total. Regression for the multi-window remediation sum bug
    where three 2-row windows reported 2, 4, 6 (total 12) instead of 2, 2, 2."""
    db = VulnerabilityDatabase(str(tmp_path / "append_delta.db"))
    prefix_map = {"vulnerability_remediation": [sample_remediation_parquet_file]}

    first = db.load_parquet_files_by_prefix(prefix_map, append=True)
    second = db.load_parquet_files_by_prefix(prefix_map, append=True)
    third = db.load_parquet_files_by_prefix(prefix_map, append=True)

    # Each call inserted the same 2 rows — the returned count is the delta.
    assert first["vulnerability_remediation"] == 2
    assert second["vulnerability_remediation"] == 2
    assert third["vulnerability_remediation"] == 2

    # Summing per-call counts gives the true total (2+2+2=6), not 2+4+6=12.
    total = (
        first["vulnerability_remediation"] + second["vulnerability_remediation"] + third["vulnerability_remediation"]
    )
    assert total == 6
    # And the table really does hold 6 rows.
    assert db.query("SELECT COUNT(*) AS c FROM vulnerability_remediation")[0]["c"] == 6
    db.close()


def test_query_timeout_cancels_slow_query(sample_parquet_file, monkeypatch, tmp_path):
    """A query exceeding the configured limit is cancelled with a friendly message."""
    monkeypatch.setenv("DUCKDB_QUERY_TIMEOUT_SECONDS", "0.2")
    db = VulnerabilityDatabase(str(tmp_path / "timeout.db"))
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    with pytest.raises(ValueError, match="Query cancelled") as exc_info:
        db.query(_SLOW_QUERY)

    message = str(exc_info.value)
    # The cancellation must be distinguishable from the generic failure wrapper
    # and must guide the user toward a query that fits.
    assert "Query execution failed" not in message
    assert "LIMIT" in message
    assert "WHERE" in message

    db.close()


def test_query_timeout_does_not_affect_fast_query(sample_parquet_file, monkeypatch, tmp_path):
    """A fast query completes normally even with a small timeout configured."""
    monkeypatch.setenv("DUCKDB_QUERY_TIMEOUT_SECONDS", "0.2")
    db = VulnerabilityDatabase(str(tmp_path / "fast.db"))
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    results = db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")
    assert results[0]["cnt"] == 3

    db.close()


def test_query_timeout_timer_cancelled_on_success(sample_parquet_file, monkeypatch, tmp_path):
    """A successful query leaves no watchdog timer thread running."""
    monkeypatch.setenv("DUCKDB_QUERY_TIMEOUT_SECONDS", "30")
    db = VulnerabilityDatabase(str(tmp_path / "no_leak.db"))
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    before = threading.active_count()
    db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")
    # The timer is cancelled synchronously inside query(), so the thread count
    # returns to baseline immediately — a leaked timer would leave a live thread
    # counting down toward the 30-second interrupt.
    assert threading.active_count() == before

    db.close()


def test_query_timeout_disabled_runs_slow_query(sample_parquet_file, monkeypatch, tmp_path):
    """A limit of 0 disables the watchdog so no interrupt fires."""
    monkeypatch.setenv("DUCKDB_QUERY_TIMEOUT_SECONDS", "0")
    db = VulnerabilityDatabase(str(tmp_path / "disabled.db"))
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    before = threading.active_count()
    # With the guard off this completes rather than being cancelled; a bounded
    # slow query keeps the test quick while proving no timer was armed.
    results = db.query("SELECT COUNT(*) AS cnt FROM range(200000) WHERE range % 3 = 0")
    assert results[0]["cnt"] > 0
    assert threading.active_count() == before

    db.close()


def test_resolve_query_timeout_disabled_when_unset(monkeypatch):
    """Unset means no limit, so local queries run to completion as before."""
    monkeypatch.delenv("DUCKDB_QUERY_TIMEOUT_SECONDS", raising=False)
    assert _resolve_query_timeout() == DEFAULT_QUERY_TIMEOUT_SECONDS
    assert DEFAULT_QUERY_TIMEOUT_SECONDS <= 0


def test_query_timeout_unset_arms_no_timer(sample_parquet_file, monkeypatch, tmp_path):
    """With the variable unset, a query arms no watchdog timer at all."""
    monkeypatch.delenv("DUCKDB_QUERY_TIMEOUT_SECONDS", raising=False)
    db = VulnerabilityDatabase(str(tmp_path / "unset.db"))
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    armed = []
    real_timer = threading.Timer

    def _recording_timer(*args, **kwargs):
        armed.append(args)
        return real_timer(*args, **kwargs)

    monkeypatch.setattr(threading, "Timer", _recording_timer)
    db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")

    assert armed == []
    db.close()


def test_resolve_query_timeout_invalid_falls_back_with_warning(monkeypatch, capsys):
    """An unparseable value falls back to the default and warns on stderr."""
    monkeypatch.setenv("DUCKDB_QUERY_TIMEOUT_SECONDS", "not-a-number")
    assert _resolve_query_timeout() == DEFAULT_QUERY_TIMEOUT_SECONDS
    captured = capsys.readouterr()
    assert "ignoring invalid DUCKDB_QUERY_TIMEOUT_SECONDS" in captured.err


def test_load_metadata_recorded_for_loaded_tables(sample_parquet_file, tmp_path):
    """A load stamps a last-loaded time for each table it wrote."""
    db = VulnerabilityDatabase(str(tmp_path / "load_meta.db"))
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    metadata = db.get_load_metadata()

    assert set(metadata) == {"vulnerabilities"}
    assert isinstance(metadata["vulnerabilities"], datetime)
    db.close()


def test_load_metadata_empty_when_nothing_loaded(tmp_path):
    """A database that has never loaded data reports no load metadata."""
    db = VulnerabilityDatabase(str(tmp_path / "empty_meta.db"))
    assert db.get_load_metadata() == {}
    db.close()


def test_load_metadata_survives_snapshot_reload(sample_parquet_file, sample_asset_parquet_file, tmp_path):
    """A snapshot reload compacts the database; the load metadata must survive
    the copy-to-fresh-file and reflect the most recent load."""
    db = VulnerabilityDatabase(str(tmp_path / "reload_meta.db"))

    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})
    first = db.get_load_metadata()["vulnerabilities"]

    # A second snapshot load of the same tables triggers compaction (COPY FROM
    # DATABASE to a fresh file). Metadata for the reloaded table must persist
    # and advance to the newer load time.
    db.load_parquet_files_by_prefix(
        {"asset": [sample_asset_parquet_file], "asset_vulnerability": [sample_parquet_file]}
    )
    after = db.get_load_metadata()

    assert set(after) == {"assets", "vulnerabilities"}
    assert after["vulnerabilities"] >= first
    db.close()


def test_load_metadata_table_excluded_from_schema(sample_parquet_file, tmp_path):
    """The internal load-metadata table must never appear in get_schema output,
    so it is not mistaken for Rapid7 data."""
    db = VulnerabilityDatabase(str(tmp_path / "schema_meta.db"))
    db.load_parquet_files_by_prefix({"asset_vulnerability": [sample_parquet_file]})

    schema = db.get_schema()

    assert "_load_metadata" not in schema
    assert "vulnerabilities" in schema
    db.close()
