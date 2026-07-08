"""
Tests for the DuckDB loader module.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.duckdb_loader import VulnerabilityDatabase


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
