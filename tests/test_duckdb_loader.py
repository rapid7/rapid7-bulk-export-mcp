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


def test_database_initialization():
    """Test that database can be initialized."""
    db = VulnerabilityDatabase()
    assert db.conn is not None
    db.close()


def test_load_parquet_files(sample_parquet_file):
    """Test loading Parquet files into database."""
    db = VulnerabilityDatabase()

    row_count = db.load_parquet_files([sample_parquet_file])

    assert row_count == 3
    db.close()


def test_query_execution(sample_parquet_file):
    """Test executing SQL queries."""
    db = VulnerabilityDatabase()
    db.load_parquet_files([sample_parquet_file])

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
    db.load_parquet_files([sample_parquet_file])

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
    db.load_parquet_files([sample_parquet_file])

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
        db.load_parquet_files([sample_parquet_file])
        results = db.query("SELECT COUNT(*) as count FROM vulnerabilities")
        assert results[0]["count"] == 3


def test_persistent_database(sample_parquet_file):
    """Test creating a persistent database file."""
    # Create a temp directory and use a proper path
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create and populate database
        db = VulnerabilityDatabase(str(db_path))
        db.load_parquet_files([sample_parquet_file])
        db.close()

        # Reopen and verify data persists
        db = VulnerabilityDatabase(str(db_path))
        results = db.query("SELECT COUNT(*) as count FROM vulnerabilities")
        assert results[0]["count"] == 3
        db.close()


def test_query_with_aggregation(sample_parquet_file):
    """Test queries with aggregation."""
    db = VulnerabilityDatabase()
    db.load_parquet_files([sample_parquet_file])

    results = db.query("""
        SELECT severity, COUNT(*) as count
        FROM vulnerabilities
        GROUP BY severity
        ORDER BY count DESC
    """)

    assert len(results) == 3
    assert all("severity" in row and "count" in row for row in results)

    db.close()


def test_empty_parquet_list():
    """Test that empty Parquet list raises error."""
    db = VulnerabilityDatabase()

    with pytest.raises(ValueError, match="No Parquet files provided"):
        db.load_parquet_files([])

    db.close()


def test_invalid_query(sample_parquet_file):
    """Test that invalid queries raise errors."""
    db = VulnerabilityDatabase()
    db.load_parquet_files([sample_parquet_file])

    with pytest.raises(ValueError, match="Query execution failed"):
        db.query("SELECT * FROM nonexistent_table")

    db.close()
