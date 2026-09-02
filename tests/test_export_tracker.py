"""Tests for the export tracker module."""

import tempfile
from datetime import date
from pathlib import Path

import pytest

from src.export_tracker import ExportTracker


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as f:
        db_path = f.name
    # File is now deleted, DuckDB can create it fresh
    yield db_path
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def test_initialize_tracker(temp_db):
    """Test that the tracker initializes correctly."""
    tracker = ExportTracker(temp_db)
    assert tracker.db_path == temp_db
    tracker.close()


def test_save_and_get_export(temp_db):
    """Test saving and retrieving export metadata."""
    tracker = ExportTracker(temp_db)

    export_id = "test-export-123"
    parquet_urls = ["https://example.com/file1.parquet", "https://example.com/file2.parquet"]

    tracker.save_export(export_id=export_id, status="COMPLETE", parquet_urls=parquet_urls, row_count=1000)

    # Retrieve by ID
    export = tracker.get_export_by_id(export_id)
    assert export is not None
    assert export["export_id"] == export_id
    assert export["status"] == "COMPLETE"
    assert export["file_count"] == 2
    assert export["row_count"] == 1000
    assert export["parquet_urls"] == parquet_urls

    tracker.close()


def test_get_today_export(temp_db):
    """Test retrieving today's export."""
    tracker = ExportTracker(temp_db)

    # Should be None initially
    assert tracker.get_today_export() is None

    # Save an export
    export_id = "today-export-456"
    parquet_urls = ["https://example.com/file.parquet"]

    tracker.save_export(export_id=export_id, status="COMPLETE", parquet_urls=parquet_urls, row_count=500)

    # Should now return the export
    today_export = tracker.get_today_export()
    assert today_export is not None
    assert today_export["export_id"] == export_id
    assert today_export["export_date"] == date.today()

    tracker.close()


def test_list_exports(temp_db):
    """Test listing exports."""
    tracker = ExportTracker(temp_db)

    # Add multiple exports
    for i in range(5):
        tracker.save_export(
            export_id=f"export-{i}",
            status="COMPLETE",
            parquet_urls=[f"https://example.com/file{i}.parquet"],
            row_count=100 * i,
        )

    # List all
    exports = tracker.list_exports(limit=10)
    assert len(exports) == 5

    # List limited
    exports = tracker.list_exports(limit=3)
    assert len(exports) == 3

    tracker.close()


def test_update_export(temp_db):
    """Test updating an existing export."""
    tracker = ExportTracker(temp_db)

    export_id = "update-test-789"

    # Initial save
    tracker.save_export(export_id=export_id, status="PROCESSING", parquet_urls=[], row_count=None)

    # Update with completion
    parquet_urls = ["https://example.com/final.parquet"]
    tracker.save_export(export_id=export_id, status="COMPLETE", parquet_urls=parquet_urls, row_count=2000)

    # Verify update
    export = tracker.get_export_by_id(export_id)
    assert export["status"] == "COMPLETE"
    assert export["row_count"] == 2000
    assert export["parquet_urls"] == parquet_urls

    tracker.close()


def test_context_manager(temp_db):
    """Test using tracker as a context manager."""
    with ExportTracker(temp_db) as tracker:
        tracker.save_export(
            export_id="context-test", status="COMPLETE", parquet_urls=["https://example.com/test.parquet"]
        )

        export = tracker.get_export_by_id("context-test")
        assert export is not None

    # Connection should be closed after context
    # We can't easily test this without accessing private attributes


def test_find_job_by_range(temp_db):
    """find_job_by_range returns the job matching an exact export_type + range."""
    tracker = ExportTracker(temp_db)
    tracker.create_job(
        job_id="job-a",
        export_type="remediation",
        start_date="2026-01-01",
        end_date="2026-03-01",
        chunks=[{"start": "2026-01-01", "end": "2026-02-01", "export_id": None, "status": "pending"}],
        status="RUNNING",
    )
    found = tracker.find_job_by_range("remediation", "2026-01-01", "2026-03-01")
    assert found is not None and found["job_id"] == "job-a"
    # A different range does not match.
    assert tracker.find_job_by_range("remediation", "2026-01-01", "2026-02-01") is None


def test_reconcile_interrupted_job_names_loaded_and_missing_windows(temp_db):
    """A RUNNING job reconciled at restart must report per-window state, not a
    blanket re-run of the whole range (which would duplicate loaded windows)."""
    tracker = ExportTracker(temp_db)
    tracker.create_job(
        job_id="job-interrupted",
        export_type="remediation",
        start_date="2026-01-01",
        end_date="2026-03-04",
        chunks=[
            {"start": "2026-01-01", "end": "2026-02-01", "export_id": "e1", "status": "loaded"},
            {"start": "2026-02-01", "end": "2026-03-04", "export_id": None, "status": "loading"},
        ],
        status="RUNNING",
    )

    tracker.reconcile_interrupted(
        active_export_statuses=["DOWNLOADING", "LOADING"],
        active_job_statuses=["RUNNING"],
        reason="Interrupted by a server restart.",
    )

    job = tracker.get_job("job-interrupted")
    assert job["status"] == "FAILED"
    assert "2026-01-01 → 2026-02-01" in job["message"]  # loaded window named as kept
    assert "2026-02-01 → 2026-03-04" in job["message"]  # missing window named
    assert "only the missing" in job["message"]
