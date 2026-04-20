"""Integration tests for export tracking functionality."""

import tempfile
from pathlib import Path

from src.export_tracker import ExportTracker


def test_export_tracker_integration():
    """Test the complete export tracking workflow."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as f:
        db_path = f.name

    # Initialize tracker
    tracker = ExportTracker(db_path)

    # Verify no exports initially
    assert tracker.get_today_export() is None

    # Simulate saving an export
    export_id = "integration-test-export"
    parquet_urls = ["https://example.com/file1.parquet", "https://example.com/file2.parquet"]
    local_files = ["/tmp/file1.parquet", "/tmp/file2.parquet"]

    tracker.save_export(
        export_id=export_id, status="COMPLETE", parquet_urls=parquet_urls, local_files=local_files, row_count=5000
    )

    # Verify we can retrieve today's export
    today_export = tracker.get_today_export()
    assert today_export is not None
    assert today_export["export_id"] == export_id
    assert today_export["status"] == "COMPLETE"
    assert today_export["file_count"] == 2
    assert today_export["row_count"] == 5000
    assert today_export["parquet_urls"] == parquet_urls
    assert today_export["local_files"] == local_files

    # Verify it appears in the list
    exports = tracker.list_exports(limit=10)
    assert len(exports) == 1
    assert exports[0]["export_id"] == export_id

    # Verify we can retrieve by ID
    export_by_id = tracker.get_export_by_id(export_id)
    assert export_by_id is not None
    assert export_by_id["export_id"] == export_id

    tracker.close()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def test_export_reuse_logic():
    """Test the logic for determining whether to reuse an export."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as f:
        db_path = f.name

    tracker = ExportTracker(db_path)

    # First call - no export exists
    today_export = tracker.get_today_export()
    assert today_export is None  # Should create new export

    # Save an export
    tracker.save_export(
        export_id="test-export-1", status="COMPLETE", parquet_urls=["https://example.com/file.parquet"], row_count=1000
    )

    # Second call - export exists
    today_export = tracker.get_today_export()
    assert today_export is not None  # Should reuse this export
    assert today_export["export_id"] == "test-export-1"

    tracker.close()

    # Cleanup
    Path(db_path).unlink(missing_ok=True)
