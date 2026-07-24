"""
Local end-to-end test against the real Rapid7 API.

Requires RAPID7_API_KEY and RAPID7_REGION to be set. Skipped automatically
when these are absent so the test never runs in CI on the public repo.

Run locally:
    # Direct env var:
    RAPID7_API_KEY=<key> RAPID7_REGION=us uv run pytest tests/test_local_e2e.py -v -s

    # Via 1Password CLI:
    op run --env-file=.env.1password -- uv run pytest tests/test_local_e2e.py -v -s

    # Via make:
    make local-test

The test uses a temporary DATA_DIR so it never touches ~/.rapid7_mcp and cleans
up after itself. It verifies the full flow for each export type:

    start export → poll until complete → download → assert expected tables populated
    → reload (snapshot replace) → assert DB file did not grow beyond a bound
    → assert remediation survives a vulnerability snapshot reload
"""

import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from src.config import load_config
from src.download import download_all_files
from src.duckdb_loader import VulnerabilityDatabase
from src.export_manager import create_vulnerability_export, get_export_status
from src.export_tracker import ExportTracker

# ---------------------------------------------------------------------------
# Skip guard — never runs in CI
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
    not os.environ.get("RAPID7_API_KEY"),
    reason="RAPID7_API_KEY not set — local-only test",
)

_POLL_INTERVAL = 15
_MAX_WAIT = 600  # 10 minutes hard ceiling

# Tables that must be populated for each export type.
# At least one of the listed tables must have rows after a successful load.
_EXPECTED_TABLES = {
    "vulnerability": ["assets", "vulnerabilities"],
    "policy": ["policies"],
    "remediation": ["vulnerability_remediation"],
    "asset_software": ["asset_software"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _poll_until_complete(config, export_id):
    """Poll export status until terminal state, returning status_info."""
    deadline = time.monotonic() + _MAX_WAIT
    while time.monotonic() < deadline:
        status_info = get_export_status(config, export_id)
        status = status_info["status"]
        print(f"  export {export_id}: {status}", flush=True)
        if status in ("COMPLETE", "SUCCEEDED"):
            return status_info
        if status == "FAILED":
            pytest.fail(f"Export {export_id} FAILED on Rapid7 servers")
        time.sleep(_POLL_INTERVAL)
    pytest.fail(f"Export {export_id} did not complete within {_MAX_WAIT}s")


def _download_and_load(config, export_id, export_type, db, status_info):
    """Download parquet files and load into db. Returns (row_counts, file_data, url_to_prefix)."""
    parquet_urls = status_info["parquetFiles"]
    assert parquet_urls, f"{export_type} export completed but returned no Parquet URLs"
    print(f"  {len(parquet_urls)} file(s) ready")

    print(f"  Downloading {len(parquet_urls)} file(s)...")
    file_data = download_all_files(parquet_urls, config["api_key"])
    assert len(file_data) == len(parquet_urls)

    result_list = status_info.get("result") or []
    url_to_prefix = {}
    for item in result_list:
        for url in item.get("urls", []):
            url_to_prefix[url] = item.get("prefix", "unknown")

    scratch = tempfile.mkdtemp()
    try:
        prefix_file_map: dict = {}
        for i, (url, data) in enumerate(zip(parquet_urls, file_data)):
            p = Path(scratch) / f"{export_type}_{i}.parquet"
            p.write_bytes(data)
            prefix = url_to_prefix.get(url, "unknown")
            prefix_file_map.setdefault(prefix, []).append(str(p))

        if export_type == "policy":
            row_counts = db.load_parquet_files_by_prefix(prefix_file_map, skip_prefixes={"asset"})
        elif export_type == "remediation":
            row_counts = db.load_parquet_files_by_prefix(prefix_file_map, append=True)
        else:
            row_counts = db.load_parquet_files_by_prefix(prefix_file_map)
    finally:
        shutil.rmtree(scratch)

    return row_counts, file_data, url_to_prefix


def _assert_tables_populated(db, export_type, row_counts):
    """Assert that every expected table for this export type has rows."""
    expected = _EXPECTED_TABLES.get(export_type, [])
    for table in expected:
        count = row_counts.get(table, 0)
        assert count > 0, (
            f"Expected table '{table}' to be populated after {export_type} export, "
            f"but got {count} rows. Full row_counts: {row_counts}"
        )
        print(f"  ✓ {table}: {count:,} rows")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_VulnerabilityExport_FullRoundTrip():
    """Full live round-trip for vulnerability export:
    create → poll → download → assert tables populated → reload → assert size stable
    → assert remediation survives snapshot reload.
    """
    config = load_config()

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "rapid7_bulk_export.db")
        tracking_path = str(Path(tmpdir) / "rapid7_bulk_export_tracking.db")

        # --- 1. Start export ---
        print("\n[1/6] Creating vulnerability export...")
        export_id = create_vulnerability_export(config)
        print(f"  export_id: {export_id}")

        tracker = ExportTracker(tracking_path)
        tracker.save_export(export_id=export_id, status="PENDING", parquet_urls=[], export_type="vulnerability")
        tracker.close()

        # --- 2. Poll until complete ---
        print("[2/6] Polling for completion...")
        status_info = _poll_until_complete(config, export_id)

        # --- 3. Download and load ---
        print("[3/6] Downloading and loading...")
        db = VulnerabilityDatabase(db_path)
        row_counts, file_data, url_to_prefix = _download_and_load(config, export_id, "vulnerability", db, status_info)

        total_rows = sum(row_counts.values())
        print(f"  loaded {total_rows:,} rows: {row_counts}")

        tracker = ExportTracker(tracking_path)
        tracker.save_export(
            export_id=export_id,
            status="COMPLETE",
            parquet_urls=status_info["parquetFiles"],
            row_count=total_rows,
            export_type="vulnerability",
        )
        tracker.close()

        # --- 4. Assert expected tables populated ---
        print("[4/6] Asserting expected tables populated...")
        _assert_tables_populated(db, "vulnerability", row_counts)

        # Sanity SQL query
        results = db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")
        assert results[0]["cnt"] > 0

        size_after_first_load = Path(db_path).stat().st_size
        print(f"  DB size after first load: {size_after_first_load / 1_048_576:.1f} MB")

        # --- 5. Reload — assert file does not grow ---
        print("[5/6] Reloading to verify storage fix...")
        parquet_urls = status_info["parquetFiles"]
        scratch2 = tempfile.mkdtemp()
        try:
            prefix_file_map2: dict = {}
            for i, (url, data) in enumerate(zip(parquet_urls, file_data)):
                p = Path(scratch2) / f"vuln_{i}.parquet"
                p.write_bytes(data)
                prefix = url_to_prefix.get(url, "unknown")
                prefix_file_map2.setdefault(prefix, []).append(str(p))
            db.load_parquet_files_by_prefix(prefix_file_map2)
        finally:
            shutil.rmtree(scratch2)

        size_after_reload = Path(db_path).stat().st_size
        print(f"  DB size after reload:      {size_after_reload / 1_048_576:.1f} MB")

        assert size_after_reload <= size_after_first_load * 1.5, (
            f"DB grew from {size_after_first_load} → {size_after_reload} bytes after one reload "
            f"({size_after_reload / size_after_first_load:.1f}x). File-reset not working."
        )

        # --- 6. Assert remediation survives snapshot reload ---
        print("[6/6] Verifying remediation data survives snapshot reload...")
        # Seed some remediation rows directly so we can verify rescue without a full remediation export
        from src.db_utils import duckdb_connection

        with duckdb_connection(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vulnerability_remediation AS
                SELECT 'ASSET-1' AS assetId, 'CVE-2024-9999' AS cveId, 'Critical' AS cvssV3Severity
            """)

        remediation_before = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_remediation")[0]["cnt"]
        assert remediation_before > 0, "Failed to seed remediation data for rescue test"

        # Snapshot-reload vulnerabilities — remediation must survive
        scratch3 = tempfile.mkdtemp()
        try:
            prefix_file_map3: dict = {}
            for i, (url, data) in enumerate(zip(parquet_urls, file_data)):
                p = Path(scratch3) / f"vuln_{i}.parquet"
                p.write_bytes(data)
                prefix = url_to_prefix.get(url, "unknown")
                prefix_file_map3.setdefault(prefix, []).append(str(p))
            db.load_parquet_files_by_prefix(prefix_file_map3)
        finally:
            shutil.rmtree(scratch3)

        remediation_after = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_remediation")[0]["cnt"]
        assert remediation_after == remediation_before, (
            f"Remediation rows lost after snapshot reload: had {remediation_before}, now {remediation_after}"
        )
        print(f"  ✓ vulnerability_remediation: {remediation_after} row(s) preserved")

        print(f"\nAll assertions passed. Final DB size: {size_after_reload / 1_048_576:.1f} MB")
        db.close()
