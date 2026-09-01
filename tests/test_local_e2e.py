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
up after itself. It verifies:

    1. Parallel export creation (vulnerability + policy + remediation)
    2. Concurrent polling until all exports complete
    3. Sequential download + load respecting snapshot/append semantics
    4. Cross-export table coexistence (policy doesn't wipe vuln, and vice versa)
    5. DB compaction (file doesn't grow on reload)
    6. Remediation data survives snapshot reloads
    7. Every MCP-exposed tool: query, schema, stats, export listing, purge
"""

import datetime as _dt
import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from src.config import load_config
from src.download import download_all_files
from src.duckdb_loader import VulnerabilityDatabase
from src.export_manager import (
    build_remediation_date_chunks,
    create_policy_export,
    create_remediation_export,
    create_vulnerability_export,
    get_export_status,
)
from src.export_tracker import ExportTracker

# ---------------------------------------------------------------------------
# Skip guard — never runs in CI
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
    not os.environ.get("RAPID7_API_KEY"),
    reason="RAPID7_API_KEY not set — local-only test",
)

_POLL_INTERVAL = 15
_MAX_WAIT = 900  # 15 minutes hard ceiling (multiple exports polling together)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _poll_all_until_complete(config, export_ids: dict) -> dict:
    """Poll multiple exports in round-robin until all reach terminal state.

    Args:
        config: Rapid7 config dict.
        export_ids: Mapping of {label: export_id} for each export to poll.

    Returns:
        Mapping of {label: status_info} for completed exports.
    """
    results = {}
    pending = dict(export_ids)
    deadline = time.monotonic() + _MAX_WAIT

    while pending and time.monotonic() < deadline:
        for label, eid in list(pending.items()):
            status_info = get_export_status(config, eid)
            status = status_info["status"]
            print(f"  [{label}] {eid}: {status}", flush=True)
            if status in ("COMPLETE", "SUCCEEDED"):
                results[label] = status_info
                del pending[label]
            elif status == "FAILED":
                pytest.fail(f"Export [{label}] {eid} FAILED on Rapid7 servers")
        if pending:
            time.sleep(_POLL_INTERVAL)

    if pending:
        pytest.fail(f"Exports did not complete within {_MAX_WAIT}s: {list(pending.keys())}")

    return results


def _download_and_load(config, export_type, db, status_info, **load_kwargs):
    """Download parquet files and load into db.

    Returns:
        Tuple of (row_counts, file_data, url_to_prefix) for potential reuse.
    """
    parquet_urls = status_info["parquetFiles"]
    assert parquet_urls, f"{export_type} export completed but returned no Parquet URLs"
    print(f"  [{export_type}] {len(parquet_urls)} file(s) ready")

    print(f"  [{export_type}] Downloading {len(parquet_urls)} file(s)...")
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

        row_counts = db.load_parquet_files_by_prefix(prefix_file_map, **load_kwargs)
    finally:
        shutil.rmtree(scratch)

    return row_counts, file_data, url_to_prefix


def _reload_from_cached(file_data, url_to_prefix, parquet_urls, export_type, db, **load_kwargs):
    """Reload from previously downloaded file_data without re-downloading."""
    scratch = tempfile.mkdtemp()
    try:
        prefix_file_map: dict = {}
        for i, (url, data) in enumerate(zip(parquet_urls, file_data)):
            p = Path(scratch) / f"{export_type}_{i}.parquet"
            p.write_bytes(data)
            prefix = url_to_prefix.get(url, "unknown")
            prefix_file_map.setdefault(prefix, []).append(str(p))
        row_counts = db.load_parquet_files_by_prefix(prefix_file_map, **load_kwargs)
    finally:
        shutil.rmtree(scratch)
    return row_counts


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_FullExportRoundTrip_ParallelWithToolCoverage():
    """Comprehensive live test: parallel exports, cross-table coexistence,
    compaction, and full tool coverage (query, schema, stats, tracker, purge).
    """
    config = load_config()

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "rapid7_bulk_export.db")
        tracking_path = str(Path(tmpdir) / "rapid7_bulk_export_tracking.db")

        # ==================================================================
        # PHASE 1: Start exports (vuln + policy + first remediation chunk in parallel)
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 1: Starting exports")
        print("=" * 70)

        vuln_id = create_vulnerability_export(config)
        print(f"  [vulnerability] export_id: {vuln_id}")

        policy_id = create_policy_export(config)
        print(f"  [policy] export_id: {policy_id}")

        # Remediation: last 90 days to test multi-chunk logic (31-day API limit → 3 chunks)
        # The API only allows one remediation export at a time, so we start the first
        # chunk in parallel with vuln+policy, then run the rest sequentially after.
        end_date = _dt.date.today().isoformat()
        start_date = (_dt.date.today() - _dt.timedelta(days=90)).isoformat()
        remediation_chunks = build_remediation_date_chunks(start_date, end_date)
        print(f"  [remediation] {len(remediation_chunks)} chunks: {start_date} → {end_date}")

        # Start first remediation chunk alongside vuln + policy
        chunk_start, chunk_end = remediation_chunks[0]
        remediation_0_id = create_remediation_export(config, chunk_start, chunk_end)
        print(f"  [remediation_0] export_id: {remediation_0_id} ({chunk_start} → {chunk_end})")

        # Track initial exports
        tracker = ExportTracker(tracking_path)
        tracker.save_export(export_id=vuln_id, status="PENDING", parquet_urls=[], export_type="vulnerability")
        tracker.save_export(export_id=policy_id, status="PENDING", parquet_urls=[], export_type="policy")
        tracker.save_export(export_id=remediation_0_id, status="PENDING", parquet_urls=[], export_type="remediation")
        tracker.close()

        # ==================================================================
        # PHASE 2: Poll vuln + policy + remediation_0 in parallel
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 2: Polling parallel exports until complete")
        print("=" * 70)

        completed = _poll_all_until_complete(
            config,
            {
                "vulnerability": vuln_id,
                "policy": policy_id,
                "remediation_0": remediation_0_id,
            },
        )

        # ==================================================================
        # PHASE 3: Download and load data
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 3: Downloading and loading data")
        print("=" * 70)

        db = VulnerabilityDatabase(db_path)

        # Load vulnerability first (snapshot — creates assets + vulnerabilities + vulnerability_exceptions)
        print("\n  Loading vulnerability export...")
        vuln_counts, vuln_file_data, vuln_url_to_prefix = _download_and_load(
            config, "vulnerability", db, completed["vulnerability"]
        )
        print(f"  → {sum(vuln_counts.values()):,} rows: {vuln_counts}")

        # Load policy (snapshot — creates policies, skips asset prefix)
        print("\n  Loading policy export...")
        policy_counts, _, _ = _download_and_load(config, "policy", db, completed["policy"], skip_prefixes={"asset"})
        print(f"  → {sum(policy_counts.values()):,} rows: {policy_counts}")

        # Load first remediation chunk (append mode)
        print("\n  Loading remediation chunk 1/{len(remediation_chunks)}...")
        chunk_0_counts, _, _ = _download_and_load(config, "remediation", db, completed["remediation_0"], append=True)
        total_remediation_rows = sum(chunk_0_counts.values())
        print(f"  → chunk 1: {total_remediation_rows:,} rows")

        # Remaining remediation chunks: start → poll → download → load sequentially
        # (API only allows one remediation export at a time)
        for i, (chunk_start, chunk_end) in enumerate(remediation_chunks[1:], start=1):
            print(f"\n  Starting remediation chunk {i + 1}/{len(remediation_chunks)} ({chunk_start} → {chunk_end})...")
            rid = create_remediation_export(config, chunk_start, chunk_end)
            print(f"  [remediation_{i}] export_id: {rid}")

            tracker = ExportTracker(tracking_path)
            tracker.save_export(export_id=rid, status="PENDING", parquet_urls=[], export_type="remediation")
            tracker.close()

            # Poll this chunk to completion
            chunk_status = _poll_all_until_complete(config, {f"remediation_{i}": rid})

            # Download and load
            chunk_counts, _, _ = _download_and_load(
                config, "remediation", db, chunk_status[f"remediation_{i}"], append=True
            )
            chunk_rows = sum(chunk_counts.values())
            total_remediation_rows += chunk_rows
            print(f"  → chunk {i + 1}: {chunk_rows:,} rows")

        print(f"\n  Total remediation rows across {len(remediation_chunks)} chunks: {total_remediation_rows:,}")

        # ==================================================================
        # PHASE 3b: Async download tool round-trip (the timeout-crash fix)
        #
        # PHASE 3 loads via the low-level helpers. This phase exercises the
        # actual MCP tool path — download_rapid7_export() spawning a
        # background thread and returning immediately, then
        # check_rapid7_export_status() reporting the local load phase from
        # the tracker through to COMPLETE — against the live-completed vuln
        # export, in its own isolated database.
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 3b: Async download tool round-trip")
        print("=" * 70)

        from src import mcp_server

        async_dir = Path(tmpdir) / "async"
        async_dir.mkdir()
        orig_data_dir = mcp_server._DATA_DIR
        orig_db = mcp_server.db
        mcp_server._DATA_DIR = async_dir
        mcp_server.db = VulnerabilityDatabase(str(async_dir / "rapid7_bulk_export.db"))
        try:
            start = time.monotonic()
            started = mcp_server.download_rapid7_export(export_id=vuln_id, export_type="vulnerability")
            elapsed = time.monotonic() - start
            assert "Started downloading" in started, started
            assert elapsed < 5, f"download tool blocked for {elapsed:.1f}s instead of returning immediately"
            print(f"  ✓ download_rapid7_export returned in {elapsed:.2f}s (non-blocking)")

            # Poll the unified status tool until the local load reaches a terminal state.
            deadline = time.monotonic() + _MAX_WAIT
            final = ""
            while time.monotonic() < deadline:
                final = mcp_server.check_rapid7_export_status(export_id=vuln_id)
                if "loaded successfully" in final or "Error downloading/loading" in final:
                    break
                assert "in progress locally" in final, f"unexpected status: {final}"
                time.sleep(_POLL_INTERVAL)

            assert "loaded successfully" in final, f"async load did not complete: {final}"
            print("  ✓ check_rapid7_export_status reported COMPLETE via tracker phase")

            async_rows = mcp_server.db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")[0]["cnt"]
            assert async_rows > 0, "async tool path loaded 0 vulnerability rows"
            print(f"  ✓ async-loaded vulnerabilities: {async_rows:,} rows")
        finally:
            mcp_server._DATA_DIR = orig_data_dir
            mcp_server.db = orig_db

        # ==================================================================
        # PHASE 4: Assert cross-export table coexistence (the original bug)
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 4: Asserting all tables coexist")
        print("=" * 70)

        # Vulnerability export tables
        vuln_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerabilities")[0]["cnt"]
        assert vuln_rows > 0, "vulnerabilities table is empty after loading"
        print(f"  ✓ vulnerabilities: {vuln_rows:,} rows")

        asset_rows = db.query("SELECT COUNT(*) AS cnt FROM assets")[0]["cnt"]
        assert asset_rows > 0, "assets table is empty after loading"
        print(f"  ✓ assets: {asset_rows:,} rows")

        # Policy tables (must survive after vuln load)
        policy_rows = db.query("SELECT COUNT(*) AS cnt FROM policies")[0]["cnt"]
        assert policy_rows > 0, "policies table is empty — policy data was wiped by another load!"
        print(f"  ✓ policies: {policy_rows:,} rows")

        # Remediation table (may be empty if org has no remediations in the date range)
        try:
            remediation_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_remediation")[0]["cnt"]
            if remediation_rows > 0:
                print(f"  ✓ vulnerability_remediation: {remediation_rows:,} rows")
            else:
                print("  ⚠ vulnerability_remediation: 0 rows (org has no remediations in last 90 days)")
        except ValueError:
            remediation_rows = 0
            print("  ⚠ vulnerability_remediation: table not created (all chunks were empty)")

        # Vulnerability exceptions (if org has them)
        try:
            exception_rows = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_exceptions")[0]["cnt"]
            print(f"  ✓ vulnerability_exceptions: {exception_rows:,} rows")
        except ValueError:
            print("  - vulnerability_exceptions: not present (org has none)")

        # ==================================================================
        # PHASE 5: Verify compaction (reload vuln, DB doesn't grow)
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 5: Verifying DB compaction on reload")
        print("=" * 70)

        size_after_first = Path(db_path).stat().st_size
        print(f"  DB size after all loads: {size_after_first / 1_048_576:.1f} MB")

        # Reload vulnerability data (snapshot replace — drops and recreates vuln tables)
        _reload_from_cached(
            vuln_file_data,
            vuln_url_to_prefix,
            completed["vulnerability"]["parquetFiles"],
            "vulnerability",
            db,
        )

        size_after_reload = Path(db_path).stat().st_size
        print(f"  DB size after vuln reload: {size_after_reload / 1_048_576:.1f} MB")

        # The compaction should keep size stable (within 1.5x)
        assert size_after_reload <= size_after_first * 1.5, (
            f"DB grew from {size_after_first} → {size_after_reload} bytes after reload "
            f"({size_after_reload / size_after_first:.1f}x). Compaction not working."
        )
        print(f"  ✓ Size ratio: {size_after_reload / size_after_first:.2f}x (≤1.5x)")

        # ==================================================================
        # PHASE 6: Verify policy survives vuln reload (bug regression)
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 6: Verifying policy data survives vulnerability reload")
        print("=" * 70)

        policy_rows_after = db.query("SELECT COUNT(*) AS cnt FROM policies")[0]["cnt"]
        assert policy_rows_after == policy_rows, (
            f"Policy rows lost after vuln reload: had {policy_rows}, now {policy_rows_after}"
        )
        print(f"  ✓ policies: {policy_rows_after:,} rows (unchanged)")

        remediation_rows_after = db.query("SELECT COUNT(*) AS cnt FROM vulnerability_remediation")[0]["cnt"]
        if remediation_rows > 0:
            assert remediation_rows_after == remediation_rows, (
                f"Remediation rows lost after vuln reload: had {remediation_rows}, now {remediation_rows_after}"
            )
            print(f"  ✓ vulnerability_remediation: {remediation_rows_after:,} rows (unchanged)")
        else:
            print("  - vulnerability_remediation: skipped (no data to verify)")

        # ==================================================================
        # PHASE 7: Tool coverage — query_rapid7
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 7: Tool coverage — SQL queries")
        print("=" * 70)

        # Aggregation query
        severity_dist = db.query("""
            SELECT severity, COUNT(*) AS cnt
            FROM vulnerabilities
            WHERE severity IS NOT NULL
            GROUP BY severity
            ORDER BY cnt DESC
        """)
        assert len(severity_dist) > 0, "Severity distribution query returned no rows"
        print(f"  ✓ Severity distribution: {len(severity_dist)} levels")
        for row in severity_dist[:5]:
            print(f"    {row['severity']}: {row['cnt']:,}")

        # Join query (assets + vulnerabilities)
        join_results = db.query("""
            SELECT a.hostName, COUNT(v.vulnId) AS vuln_count
            FROM assets a
            JOIN vulnerabilities v ON a.assetId = v.assetId
            WHERE a.hostName IS NOT NULL
            GROUP BY a.hostName
            ORDER BY vuln_count DESC
            LIMIT 5
        """)
        assert len(join_results) > 0, "Join query returned no rows"
        print(f"  ✓ Top assets by vuln count: {len(join_results)} rows")

        # Policy query
        policy_status = db.query("""
            SELECT finalStatus, COUNT(*) AS cnt
            FROM policies
            GROUP BY finalStatus
            ORDER BY cnt DESC
        """)
        assert len(policy_status) > 0, "Policy status query returned no rows"
        print(f"  ✓ Policy status distribution: {len(policy_status)} statuses")

        # Security: external access should be blocked
        with pytest.raises(ValueError, match="Query execution failed"):
            db.query("SELECT * FROM read_csv('/etc/passwd')")
        print("  ✓ External access blocked (read_csv refused)")

        # ==================================================================
        # PHASE 8: Tool coverage — get_rapid7_schema
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 8: Tool coverage — schema inspection")
        print("=" * 70)

        schema = db.get_schema()
        assert "vulnerabilities" in schema, "Schema missing 'vulnerabilities' table"
        assert "assets" in schema, "Schema missing 'assets' table"
        assert "policies" in schema, "Schema missing 'policies' table"
        if remediation_rows > 0:
            assert "vulnerability_remediation" in schema, "Schema missing 'vulnerability_remediation' table"

        vuln_columns = [c["column_name"] for c in schema["vulnerabilities"]]
        assert "vulnId" in vuln_columns, "vulnerabilities schema missing 'vulnId' column"
        assert "severity" in vuln_columns, "vulnerabilities schema missing 'severity' column"
        assert "cvssV3Score" in vuln_columns, "vulnerabilities schema missing 'cvssV3Score' column"
        print(f"  ✓ vulnerabilities: {len(schema['vulnerabilities'])} columns")

        policy_columns = [c["column_name"] for c in schema["policies"]]
        assert "finalStatus" in policy_columns, "policies schema missing 'finalStatus' column"
        assert "source" in policy_columns, "policies schema missing 'source' column"
        print(f"  ✓ policies: {len(schema['policies'])} columns")

        print(f"  ✓ Total tables in schema: {len(schema)}")

        # ==================================================================
        # PHASE 9: Tool coverage — get_rapid7_stats
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 9: Tool coverage — statistics")
        print("=" * 70)

        stats = db.get_stats()
        assert "vulnerabilities" in stats, "Stats missing 'vulnerabilities'"
        assert stats["vulnerabilities"]["total_rows"] > 0
        print(f"  ✓ vulnerabilities: {stats['vulnerabilities']['total_rows']:,} rows")

        if "severity_distribution" in stats["vulnerabilities"]:
            print(f"    severity_distribution: {stats['vulnerabilities']['severity_distribution']}")

        assert "assets" in stats, "Stats missing 'assets'"
        print(f"  ✓ assets: {stats['assets']['total_rows']:,} rows")

        assert "policies" in stats, "Stats missing 'policies'"
        print(f"  ✓ policies: {stats['policies']['total_rows']:,} rows")

        if remediation_rows > 0:
            assert "vulnerability_remediation" in stats, "Stats missing 'vulnerability_remediation'"
            print(f"  ✓ vulnerability_remediation: {stats['vulnerability_remediation']['total_rows']:,} rows")
        else:
            print("  - vulnerability_remediation: skipped (no data)")

        # ==================================================================
        # PHASE 10: Tool coverage — list_rapid7_exports (tracker)
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 10: Tool coverage — export tracker")
        print("=" * 70)

        # Update tracker with completion info
        tracker = ExportTracker(tracking_path)
        tracker.save_export(
            export_id=vuln_id,
            status="COMPLETE",
            parquet_urls=completed["vulnerability"]["parquetFiles"],
            row_count=sum(vuln_counts.values()),
            export_type="vulnerability",
        )
        tracker.save_export(
            export_id=policy_id,
            status="COMPLETE",
            parquet_urls=completed["policy"]["parquetFiles"],
            row_count=sum(policy_counts.values()),
            export_type="policy",
        )
        # Remediation chunks were already tracked during Phase 3's sequential loop.
        # Just update the parallel exports with completion info.

        exports = tracker.list_exports(limit=20)
        expected_count = 2 + len(remediation_chunks)
        assert len(exports) >= expected_count, f"Expected at least {expected_count} tracked exports, got {len(exports)}"
        print(f"  ✓ Tracked exports: {len(exports)}")

        export_types = {e.get("export_type", "unknown") for e in exports}
        assert "vulnerability" in export_types, "Tracker missing vulnerability export"
        assert "policy" in export_types, "Tracker missing policy export"
        assert "remediation" in export_types, "Tracker missing remediation export"
        print(f"  ✓ Export types tracked: {sorted(export_types)}")

        # Verify get_today_export works
        today_vuln = tracker.get_today_export(export_type="vulnerability")
        assert today_vuln is not None, "get_today_export returned None for today's vuln export"
        assert today_vuln["export_id"] == vuln_id
        print(f"  ✓ get_today_export: found {today_vuln['export_id']}")

        tracker.close()

        # ==================================================================
        # PHASE 11: Tool coverage — purge_rapid7_data
        # ==================================================================
        print("\n" + "=" * 70)
        print("PHASE 11: Tool coverage — purge")
        print("=" * 70)

        db.purge()
        assert not db.has_data(), "Database should be empty after purge"
        print("  ✓ purge: database cleared")

        # After purge, queries should fail
        with pytest.raises(ValueError):
            db.query("SELECT * FROM vulnerabilities")
        print("  ✓ purge: queries correctly fail on empty DB")

        # Tracker purge
        tracker = ExportTracker(tracking_path)
        tracker.purge()
        print("  ✓ purge: tracker cleared")

        # ==================================================================
        # Done
        # ==================================================================
        print("\n" + "=" * 70)
        print("ALL PHASES PASSED")
        print("=" * 70)
        print(f"  Vulnerability rows: {vuln_rows:,}")
        print(f"  Policy rows: {policy_rows:,}")
        print(f"  Remediation rows: {remediation_rows:,}")
        print(f"  Final DB size: {size_after_reload / 1_048_576:.1f} MB")
        print(f"  Compaction ratio: {size_after_reload / size_after_first:.2f}x")
