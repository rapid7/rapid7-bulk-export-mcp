#!/usr/bin/env python3
"""
FastMCP Server for Rapid7 Vulnerability Data

This server exposes vulnerability data through the Model Context Protocol,
allowing AI assistants to query and analyze the data.
"""

import datetime as _dt
import glob
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Optional

import duckdb as _duckdb
from fastmcp import FastMCP
from mcp.types import ToolAnnotations

from .config import load_config
from .download import download_all_files
from .duckdb_loader import VulnerabilityDatabase
from .export_manager import (
    ExportInProgressError,
    build_remediation_date_chunks,
    create_asset_software_export,
    create_policy_export,
    create_remediation_export,
    create_vulnerability_export,
    get_export_status,
    poll_until_complete,
)
from .export_tracker import ExportTracker

# Initialize FastMCP server
mcp = FastMCP("rapid7-bulk-export")

# Global database instance
db: Optional[VulnerabilityDatabase] = None

# Data directory — resolved once at startup, used for all database paths.
# Defaults to ~/.rapid7_mcp so relative-path writes never hit a read-only CWD.
_DATA_DIR: Path = Path(os.environ.get("DATA_DIR", "~/.rapid7_mcp")).expanduser().resolve()

VALID_EXPORT_TYPES = ("vulnerability", "policy", "remediation", "asset_software")

# ---------------------------------------------------------------------------
# Background download/load job tracking
#
# Loading a full export (potentially millions of rows) can take much longer
# than an MCP client's tool-call timeout — Claude Desktop, for example,
# hard-cancels a tool call after ~4 minutes. Running the download+load
# synchronously inside a single tool call means the client gives up and
# cancels while the server keeps working, and when the server later tries
# to respond to that already-cancelled request, some MCP client/session
# implementations raise on a duplicate response and crash the whole stdio
# server process.
#
# To avoid this, download_rapid7_export() only *starts* the work in a
# background thread and returns immediately. Progress and results are
# recorded as phase transitions on the durable ExportTracker row (not an
# in-memory dict), so state is durable and inspectable after a restart and is
# reported by the existing check_rapid7_export_status() and list_rapid7_exports()
# tools — there is no separate status tool to poll. Workers do not resume across
# a restart; interrupted work is reconciled to a retryable FAILED at startup.
# ---------------------------------------------------------------------------

# Local load-phase values written to the tracker's `status` column. These
# describe THIS server's download+load progress, distinct from the Rapid7
# platform-side export status returned by the API (PENDING/PROCESSING/…).
PHASE_DOWNLOADING = "DOWNLOADING"
PHASE_LOADING = "LOADING"
PHASE_COMPLETE = "COMPLETE"  # data loaded locally and queryable
PHASE_FAILED = "FAILED"

# States that mean a background load is actively touching the database, so a
# second download must not start and reads should back off.
_ACTIVE_PHASES = (PHASE_DOWNLOADING, PHASE_LOADING)

# Multi-chunk job status values (export_jobs.status). A job spans N per-chunk
# export IDs; these describe the job as a whole.
JOB_RUNNING = "RUNNING"
JOB_COMPLETE = "COMPLETE"
JOB_FAILED = "FAILED"  # at least one chunk failed; loaded chunks remain

# How long to wait between retries when the platform reports another export of
# the same type already in flight, and how long to keep retrying before giving
# up on a chunk.
_IN_PROGRESS_RETRY_SECS = 30
_IN_PROGRESS_MAX_WAIT_SECS = 20 * 60

# Guards all access to the shared `db` connection (both the background
# load and the read tools below) so a query can never run concurrently
# with a table being dropped/recreated mid-load. Re-entrant so a locked
# section can safely call another helper that also acquires it.
_db_lock = threading.RLock()


def _tracker() -> ExportTracker:
    """Open a tracker handle on the standard tracking database."""
    return ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))


def initialize_database(db_path: Optional[str] = None) -> VulnerabilityDatabase:
    """Initialize the vulnerability database."""
    global db
    if db is None:
        resolved = db_path or str(_DATA_DIR / "rapid7_bulk_export.db")
        db = VulnerabilityDatabase(resolved)
    return db


@mcp.tool(
    annotations=ToolAnnotations(
        title="Load Rapid7 Parquet File",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def load_rapid7_parquet(parquet_path: str) -> str:
    """Load vulnerability data from existing Parquet file(s).

    Use this if you already have Parquet files downloaded and want to skip
    the export process. This is much faster than running a full export.

    Args:
        parquet_path: Path to a Parquet file or directory containing Parquet files

    Returns:
        Summary of loaded data including row count and statistics.
    """
    global db

    try:
        ALLOWED_ROOT = (_DATA_DIR / "imports").resolve()

        # Resolve and validate path is within allowed root
        resolved = Path(parquet_path).resolve()
        try:
            resolved.relative_to(ALLOWED_ROOT)
        except ValueError:
            return (
                f"✗ Error: Path must be within {ALLOWED_ROOT}\n"
                f"Resolved path '{resolved}' is outside the allowed directory.\n"
                f"Please copy your Parquet files into {ALLOWED_ROOT} first."
            )

        # Check if path exists
        if not resolved.exists():
            return f"✗ Error: Path does not exist: {resolved}"

        # Get list of parquet files
        if resolved.is_file():
            parquet_files = [str(resolved)]
        else:
            parquet_files = glob.glob(str(resolved / "*.parquet"))

        if not parquet_files:
            return f"✗ Error: No Parquet files found at: {resolved}"

        if not _db_lock.acquire(blocking=False):
            return (
                "⏳ A background download/load is currently in progress. "
                "Try again shortly, or check "
                "check_rapid7_export_status(export_id=...) for progress."
            )
        try:
            # Initialize database if needed
            if db is None:
                initialize_database()

            # Detect file types by peeking at schema and build prefix map
            prefix_file_map: dict = {}
            for pf in parquet_files:
                try:
                    cols = [
                        desc[0]
                        for desc in _duckdb.execute(
                            f"SELECT * FROM read_parquet('{pf}') LIMIT 0"  # nosec B608
                        ).description
                    ]
                    if "vulnId" in cols or "checkId" in cols:
                        prefix_file_map.setdefault("asset_vulnerability", []).append(pf)
                    else:
                        prefix_file_map.setdefault("asset", []).append(pf)
                except Exception:
                    # If we can't determine type, skip the file
                    continue

            if not prefix_file_map:
                return f"✗ Error: Could not determine schema for any Parquet files at: {resolved}"

            # Load into database
            row_counts = db.load_parquet_files_by_prefix(prefix_file_map)
            row_count = sum(row_counts.values())

            # Get statistics
            stats = db.get_stats()
        finally:
            _db_lock.release()

        return (
            f"✓ Successfully loaded {row_count} rows from {len(parquet_files)} file(s).\n\n"
            f"Per-table row counts: {json.dumps(row_counts, default=str)}\n\n"
            f"Statistics:\n{json.dumps(stats, indent=2, default=str)}\n\n"
            f"You can now query the data using query_rapid7, get_rapid7_schema, or get_rapid7_stats tools."
        )

    except Exception as e:
        return f"✗ Error loading Parquet files: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Start Rapid7 Export",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def start_rapid7_export(
    export_type: str = "vulnerability",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Start a new Rapid7 export job (non-blocking).

    This is a fast, non-blocking call that creates an export job on the
    Rapid7 platform and returns the export ID immediately. The export
    will process in the background on Rapid7's servers (typically 3-5
    minutes).

    Use check_rapid7_export_status(export_id) to monitor progress, then
    download_rapid7_export(export_id, export_type="...") once it completes.

    If an export from today already exists, returns that export's ID
    instead of creating a duplicate.

    For remediation exports, the Rapid7 API limits each request to 31 days.
    If the date range exceeds 31 days, this tool automatically splits it
    into multiple 31-day chunks and kicks off an export for each chunk.

    Args:
        export_type: Type of export to create. One of "vulnerability",
                     "policy", or "remediation".
        start_date: Start date in YYYY-MM-DD format (only for remediation exports).
                    Defaults to 30 days ago if not specified.
        end_date: End date in YYYY-MM-DD format (only for remediation exports).
                  Defaults to today if not specified.

    Returns:
        The export ID and next steps.
    """
    if export_type not in VALID_EXPORT_TYPES:
        return f"✗ Invalid export_type: '{export_type}'. Valid values are: {', '.join(VALID_EXPORT_TYPES)}"

    try:
        config = load_config()

        tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))

        # Return a cached export from today unless it's remediation (which is date-range keyed)
        today_export = tracker.get_today_export(export_type=export_type)
        if today_export and export_type != "remediation":
            tracker.close()
            eid = today_export["export_id"]
            return (
                f"♻️ A {export_type} export from today already exists.\n\n"
                f"Export ID: {eid}\n"
                f"Status: COMPLETE\n"
                f"Created: {today_export['created_at']}\n"
                f"Rows: {today_export['row_count']}\n\n"
                f"Load it with: "
                f"download_rapid7_export("
                f'export_id="{eid}", '
                f'export_type="{export_type}")'
            )

        # Create the export based on type
        if export_type == "vulnerability":
            print("Creating new vulnerability export...", file=sys.stderr)
            new_id = create_vulnerability_export(config)
            print(f"Created {export_type} export with ID: {new_id}", file=sys.stderr)
            tracker.save_export(export_id=new_id, status="PENDING", parquet_urls=[], export_type=export_type)
            tracker.close()

            return (
                f"✓ Vulnerability export job created.\n\n"
                f"Export ID: {new_id}\n"
                f"Status: PENDING\n\n"
                f"The export is now processing on Rapid7's servers "
                f"(typically 3-5 minutes).\n"
                f'Check progress: check_rapid7_export_status(export_id="{new_id}")\n'
                f"Once COMPLETE, load with: "
                f'download_rapid7_export(export_id="{new_id}", export_type="vulnerability")'
            )

        elif export_type == "policy":
            print("Creating new policy export...", file=sys.stderr)
            new_id = create_policy_export(config)
            print(f"Created {export_type} export with ID: {new_id}", file=sys.stderr)
            tracker.save_export(export_id=new_id, status="PENDING", parquet_urls=[], export_type=export_type)
            tracker.close()

            return (
                f"✓ Policy export job created.\n\n"
                f"Export ID: {new_id}\n"
                f"Status: PENDING\n\n"
                f"The export is now processing on Rapid7's servers "
                f"(typically 3-5 minutes).\n"
                f'Check progress: check_rapid7_export_status(export_id="{new_id}")\n'
                f"Once COMPLETE, load with: "
                f'download_rapid7_export(export_id="{new_id}", export_type="policy")'
            )

        elif export_type == "remediation":
            if not start_date:
                start_date = (_dt.date.today() - _dt.timedelta(days=30)).isoformat()
            if not end_date:
                end_date = _dt.date.today().isoformat()

            # A remediation range may span multiple ≤31-day windows, and the
            # platform allows only one remediation export in flight at a time.
            # Hand the whole range to a single background job that creates,
            # polls, downloads, and loads each window sequentially and appends
            # them into vulnerability_remediation. The caller polls one job id.
            tracker.close()
            chunk_ranges = build_remediation_date_chunks(start_date, end_date)
            job_id = _start_remediation_job(start_date, end_date)

            return (
                f"▶️ Started loading remediation data for {start_date} → {end_date} "
                f"in the background.\n\n"
                f"Job ID: {job_id}\n"
                f"Windows: {len(chunk_ranges)} (each ≤31 days, loaded sequentially)\n\n"
                f"The platform allows only one remediation export at a time, so windows "
                f"are processed one after another; this can take several minutes each.\n"
                f"All windows append into the same vulnerability_remediation table.\n\n"
                f"Check progress with: "
                f'check_rapid7_export_status(export_id="{job_id}")'
            )

        elif export_type == "asset_software":
            new_id = create_asset_software_export(config)
            print(f"Created asset_software export with ID: {new_id}", file=sys.stderr)
            tracker.save_export(export_id=new_id, status="PENDING", parquet_urls=[], export_type="asset_software")
            tracker.close()

            return (
                f"✓ Asset software export job created.\n\n"
                f"Export ID: {new_id}\n"
                f"Status: PENDING\n\n"
                f"The export is now processing on Rapid7's servers "
                f"(typically 3-5 minutes).\n"
                f'Check progress: check_rapid7_export_status(export_id="{new_id}")\n'
                f"Once COMPLETE, load with: "
                f'download_rapid7_export(export_id="{new_id}", export_type="asset_software")'
            )

    except Exception as e:
        return f"✗ Error starting {export_type} export: {str(e)}"


@mcp.tool(
    annotations=ToolAnnotations(
        title="Check Rapid7 Export Status",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def check_rapid7_export_status(export_id: str) -> str:
    """Check the status of a Rapid7 export, including local download/load progress.

    Fast, non-blocking call. Reports whichever stage the export is in:

      - While this server is downloading or loading the export in the
        background, reports that local phase and per-file progress — and
        skips the Rapid7 API call, since the platform-side export is
        already known to be complete.
      - Once loaded (COMPLETE) or failed (FAILED) locally, returns the
        stored summary/error so no work is repeated.
      - Otherwise queries the Rapid7 API once for the platform-side export
        status (PENDING/PROCESSING/COMPLETE/FAILED).

    Because local phase lives in the durable tracker, it is inspectable after
    a server restart (interrupted work is reconciled to a retryable FAILED at
    startup rather than resumed). Does NOT poll or wait.

    Args:
        export_id: The export ID returned by start_rapid7_export, or a
            job ID returned by a multi-window remediation load.

    Returns:
        Current export/load status and next steps.
    """
    try:
        # A multi-window remediation load is tracked as a job spanning N export
        # IDs; if the id names a job, report the job's progress.
        tracker = _tracker()
        job = tracker.get_job(export_id)
        if job is not None:
            tracker.close()
            return _format_job_status(job)

        # Local load phase takes precedence: if a background download/load is
        # active or has reached a terminal state, report that and skip the
        # (now-redundant) platform-side status call.
        local = tracker.get_export_by_id(export_id)
        tracker.close()

        if local is not None:
            local_status = local.get("status")

            if local_status == PHASE_COMPLETE:
                return local.get("message") or (
                    f"✓ Data loaded and queryable.\n\nExport ID: {export_id}\nRows: {local.get('row_count')}"
                )
            if local_status == PHASE_FAILED:
                return local.get("message") or f"✗ Download/load failed.\n\nExport ID: {export_id}"
            if local_status in _ACTIVE_PHASES:
                detail = local.get("phase_detail")
                detail_line = f"\n{detail}" if detail else ""
                return (
                    f"⏳ Downloading/loading in progress locally ({local_status}).{detail_line}\n\n"
                    f"Export ID: {export_id}\n"
                    f"Last update: {local.get('updated_at') or local.get('created_at', 'unknown')}\n\n"
                    f"Check again in 30-60 seconds with: "
                    f'check_rapid7_export_status(export_id="{export_id}")'
                )
            # PENDING or any other value falls through to the platform-side check.

        config = load_config()
        status_info = get_export_status(config, export_id)
        current_status = status_info["status"]
        file_count = len(status_info.get("parquetFiles", []))

        if current_status in ["COMPLETE", "SUCCEEDED"]:
            return (
                f"✓ Export is complete on Rapid7's side and ready to download.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: {current_status}\n"
                f"Files ready: {file_count}\n\n"
                f"Load the data with: "
                f"download_rapid7_export("
                f'export_id="{export_id}", '
                f'export_type="...")'
            )
        elif current_status == "FAILED":
            return (
                f"✗ Export failed.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: FAILED\n\n"
                f"Start a new export with: start_rapid7_export()"
            )
        else:
            return (
                f"⏳ Export still processing.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: {current_status}\n\n"
                f"Check again in 30-60 seconds with: "
                f"check_rapid7_export_status("
                f'export_id="{export_id}")'
            )

    except Exception as e:
        return f"✗ Error checking export status: {str(e)}"


def _download_and_load_files(
    export_type: str,
    status_info: dict,
    api_key: str,
    on_downloaded=None,
) -> tuple:
    """Download an export's parquet files and load them into DuckDB.

    Shared by the single-export worker and the multi-chunk remediation
    orchestrator so the download/route/load path is not forked. Acquires
    _db_lock around the load. If provided, on_downloaded() is called once
    the files are actually downloaded and before the load begins, so a
    caller can record the LOADING phase honestly. Returns (row_count,
    row_counts, stats, validation_warnings).
    """
    global db

    parquet_urls = status_info["parquetFiles"]
    file_data = download_all_files(parquet_urls, api_key)
    if on_downloaded is not None:
        on_downloaded()

    temp_dir = tempfile.mkdtemp()
    validation_warnings: list = []
    try:
        with _db_lock:
            if db is None:
                initialize_database()

            result_list = status_info.get("result") or []
            url_to_prefix = {}
            for item in result_list:
                prefix = item.get("prefix", "")
                for url in item.get("urls", []):
                    url_to_prefix[url] = prefix

            prefix_file_map: dict = {}
            for i, (url, data) in enumerate(zip(parquet_urls, file_data)):
                temp_path = Path(temp_dir) / f"{export_type}_export_{i}.parquet"
                temp_path.write_bytes(data)
                prefix = url_to_prefix.get(url, "unknown")
                prefix_file_map.setdefault(prefix, []).append(str(temp_path))
                if len(data) < 100:
                    validation_warnings.append(f"File {i + 1} (prefix={prefix}): unusually small ({len(data)} bytes)")

            if export_type == "policy":
                row_counts = db.load_parquet_files_by_prefix(prefix_file_map, skip_prefixes={"asset"})
            elif export_type == "remediation":
                row_counts = db.load_parquet_files_by_prefix(prefix_file_map, append=True)
            else:
                row_counts = db.load_parquet_files_by_prefix(prefix_file_map)

            row_count = sum(row_counts.values())
            if row_count == 0 and len(file_data) > 0:
                validation_warnings.append(
                    f"⚠️  {len(file_data)} file(s) downloaded but 0 rows loaded. "
                    f"Prefixes received: {list(prefix_file_map.keys())}. "
                    f"Check that prefixes match expected routing."
                )

            stats = db.get_stats()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return row_count, row_counts, stats, validation_warnings


def _run_download_and_load(export_id: str, export_type: str) -> None:
    """Background worker: download parquet files and load them into DuckDB.

    Runs in its own thread. All progress/results are written to the
    ExportTracker row for this export_id rather than returned, since
    nothing is waiting on a function return here. Any exception is caught
    and recorded as a FAILED phase (with the error text) so the status
    tools can report it, instead of the process crashing on a late/
    duplicate response the way the synchronous version could when a client
    had already timed out and cancelled the request.
    """
    tracker = _tracker()
    try:
        config = load_config()
        status_info = get_export_status(config, export_id)
        parquet_urls = status_info["parquetFiles"]

        tracker.set_phase(
            export_id,
            PHASE_DOWNLOADING,
            phase_detail=f"downloading {len(parquet_urls)} file(s)",
        )
        print(f"Downloading {len(parquet_urls)} {export_type} files...", file=sys.stderr)

        def _mark_loading() -> None:
            tracker.set_phase(
                export_id,
                PHASE_LOADING,
                phase_detail=f"{len(parquet_urls)} file(s) downloaded, loading into database",
            )

        row_count, row_counts, stats, validation_warnings = _download_and_load_files(
            export_type, status_info, config["api_key"], on_downloaded=_mark_loading
        )
        row_info = f"Rows loaded: {row_count}\nPer-table row counts: {json.dumps(row_counts, default=str)}"

        warnings_section = ""
        if validation_warnings:
            warnings_section = "\nValidation Warnings:\n" + "\n".join(f"  {w}" for w in validation_warnings) + "\n"

        message = (
            f"✓ {export_type.capitalize()} data loaded successfully.\n\n"
            f"Export ID: {export_id}\n"
            f"Files processed: {len(parquet_urls)}\n"
            f"{row_info}\n"
            f"{warnings_section}\n"
            f"Statistics:\n"
            f"{json.dumps(stats, indent=2, default=str)}\n\n"
            f"Query the data with query_rapid7, get_rapid7_schema, or get_rapid7_stats."
        )
        # Record the completed load with its parquet URLs and final row count.
        tracker.save_export(
            export_id=export_id,
            status=PHASE_COMPLETE,
            parquet_urls=parquet_urls,
            row_count=row_count,
            export_type=export_type,
        )
        tracker.set_phase(export_id, PHASE_COMPLETE, phase_detail=None, message=message, row_count=row_count)

    except Exception as e:
        error_text = f"{e}\n{traceback.format_exc()}"
        message = (
            f"✗ Error downloading/loading {export_type}: {str(e)}\n\n"
            f"Export ID: {export_id}\n"
            f"Retry with: download_rapid7_export("
            f'export_id="{export_id}", '
            f'export_type="{export_type}")\n\n'
            f"{error_text}"
        )
        tracker.set_phase(export_id, PHASE_FAILED, phase_detail=None, message=message)
    finally:
        tracker.close()


def _create_remediation_chunk_waiting(config: dict, chunk_start: str, chunk_end: str) -> str:
    """Create one remediation chunk, waiting out any foreign in-flight export.

    The platform permits only one remediation export in flight at a time. If
    another is running (this job's previous chunk, or an unrelated export),
    create is rejected with ExportInProgressError. We must NOT adopt that
    foreign id — it may cover a different date range — so we back off and
    recreate THIS chunk's own range until the platform frees up.
    """
    deadline = time.monotonic() + _IN_PROGRESS_MAX_WAIT_SECS
    while True:
        try:
            return create_remediation_export(config, chunk_start, chunk_end)
        except ExportInProgressError:
            if time.monotonic() >= deadline:
                raise
            print(
                f"Remediation export slot busy; waiting to create {chunk_start} → {chunk_end}...",
                file=sys.stderr,
            )
            time.sleep(_IN_PROGRESS_RETRY_SECS)


def _run_remediation_job(job_id: str, chunks: list) -> None:
    """Background worker: load a multi-window remediation range as one job.

    Processes each ≤31-day window strictly sequentially (create → poll →
    download → load append), because the platform serialises remediation
    exports anyway. Per-chunk outcome is persisted on the export_jobs row so a
    partial failure names exactly which windows loaded and which did not.
    """
    tracker = _tracker()
    try:
        config = load_config()
        total = len(chunks)
        total_rows = 0

        for i, chunk in enumerate(chunks):
            cs, ce = chunk["start"], chunk["end"]
            window = f"{cs} → {ce}"

            def _mark(state: str) -> None:
                chunks[i]["status"] = state
                tracker.update_job(
                    job_id,
                    current_index=i,
                    chunks=chunks,
                    message=f"chunk {i + 1}/{total} ({window}), {state}",
                )

            _mark("creating")
            eid = _create_remediation_chunk_waiting(config, cs, ce)
            chunks[i]["export_id"] = eid

            _mark("waiting for export")
            parquet_urls = poll_until_complete(config, eid)
            status_info = get_export_status(config, eid)
            if not parquet_urls:
                status_info["parquetFiles"] = status_info.get("parquetFiles", [])

            row_count, _, _, _ = _download_and_load_files(
                "remediation", status_info, config["api_key"], on_downloaded=lambda: _mark("loading")
            )
            # Mark loaded the instant the append has committed, BEFORE any
            # further tracker writes. If save_export below throws, the window
            # is already recorded loaded so the failure report can never tell
            # the user to re-run a window whose rows are already present.
            total_rows += row_count
            chunks[i]["row_count"] = row_count
            _mark("loaded")
            tracker.save_export(
                export_id=eid,
                status=PHASE_COMPLETE,
                parquet_urls=parquet_urls,
                row_count=row_count,
                export_type="remediation",
            )

        loaded = [f"{c['start']} → {c['end']} ({c.get('row_count', 0)} rows)" for c in chunks]
        message = (
            f"✓ Remediation data loaded for all {total} window(s).\n\n"
            f"Total rows: {total_rows}\n"
            f"Windows loaded:\n" + "\n".join(f"  {w}" for w in loaded) + "\n\n"
            "Query the data with query_rapid7, get_rapid7_schema, or get_rapid7_stats."
        )
        tracker.update_job(job_id, status=JOB_COMPLETE, current_index=total - 1, chunks=chunks, message=message)

    except Exception as e:
        error_text = f"{e}\n{traceback.format_exc()}"
        loaded = [f"{c['start']} → {c['end']}" for c in chunks if c.get("status") == "loaded"]
        missing = [f"{c['start']} → {c['end']}" for c in chunks if c.get("status") != "loaded"]
        message = (
            f"✗ Remediation load failed partway through.\n\n"
            f"Loaded windows (kept): {', '.join(loaded) or 'none'}\n"
            f"Missing windows: {', '.join(missing) or 'none'}\n\n"
            f"Re-run only the missing range with start_rapid7_export("
            f'export_type="remediation", start_date="...", end_date="...").\n\n'
            f"{error_text}"
        )
        tracker.update_job(job_id, status=JOB_FAILED, chunks=chunks, message=message)
    finally:
        tracker.close()


def _start_remediation_job(start_date: str, end_date: str) -> str:
    """Create and launch a multi-window remediation load job. Returns the job_id."""
    chunk_ranges = build_remediation_date_chunks(start_date, end_date)
    chunks = [{"start": cs, "end": ce, "export_id": None, "status": "pending"} for cs, ce in chunk_ranges]

    job_id = f"remediation-{uuid.uuid4().hex[:12]}"
    tracker = _tracker()
    tracker.create_job(
        job_id=job_id,
        export_type="remediation",
        start_date=start_date,
        end_date=end_date,
        chunks=chunks,
        status=JOB_RUNNING,
    )
    tracker.close()

    thread = threading.Thread(target=_run_remediation_job, args=(job_id, chunks), daemon=True)
    thread.start()
    return job_id


def _format_job_status(job: dict) -> str:
    """Render an export_jobs row for check_rapid7_export_status."""
    status = job.get("status")
    if status == JOB_COMPLETE:
        return job.get("message") or f"✓ Remediation job {job['job_id']} complete."
    if status == JOB_FAILED:
        return job.get("message") or f"✗ Remediation job {job['job_id']} failed."

    total = len(job.get("chunks") or [])
    return (
        f"⏳ Remediation load in progress.\n\n"
        f"Job ID: {job['job_id']}\n"
        f"Range: {job.get('start_date')} → {job.get('end_date')} ({total} window(s))\n"
        f"{job.get('message', '')}\n\n"
        f"Check again in 30-60 seconds with: "
        f'check_rapid7_export_status(export_id="{job["job_id"]}")'
    )


@mcp.tool(
    annotations=ToolAnnotations(
        title="Download Rapid7 Export",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def download_rapid7_export(export_id: str, export_type: str = "vulnerability") -> str:
    """Start downloading a completed Rapid7 export and loading it into the database.

    Call this after check_rapid7_export_status confirms the export is COMPLETE.
    This kicks off the download and load in the background and returns
    immediately — large exports (potentially millions of rows) can take
    several minutes to load, longer than most MCP clients will wait on a
    single tool call. Poll progress with check_rapid7_export_status(export_id).

    Args:
        export_id: The export ID of a completed export.
        export_type: Type of export. One of "vulnerability", "policy",
                     "remediation", or "asset_software".

    Returns:
        Confirmation that the background job has started, plus how to
        check on it.
    """
    if export_type not in VALID_EXPORT_TYPES:
        return f"✗ Invalid export_type: '{export_type}'. Valid values are: {', '.join(VALID_EXPORT_TYPES)}"

    try:
        config = load_config()

        # Quick call — just confirms the export is ready, doesn't download anything.
        status_info = get_export_status(config, export_id)
        current_status = status_info["status"]

        if current_status not in ["COMPLETE", "SUCCEEDED"]:
            return (
                f"✗ Export is not yet complete.\n\n"
                f"Export ID: {export_id}\n"
                f"Status: {current_status}\n\n"
                f"Check again with: "
                f"check_rapid7_export_status("
                f'export_id="{export_id}")'
            )

        if not status_info["parquetFiles"]:
            return f"✗ Export complete but has no files.\n\nExport ID: {export_id}"

        # Don't start a second job for the same export if one's already running.
        tracker = _tracker()
        existing = tracker.get_export_by_id(export_id)
        if existing is not None and existing.get("status") in _ACTIVE_PHASES:
            tracker.close()
            return (
                f"⏳ A download for this export is already in progress "
                f"(status: {existing['status']}).\n\n"
                f"Export ID: {export_id}\n"
                f'Check progress with: check_rapid7_export_status(export_id="{export_id}")'
            )

        # Record the initial phase durably. The row usually already exists at
        # PENDING (start_rapid7_export inserts it); save_export upserts so a
        # directly-supplied export_id is tracked too.
        parquet_urls = status_info["parquetFiles"]
        tracker.save_export(
            export_id=export_id,
            status=PHASE_DOWNLOADING,
            parquet_urls=parquet_urls,
            export_type=export_type,
        )
        tracker.set_phase(
            export_id,
            PHASE_DOWNLOADING,
            phase_detail=f"0 / {len(parquet_urls)} files downloaded",
        )
        tracker.close()

        thread = threading.Thread(
            target=_run_download_and_load,
            args=(export_id, export_type),
            daemon=True,
        )
        thread.start()

        return (
            f"▶️ Started downloading and loading {export_type} export in the background.\n\n"
            f"Export ID: {export_id}\n"
            f"Files: {len(parquet_urls)}\n\n"
            f"This can take several minutes for large exports. Check progress with:\n"
            f'check_rapid7_export_status(export_id="{export_id}")'
        )

    except Exception as e:
        return (
            f"✗ Error starting download for {export_type}: {str(e)}\n\n"
            f"Export ID: {export_id}\n"
            f"Retry with: download_rapid7_export("
            f'export_id="{export_id}", '
            f'export_type="{export_type}")'
        )


@mcp.tool(
    annotations=ToolAnnotations(
        title="Query Rapid7 Data",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def query_rapid7(sql: str) -> str:
    """Execute a SQL query against the Rapid7 database.

    The database contains the following tables loaded from Rapid7 InsightVM
    Bulk Export API Parquet files:

    **assets** — Asset inventory data:
      Key fields: orgId, assetId, agentId, hostName, ip, mac, osFamily,
      osProduct, osVersion, osDescription, riskScore, sites, assetGroups, tags,
      awsInstanceId, azureResourceId, gcpObjectId

    **vulnerabilities** — Combined asset + vulnerability data:
      Key fields: orgId, assetId, vulnId, checkId, port, protocol, title,
      description, severity, severityRank, cvssScore, cvssV3Score,
      cvssV3Severity, hasExploits, epssscore, epsspercentile, riskScoreV2_0,
      cves, firstFoundTimestamp, reintroducedTimestamp, dateAdded,
      dateModified, datePublished, pciCompliant, pciSeverity

    **vulnerability_exceptions** — Vulnerability exceptions (waived/accepted risk):
      Key fields: orgId, assetId, vulnId, checkId, key, port, protocol,
      nic, proof, firstFoundTimestamp, reintroducedTimestamp, exceptionDetails

    **policies** — Policy compliance results (agent and scan based):
      Key fields: orgId, assetId, benchmarkNaturalId, profileNaturalId,
      benchmarkVersion, ruleNaturalId, ruleTitle, finalStatus, proof,
      lastAssessmentTimestamp, benchmarkTitle, profileTitle, publisher,
      fixTexts, rationales, source ('agent' or 'scan')

    **vulnerability_remediation** — Vulnerability remediation tracking:
      Key fields: orgId, assetId, cveId, vulnId, proof, firstFoundTimestamp,
      reintroducedTimestamp, lastDetected, lastRemoved, title, description,
      cvssV2Score, cvssV3Score, cvssV2Severity, cvssV3Severity,
      cvssV2AttackVector, cvssV3AttackVector, riskScoreV2_0, datePublished,
      dateAdded, dateModified, epssscore, epsspercentile

    Use this tool to query any of the above tables. You can filter, aggregate,
    join across tables, or perform any SQL-based analysis supported by DuckDB.

    Examples:
    - SELECT * FROM vulnerabilities WHERE severity = 'Critical' LIMIT 10
    - SELECT severity, COUNT(*) FROM vulnerabilities GROUP BY severity
    - SELECT * FROM policies WHERE finalStatus = 'fail' LIMIT 10
    - SELECT cveId, COUNT(*) FROM vulnerability_remediation GROUP BY cveId

    Args:
        sql: SQL query to execute against the database

    Returns:
        Query results as formatted JSON
    """
    global db

    # Acquire the lock BEFORE touching db at all — has_data() opens its own
    # DuckDB connection, which conflicts with a background load's read-write
    # connection. Locking first turns that into a clean busy response.
    if not _db_lock.acquire(blocking=False):
        return (
            "⏳ A background download/load is currently in progress, so the "
            "database can't be safely queried right now. Try again shortly, "
            "or check check_rapid7_export_status(export_id=...) for progress."
        )
    try:
        if db is None or not db.has_data():
            return "Error: No data loaded. Please run start_rapid7_export and download_rapid7_export first."
        results = db.query(sql)
        result_text = json.dumps(results, indent=2, default=str)
        return f"Query executed successfully. {len(results)} rows returned.\n\n{result_text}"
    except Exception as e:
        return f"Error executing query: {str(e)}"
    finally:
        _db_lock.release()


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 Schema",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def get_rapid7_schema() -> str:
    """Get the schema of all database tables.

    Returns column names and data types for all existing tables:
    assets, vulnerabilities, policies, and vulnerability_remediation.
    Tables that have not been loaded yet are omitted.

    Use this to understand what data is available before writing queries.

    Returns:
        Table schemas as formatted JSON, keyed by table name
    """
    global db

    if not _db_lock.acquire(blocking=False):
        return (
            "⏳ A background download/load is currently in progress, so the "
            "schema can't be safely read right now. Try again shortly, or "
            "check check_rapid7_export_status(export_id=...) for progress."
        )
    try:
        if db is None or not db.has_data():
            return "Error: No data loaded. Please run start_rapid7_export and download_rapid7_export first."
        schema = db.get_schema()
        schema_text = json.dumps(schema, indent=2)
        return f"Database schema:\n\n{schema_text}"
    except Exception as e:
        return f"Error getting schema: {str(e)}"
    finally:
        _db_lock.release()


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Rapid7 Statistics",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def get_rapid7_stats() -> str:
    """Get summary statistics for all database tables.

    Returns row counts and relevant distributions for all existing tables:
    assets, vulnerabilities, policies, and vulnerability_remediation.
    Tables that have not been loaded yet are omitted.

    Useful for getting an overview of the data across all loaded datasets.

    Returns:
        Summary statistics as formatted JSON, keyed by table name
    """
    global db

    if not _db_lock.acquire(blocking=False):
        return (
            "⏳ A background download/load is currently in progress, so "
            "statistics can't be safely read right now. Try again shortly, "
            "or check check_rapid7_export_status(export_id=...) for progress."
        )
    try:
        if db is None or not db.has_data():
            return "Error: No data loaded. Please run start_rapid7_export and download_rapid7_export first."
        stats = db.get_stats()
        stats_text = json.dumps(stats, indent=2, default=str)
        return f"Database statistics:\n\n{stats_text}"
    except Exception as e:
        return f"Error getting statistics: {str(e)}"
    finally:
        _db_lock.release()


@mcp.tool(
    annotations=ToolAnnotations(
        title="Purge Rapid7 Data",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def purge_rapid7_data() -> str:
    """Permanently delete all local Rapid7 data and tracking databases.

    This removes:
    - The main vulnerability database (rapid7_bulk_export.db)
    - The export tracking database (rapid7_bulk_export_tracking.db)
    - Any associated WAL files

    Use this when you are done with your analysis session, before handing
    off a machine, or to free disk space. After purging, you will need to
    run a new export to query data again.

    Returns:
        Confirmation of purged data.
    """
    global db

    # Refuse to purge while a background load holds the database — dropping the
    # file mid-load would corrupt the in-flight load and leave stale tracker
    # rows. Non-blocking to match the read tools.
    if not _db_lock.acquire(blocking=False):
        return (
            "⏳ A background download/load is currently in progress, so the "
            "database can't be purged right now. Try again once it finishes "
            "(check_rapid7_export_status(export_id=...) shows progress)."
        )
    try:
        # Even with the lock, a job can be between windows (downloading/polling)
        # while the lock is momentarily free. Refuse if any durable export or
        # job is still active, so a purge can't delete data a worker will then
        # repopulate under a deleted tracker row.
        active_tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))
        try:
            if active_tracker.has_active_work(
                active_export_statuses=list(_ACTIVE_PHASES),
                active_job_statuses=[JOB_RUNNING],
            ):
                return (
                    "⏳ A background download/load or multi-window remediation job "
                    "is still active, so the database can't be purged right now. "
                    "Wait until check_rapid7_export_status(export_id=...) reports it "
                    "finished, then purge."
                )
        finally:
            active_tracker.close()

        # Purge main database
        if db is not None:
            db.purge()

        # Purge tracking database (also clears all export/phase rows)
        tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))
        tracker.purge()

        return (
            "✓ All local Rapid7 data has been purged.\n\n"
            "Deleted:\n"
            "  - Vulnerability database (rapid7_bulk_export.db)\n"
            "  - Export tracking database (rapid7_bulk_export_tracking.db)\n\n"
            "To load new data, run start_rapid7_export() followed by download_rapid7_export()."
        )

    except Exception as e:
        return f"✗ Error purging data: {str(e)}"
    finally:
        _db_lock.release()


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Rapid7 Exports",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def list_rapid7_exports(limit: int = 10) -> str:
    """List recent Rapid7 exports tracked in the system.

    Shows export metadata including export ID, date, status, type, and row counts.
    Useful for understanding what exports are available for reuse.

    Args:
        limit: Maximum number of exports to return (default: 10)

    Returns:
        Formatted list of recent exports
    """
    try:
        # Reads only the local tracker DB (not the shared query database), so
        # it needs no _db_lock guard and is safe to call during a load — that
        # is in fact how you watch a background load progress.
        tracker = ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))
        exports = tracker.list_exports(limit=limit)
        tracker.close()

        if not exports:
            return "No exports found in the tracker database."

        result = f"Recent Exports (showing up to {limit}):\n\n"
        for exp in exports:
            result += f"Export ID: {exp['export_id']}\n"
            result += f"  Type: {exp.get('export_type', 'vulnerability')}\n"
            result += f"  Date: {exp['export_date']}\n"
            result += f"  Created: {exp['created_at']}\n"
            result += f"  Status: {exp['status']}\n"
            if exp.get("phase_detail"):
                result += f"  Progress: {exp['phase_detail']}\n"
            result += f"  Files: {exp['file_count']}\n"
            result += f"  Rows: {exp['row_count']}\n\n"

        return result

    except Exception as e:
        return f"✗ Error listing exports: {str(e)}"


def main():
    """Entry point for the MCP server command."""
    # Handle help flag
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print("Usage: rapid7-mcp-server [database_path]")
        print()
        print("Start the MCP server for Rapid7 vulnerability data.")
        print()
        print("Arguments:")
        print("  database_path    Path to the DuckDB database file (optional, overrides DATA_DIR default)")
        print()
        print("Environment Variables:")
        print("  RAPID7_API_KEY    Your Rapid7 InsightVM API key (required)")
        print("  RAPID7_REGION     Your Rapid7 region: us, eu, ca, au, or ap (required)")
        print("  DATA_DIR          Directory for database files (default: ~/.rapid7_mcp)")
        print("  MCP_TRANSPORT     Transport protocol: 'stdio' (default) or 'http'")
        print("  MCP_HOST          HTTP bind address (default: 0.0.0.0)")
        print("  MCP_PORT          HTTP port (default: 8000)")
        print()
        print("Example:")
        print("  rapid7-mcp-server /path/to/rapid7_bulk_export.db")
        print()
        print("The server communicates via stdio by default, or streamable HTTP")
        print("when MCP_TRANSPORT=http (for Docker / remote deployments).")
        print()
        print("See README.md for configuration details.")
        sys.exit(0)

    # Ensure data directory exists
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Get database path from args or use default
    db_path = sys.argv[1] if len(sys.argv) > 1 else str(_DATA_DIR / "rapid7_bulk_export.db")

    # Initialize database
    try:
        initialize_database(db_path)
        print(f"Initialized database from: {db_path}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Could not initialize database: {e}", file=sys.stderr)
        print("Database will be created when data is loaded.", file=sys.stderr)

    # Reconcile any work left mid-flight by a previous crash/restart. Background
    # workers are daemon threads with no resume, so a stuck DOWNLOADING/LOADING
    # export or RUNNING job would otherwise block retry forever.
    try:
        recon_tracker = _tracker()
        recon_tracker.reconcile_interrupted(
            active_export_statuses=list(_ACTIVE_PHASES),
            active_job_statuses=[JOB_RUNNING],
            reason="Interrupted by a server restart before completing. Re-run the export/range to retry.",
        )
        recon_tracker.close()
    except Exception as e:
        print(f"Warning: could not reconcile interrupted exports: {e}", file=sys.stderr)

    # Determine transport mode from environment
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport == "http":
        host = os.environ.get("MCP_HOST", "0.0.0.0")  # nosec B104 - intentional for Docker
        port = int(os.environ.get("MCP_PORT", "8000"))
        print(f"Starting HTTP transport on {host}:{port}", file=sys.stderr)
        mcp.run(transport="http", host=host, port=port, show_banner=False)
    else:
        mcp.run(show_banner=False)


if __name__ == "__main__":
    main()
