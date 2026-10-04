#!/usr/bin/env python3
"""Synchronous export orchestration for Rapid7 vulnerability data.

The create → poll → download → load pipeline lives here rather than in the MCP
server so it can be driven two ways from one implementation:

  - The MCP tools spawn a thread around it and return a job id immediately,
    because a hosted connector cancels a tool call after ~100s.
  - A scheduled job calls it in the foreground — single process, no threads —
    because a batch job has no such budget and wants a real exit code.

Calling the MCP tools from a job would appear to work (FastMCP leaves decorated
functions directly callable) but is exactly wrong: ``start_rapid7_export``
returns a PENDING id and ``download_rapid7_export`` hands the work to a daemon
thread that dies when the main thread exits, so a job would terminate mid-write
and could leave a partial database.

Process-level dependencies the caller owns — the platform API functions, the
shared database handle and its lock, the data directory, and retry timing — are
injected through :class:`OrchestratorContext` so this module never reaches back
into the server. That keeps it unit-testable without FastMCP and lets a caller
build into a database path of its choosing rather than mutating the live one.
"""

import datetime as _dt
import json
import shutil
import sys
import tempfile
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from .config import load_config as _load_config
from .config import redact_secret as _redact_secret
from .download import download_all_files as _download_all_files
from .duckdb_loader import VulnerabilityDatabase
from .export_manager import (
    ExportInProgressError,
    build_remediation_date_chunks,
)
from .export_manager import (
    create_asset_software_export as _create_asset_software_export,
)
from .export_manager import (
    create_policy_export as _create_policy_export,
)
from .export_manager import (
    create_remediation_export as _create_remediation_export,
)
from .export_manager import (
    create_vulnerability_export as _create_vulnerability_export,
)
from .export_manager import (
    get_export_status as _get_export_status,
)
from .export_manager import (
    poll_until_complete as _poll_until_complete,
)
from .export_tracker import ExportTracker

# Local load-phase values written to the tracker's `status` column. These
# describe THIS process's download+load progress, distinct from the Rapid7
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

# Export types a full refresh can produce, in the order refresh_all runs them:
# snapshot types first, then the remediation range appended alongside them.
VALID_REFRESH_TYPES = ("vulnerability", "policy", "asset_software", "remediation")


@dataclass
class OrchestratorContext:
    """Process-level dependencies injected into the orchestration path.

    Kept out of the module body so the caller decides where the shared database
    lives and how the platform is reached. The MCP tools build one from the
    server's own module state (which the tool tests monkeypatch); the CLI and
    unit tests build one directly.

    Attributes:
        data_dir: Directory holding the tracking database.
        db_lock: Guards all access to ``get_db()``'s connection so a query can
            never run concurrently with a table being dropped/recreated.
        get_db: Return the current shared database handle, or None.
        ensure_db: Open the shared database if it is not open yet and return it.
        set_db: Replace the shared database handle (used when building into a
            fresh ``db_path``).
        load_config: Load and validate the Rapid7 configuration.
        get_export_status: Fetch an export's platform-side status.
        download_all_files: Download the given parquet URLs.
        create_remediation_export: Create one remediation export window.
        poll_until_complete: Block until an export is ready, returning its URLs.
        snapshot_creators: Map of snapshot export_type -> create function; the
            foreground refresh looks each type up here so tests can stub them.
        download_and_load: The download+route+load step; injected as a seam so a
            caller can wrap it (the tools expose it as a patch point).
        in_progress_retry_secs: Backoff between remediation create retries.
    """

    data_dir: Path
    db_lock: threading.RLock
    get_db: Callable[[], Optional[VulnerabilityDatabase]]
    ensure_db: Callable[[], VulnerabilityDatabase]
    set_db: Callable[[Optional[VulnerabilityDatabase]], None]
    load_config: Callable[[], dict] = _load_config
    get_export_status: Callable[..., dict] = _get_export_status
    download_all_files: Callable[..., list] = _download_all_files
    create_remediation_export: Callable[..., str] = _create_remediation_export
    poll_until_complete: Callable[..., list] = _poll_until_complete
    snapshot_creators: Optional[dict] = None
    download_and_load: Optional[Callable[..., tuple]] = None
    in_progress_retry_secs: float = _IN_PROGRESS_RETRY_SECS

    def tracker(self) -> ExportTracker:
        """Open a tracker handle on the standard tracking database."""
        return ExportTracker(str(self.data_dir / "rapid7_bulk_export_tracking.db"))


def download_and_load_files(
    ctx: OrchestratorContext,
    export_type: str,
    status_info: dict,
    api_key: str,
    on_downloaded=None,
) -> tuple:
    """Download an export's parquet files and load them into DuckDB.

    Shared by the single-export worker and the multi-chunk remediation
    orchestrator so the download/route/load path is not forked. Acquires
    the db lock around the load. If provided, on_downloaded() is called once
    the files are actually downloaded and before the load begins, so a
    caller can record the LOADING phase honestly. Returns (row_count,
    row_counts, stats, validation_warnings).
    """
    parquet_urls = status_info["parquetFiles"]
    file_data = ctx.download_all_files(parquet_urls, api_key)
    if on_downloaded is not None:
        on_downloaded()

    temp_dir = tempfile.mkdtemp()
    validation_warnings: list = []
    try:
        with ctx.db_lock:
            db = ctx.get_db()
            if db is None:
                db = ctx.ensure_db()

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


def _load_step(ctx: OrchestratorContext, *args, **kwargs) -> tuple:
    """Dispatch the download+load step through the context's optional seam.

    A caller may override ``download_and_load`` (the MCP tools expose it as a
    patch point); otherwise the module implementation runs directly.
    """
    if ctx.download_and_load is not None:
        return ctx.download_and_load(*args, **kwargs)
    return download_and_load_files(ctx, *args, **kwargs)


def run_download_and_load(ctx: OrchestratorContext, export_id: str, export_type: str) -> None:
    """Background worker: download parquet files and load them into DuckDB.

    Runs in its own thread. All progress/results are written to the
    ExportTracker row for this export_id rather than returned, since
    nothing is waiting on a function return here. Any exception is caught
    and recorded as a FAILED phase (with the error text) so the status
    tools can report it, instead of the process crashing on a late/
    duplicate response the way the synchronous version could when a client
    had already timed out and cancelled the request.
    """
    tracker = ctx.tracker()
    try:
        config = ctx.load_config()
        status_info = ctx.get_export_status(config, export_id)
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

        row_count, row_counts, stats, validation_warnings = _load_step(
            ctx, export_type, status_info, config["api_key"], on_downloaded=_mark_loading
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
        # This message is stored on the tracker and later surfaced to the model
        # by the status tools; a traceback here can carry request material, so
        # scrub the credential before it is persisted or shown.
        tracker.set_phase(export_id, PHASE_FAILED, phase_detail=None, message=_redact_secret(message))
    finally:
        tracker.close()


def create_remediation_chunk_waiting(ctx: OrchestratorContext, config: dict, chunk_start: str, chunk_end: str) -> str:
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
            return ctx.create_remediation_export(config, chunk_start, chunk_end)
        except ExportInProgressError:
            if time.monotonic() >= deadline:
                raise
            print(
                f"Remediation export slot busy; waiting to create {chunk_start} → {chunk_end}...",
                file=sys.stderr,
            )
            time.sleep(ctx.in_progress_retry_secs)


def run_remediation_job(ctx: OrchestratorContext, job_id: str, chunks: list) -> None:
    """Background worker: load a multi-window remediation range as one job.

    Processes each ≤31-day window strictly sequentially (create → poll →
    download → load append), because the platform serialises remediation
    exports anyway. Per-chunk outcome is persisted on the export_jobs row so a
    partial failure names exactly which windows loaded and which did not.
    """
    tracker = ctx.tracker()
    try:
        config = ctx.load_config()
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
            eid = create_remediation_chunk_waiting(ctx, config, cs, ce)
            chunks[i]["export_id"] = eid

            _mark("waiting for export")
            parquet_urls = ctx.poll_until_complete(config, eid)
            status_info = ctx.get_export_status(config, eid)
            if not parquet_urls:
                status_info["parquetFiles"] = status_info.get("parquetFiles", [])

            row_count, _, _, _ = _load_step(
                ctx, "remediation", status_info, config["api_key"], on_downloaded=lambda: _mark("loading")
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
        tracker.update_job(job_id, status=JOB_FAILED, chunks=chunks, message=_redact_secret(message))
    finally:
        tracker.close()


def start_remediation_job(ctx: OrchestratorContext, start_date: str, end_date: str) -> str:
    """Create and launch a multi-window remediation load job. Returns the job_id."""
    chunk_ranges = build_remediation_date_chunks(start_date, end_date)
    chunks = [{"start": cs, "end": ce, "export_id": None, "status": "pending"} for cs, ce in chunk_ranges]

    job_id = f"remediation-{uuid.uuid4().hex[:12]}"
    tracker = ctx.tracker()
    tracker.create_job(
        job_id=job_id,
        export_type="remediation",
        start_date=start_date,
        end_date=end_date,
        chunks=chunks,
        status=JOB_RUNNING,
    )
    tracker.close()

    thread = threading.Thread(target=run_remediation_job, args=(ctx, job_id, chunks), daemon=True)
    thread.start()
    return job_id


# ---------------------------------------------------------------------------
# Synchronous refresh entry points
#
# These are the headless job's surface: each creates, polls, downloads and
# loads to completion on the calling thread and returns a structured result,
# with no background thread that could die mid-write. They build into the
# ``db_path`` they are handed rather than assuming the live database, so a job
# can produce a fresh artifact instead of mutating the one being served.
# ---------------------------------------------------------------------------


@dataclass
class WindowResult:
    """Outcome of one refreshed window (a snapshot type, or a remediation range)."""

    kind: str  # export_type, or "remediation"
    ok: bool
    row_count: int = 0
    detail: str = ""
    error: str = ""


@dataclass
class RefreshResult:
    """Structured result of a foreground refresh across one or more windows."""

    ok: bool
    windows: list = field(default_factory=list)
    total_rows: int = 0

    @property
    def failed(self) -> list:
        """The windows that did not load, for the CLI's non-zero-exit decision."""
        return [w for w in self.windows if not w.ok]


def _default_snapshot_creators() -> dict:
    """The real snapshot create functions, keyed by export type."""
    return {
        "vulnerability": _create_vulnerability_export,
        "policy": _create_policy_export,
        "asset_software": _create_asset_software_export,
    }


def _foreground_context(db_path: str, data_dir: Path, **overrides) -> OrchestratorContext:
    """Build a context bound to a single, freshly opened database at ``db_path``.

    The database is private to this call — no shared global, no lock contention
    with a serving replica — which is what lets a job build a new artifact
    instead of mutating the live one.
    """
    holder: dict = {"db": None}

    def _ensure() -> VulnerabilityDatabase:
        if holder["db"] is None:
            holder["db"] = VulnerabilityDatabase(db_path)
        return holder["db"]

    return OrchestratorContext(
        data_dir=data_dir,
        db_lock=threading.RLock(),
        get_db=lambda: holder["db"],
        ensure_db=_ensure,
        set_db=lambda value: holder.__setitem__("db", value),
        **overrides,
    )


def run_snapshot_refresh(
    export_type: str,
    *,
    config: dict,
    db_path: str,
    data_dir: Path,
    ctx: Optional[OrchestratorContext] = None,
) -> WindowResult:
    """Refresh one snapshot export type synchronously: create → poll → load.

    Args:
        export_type: One of "vulnerability", "policy", "asset_software".
        config: Rapid7 configuration (as returned by load_config()).
        db_path: Database file to build into.
        data_dir: Directory holding the tracking database.
        ctx: Pre-built context (used by tests); one is created when omitted.

    Returns:
        A WindowResult describing whether the window loaded and its row count.
    """
    ctx = ctx or _foreground_context(db_path, data_dir, load_config=lambda: config)
    creators = ctx.snapshot_creators or _default_snapshot_creators()
    if export_type not in creators:
        return WindowResult(kind=export_type, ok=False, error=f"unsupported snapshot type: {export_type}")

    try:
        export_id = creators[export_type](config)
        parquet_urls = ctx.poll_until_complete(config, export_id)
        status_info = ctx.get_export_status(config, export_id)
        if not parquet_urls:
            status_info["parquetFiles"] = status_info.get("parquetFiles", [])

        row_count, _, _, warnings = _load_step(ctx, export_type, status_info, config["api_key"])
        detail = f"export {export_id}, {row_count} rows"
        if warnings:
            detail += "; " + "; ".join(warnings)
        return WindowResult(kind=export_type, ok=True, row_count=row_count, detail=detail)
    except Exception as e:
        return WindowResult(kind=export_type, ok=False, error=str(e))


def run_remediation_refresh(
    start_date: str,
    end_date: str,
    *,
    config: dict,
    db_path: str,
    data_dir: Path,
    ctx: Optional[OrchestratorContext] = None,
) -> WindowResult:
    """Refresh a remediation date range synchronously across its ≤31-day windows.

    Runs each window in sequence on the calling thread (create → poll →
    download → load append), because the platform serialises remediation
    exports. Any window failure aborts and is reported.

    Args:
        start_date: Range start in YYYY-MM-DD.
        end_date: Range end in YYYY-MM-DD.
        config: Rapid7 configuration.
        db_path: Database file to build into.
        data_dir: Directory holding the tracking database.
        ctx: Pre-built context (used by tests); one is created when omitted.

    Returns:
        A WindowResult for the whole range, with the summed row count.
    """
    ctx = ctx or _foreground_context(db_path, data_dir, load_config=lambda: config)
    chunk_ranges = build_remediation_date_chunks(start_date, end_date)

    total_rows = 0
    try:
        for cs, ce in chunk_ranges:
            export_id = create_remediation_chunk_waiting(ctx, config, cs, ce)
            parquet_urls = ctx.poll_until_complete(config, export_id)
            status_info = ctx.get_export_status(config, export_id)
            if not parquet_urls:
                status_info["parquetFiles"] = status_info.get("parquetFiles", [])
            row_count, _, _, _ = _load_step(ctx, "remediation", status_info, config["api_key"])
            total_rows += row_count
        detail = f"{len(chunk_ranges)} window(s) {start_date} → {end_date}, {total_rows} rows"
        return WindowResult(kind="remediation", ok=True, row_count=total_rows, detail=detail)
    except Exception as e:
        return WindowResult(kind="remediation", ok=False, row_count=total_rows, error=str(e))


def refresh_all(
    types,
    *,
    config: dict,
    db_path: str,
    data_dir: Path,
    start_date: str = "",
    end_date: str = "",
) -> RefreshResult:
    """Refresh every requested export type into ``db_path`` synchronously.

    The headless job's entry point. Snapshot types and a remediation range are
    each loaded into the SAME database and lock, so cross-type coexistence and
    append semantics match the interactive path. A failure in one window does
    not abort the others — every requested window is attempted so the result
    names exactly what did and did not load.

    Args:
        types: Iterable of export types to refresh. "remediation" consults
            start_date/end_date (defaulting to the last 30 days).
        config: Rapid7 configuration.
        db_path: Database file to build into.
        data_dir: Directory holding the tracking database.
        start_date: Remediation range start (YYYY-MM-DD); default 30 days ago.
        end_date: Remediation range end (YYYY-MM-DD); default today.

    Returns:
        A RefreshResult; ``ok`` is False if any window failed.
    """
    # One context — hence one database handle and one lock — shared across every
    # window, so a later snapshot cannot clobber an earlier append and vice versa.
    ctx = _foreground_context(db_path, data_dir, load_config=lambda: config)

    windows: list = []
    for export_type in types:
        if export_type == "remediation":
            s = start_date or (_dt.date.today() - _dt.timedelta(days=30)).isoformat()
            e = end_date or _dt.date.today().isoformat()
            windows.append(run_remediation_refresh(s, e, config=config, db_path=db_path, data_dir=data_dir, ctx=ctx))
        else:
            windows.append(
                run_snapshot_refresh(export_type, config=config, db_path=db_path, data_dir=data_dir, ctx=ctx)
            )

    total_rows = sum(w.row_count for w in windows)
    return RefreshResult(ok=all(w.ok for w in windows), windows=windows, total_rows=total_rows)
