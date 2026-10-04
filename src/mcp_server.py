#!/usr/bin/env python3
"""
FastMCP Server for Rapid7 Vulnerability Data

This server exposes vulnerability data through the Model Context Protocol,
allowing AI assistants to query and analyze the data.
"""

import datetime as _dt
import glob
import json
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Optional

import duckdb as _duckdb
from fastmcp import FastMCP
from fastmcp.server.auth import restrict_tag
from mcp.types import ToolAnnotations

from . import orchestrator
from .artifact_store import download_artifact, storage_configured
from .auth import ENV_AUDIENCE, ENV_ISSUER, ENV_JWKS_URI, build_auth
from .config import key_configured, load_config, redact_secret
from .download import download_all_files
from .duckdb_loader import VulnerabilityDatabase
from .export_manager import (
    build_remediation_date_chunks,
    create_asset_software_export,
    create_policy_export,
    create_remediation_export,
    create_vulnerability_export,
    get_export_status,
    poll_until_complete,
)
from .export_tracker import ExportTracker

# The remediation retry path lives in the orchestrator now; re-export the
# exception here because the tool tests raise it as mcp_server.ExportInProgressError.
ExportInProgressError = orchestrator.ExportInProgressError

# Initialize FastMCP server.
#
# Inbound auth is built from the environment at startup: a JWTVerifier (or a
# MultiAuth of several) when configured, or None when not. None means the HTTP
# transport will refuse to start (see main); stdio stays unauthenticated by
# design because the client owns the process. A misconfiguration — a JWKS URI
# without an issuer or audience — raises here, failing fast before any request.
_auth = build_auth()
mcp = FastMCP("rapid7-bulk-export", auth=_auth)

# Global database instance
db: Optional[VulnerabilityDatabase] = None

# Data directory — resolved once at startup, used for all database paths.
# Precedence: explicit DATA_DIR wins (existing installs unaffected), then the
# plugin host's PLUGIN_DATA (Agent Plugins spec: per-install, writable, survives
# updates), then ~/.rapid7_mcp so relative-path writes never hit a read-only CWD.
_DATA_DIR: Path = (
    Path(os.environ.get("DATA_DIR") or os.environ.get("PLUGIN_DATA") or "~/.rapid7_mcp").expanduser().resolve()
)

VALID_EXPORT_TYPES = ("vulnerability", "policy", "remediation", "asset_software")

# Read/write authorization split.
#
# The mutating tools trigger expensive, multi-window platform exports or purge
# the local database; the read tools only query it. Published to a broad Teams /
# M365 Copilot audience with no per-user data filtering, the one control that must
# hold is keeping casual callers off the write tools. Tag them and require a scope
# the everyday chat token does not carry, so the split is one declarative rule
# rather than a bespoke check per tool. Read tools carry no tag and stay open to
# any authenticated caller. The scope name is a deploy-time contract with the IdP,
# so it is configurable; the default matches the docs and templates.
WRITE_TOOL_TAG = "write"
WRITE_SCOPE = os.environ.get("MCP_AUTH_WRITE_SCOPE", "rapid7.write").strip() or "rapid7.write"

# One shared check reused across the write tools so the tag and scope are declared
# in a single place. On the stdio transport FastMCP skips component auth entirely
# (the client owns the process), so this is inert there — the unauthenticated stdio
# behaviour is unchanged.
_require_write_scope = restrict_tag(WRITE_TOOL_TAG, scopes=[WRITE_SCOPE])


# Fail-closed message shared by every write tool when no Rapid7 key is present.
#
# Separation, not storage, is the control: in a hosted deployment only the
# refresh job holds the key, and the network-facing replica holds none. A write
# tool there cannot reach the Rapid7 platform, so it must say so plainly rather
# than let load_config() raise and surface an obscure API-client error to the
# model. The wording names the cause and the fix without implying the replica is
# broken.
_NO_KEY_MESSAGE = (
    "✗ This server has no Rapid7 API key configured, so write operations are "
    "unavailable here. In a hosted deployment only the scheduled refresh job "
    "holds the key; the request-handling replica intentionally holds none. Run "
    "exports and loads from the refresh job (or a local stdio instance with a "
    "key configured), and query the loaded data here."
)


# Set at startup when hosted storage is configured but no artifact has been
# published yet. That is a BOOTSTRAP condition, not a fault: only the refresh job
# publishes an artifact, so a freshly deployed environment legitimately has none.
#
# The server used to exit here. The intent was right — never answer from an
# incomplete database — but refusing to START is the wrong mechanism for it: the
# deployment could not bootstrap at all, and the caller saw a connector timeout
# instead of an explanation. Refusing to ANSWER, with a message that names the cause
# and the fix, enforces the same invariant and leaves a diagnosable replica running.
_AWAITING_FIRST_REFRESH = False

_NO_DATA_MESSAGE = (
    "✗ No Rapid7 data is loaded yet. This server serves a point-in-time copy "
    "published by the scheduled refresh job, and that job has not completed a "
    "successful run yet — so there is no dataset to query rather than an empty one. "
    "Run the refresh job (or wait for its next scheduled run), then restart this "
    "app's revision so the replica downloads the published dataset."
)


def _read_precondition() -> Optional[str]:
    """Return the no-data message when no artifact has been published, else None.

    Called at the top of each tool that reads the artifact dataset, so the read
    surface refuses uniformly and with an explanation rather than returning empty
    results that a model would report as "you have no vulnerabilities".
    """
    return _NO_DATA_MESSAGE if _AWAITING_FIRST_REFRESH else None


def _write_precondition() -> Optional[str]:
    """Return the fail-closed message when no key is configured, else None.

    Called at the top of each write tool so the whole mutating surface refuses
    uniformly on a credential-less replica, before any work or any call that
    could raise a lower-level error.
    """
    if not key_configured():
        return _NO_KEY_MESSAGE
    return None


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

# Load-phase and job-status constants and the in-progress retry timing live in
# the orchestrator, which owns the create/poll/download/load pipeline. They are
# re-exported here because these names ARE this server's public phase vocabulary
# — the status tools and tests reference them through this module.
PHASE_DOWNLOADING = orchestrator.PHASE_DOWNLOADING
PHASE_LOADING = orchestrator.PHASE_LOADING
PHASE_COMPLETE = orchestrator.PHASE_COMPLETE
PHASE_FAILED = orchestrator.PHASE_FAILED
_ACTIVE_PHASES = orchestrator._ACTIVE_PHASES

JOB_RUNNING = orchestrator.JOB_RUNNING
JOB_COMPLETE = orchestrator.JOB_COMPLETE
JOB_FAILED = orchestrator.JOB_FAILED

# Backoff between remediation create retries. Kept as a module attribute so a
# test can shrink it; the orchestrator reads it from the context we build.
_IN_PROGRESS_RETRY_SECS = orchestrator._IN_PROGRESS_RETRY_SECS

# Guards all access to the shared `db` connection (both the background
# load and the read tools below) so a query can never run concurrently
# with a table being dropped/recreated mid-load. Re-entrant so a locked
# section can safely call another helper that also acquires it.
_db_lock = threading.RLock()


def _initialize_shared_db() -> VulnerabilityDatabase:
    """Open the shared database at the default path if not already open.

    The orchestrator asks for this via its context; it mirrors
    initialize_database() but always targets the standard file so a background
    load and the read tools share one handle.
    """
    return initialize_database()


def _orchestrator_context() -> orchestrator.OrchestratorContext:
    """Build an orchestration context bound to this server's shared state.

    The platform functions and the shared database are read from THIS module's
    namespace at call time, so the tool tests' monkeypatching of
    mcp_server.load_config / get_export_status / download_all_files /
    create_remediation_export / poll_until_complete / db / _download_and_load_files
    / _IN_PROGRESS_RETRY_SECS all continue to take effect.
    """

    def _set_db(value):
        global db
        db = value

    return orchestrator.OrchestratorContext(
        data_dir=_DATA_DIR,
        db_lock=_db_lock,
        get_db=lambda: db,
        ensure_db=_initialize_shared_db,
        set_db=_set_db,
        load_config=lambda: load_config(),
        get_export_status=lambda config, export_id: get_export_status(config, export_id),
        download_all_files=lambda urls, api_key: download_all_files(urls, api_key),
        create_remediation_export=lambda config, start, end: create_remediation_export(config, start, end),
        poll_until_complete=lambda config, eid: poll_until_complete(config, eid),
        download_and_load=lambda *a, **kw: _download_and_load_files(*a, **kw),
        in_progress_retry_secs=_IN_PROGRESS_RETRY_SECS,
    )


def _tracker() -> ExportTracker:
    """Open a tracker handle on the standard tracking database."""
    return ExportTracker(str(_DATA_DIR / "rapid7_bulk_export_tracking.db"))


def _humanize_age(loaded_at: _dt.datetime, now: _dt.datetime) -> str:
    """Render how long ago a load happened in coarse, human units."""
    seconds = max(0, int((now - loaded_at).total_seconds()))
    if seconds < 60:
        return "less than a minute ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''} ago"


def _freshness_note() -> str:
    """Return a short data-age note for a successful query, or an empty string.

    Only added when serving a published artifact. There the data is a copy that
    may be hours old and the user cannot see when it was taken; a local user
    loaded the data themselves, so the note would only change output they rely on.
    Sourced from the load-metadata table INSIDE the data database, because the
    timestamps travel with the artifact rather than living in a tracker the
    serving replica never sees.

    Fail-soft by contract: any error reading the metadata yields an empty note
    and never propagates, because a freshness annotation must never turn a good
    query into a failed one. Returns "" when nothing has ever been loaded.
    """
    try:
        if db is None or not storage_configured():
            return ""
        metadata = db.get_load_metadata()
        if not metadata:
            return ""
        newest = max(metadata.values())
        return f"\n\nData last loaded {_humanize_age(newest, _dt.datetime.now())}."
    except Exception:
        return ""


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
    ),
    tags={WRITE_TOOL_TAG},
    auth=_require_write_scope,
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

    precondition = _write_precondition()
    if precondition is not None:
        return precondition

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
        return redact_secret(f"✗ Error loading Parquet files: {str(e)}")


@mcp.tool(
    annotations=ToolAnnotations(
        title="Start Rapid7 Export",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
    tags={WRITE_TOOL_TAG},
    auth=_require_write_scope,
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
    precondition = _write_precondition()
    if precondition is not None:
        return precondition

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

            # Idempotency: this MCP tool may be retried. A remediation job
            # appends its windows, so starting a second job for the SAME range
            # would double the rows. Reuse an existing job for the exact range —
            # report progress if it's still RUNNING, or its stored result if
            # COMPLETE — and only start a fresh job when none exists or the
            # prior one FAILED (an intended retry).
            existing = tracker.find_job_by_range("remediation", start_date, end_date)
            if existing is not None and existing.get("status") in (JOB_RUNNING, JOB_COMPLETE):
                tracker.close()
                jid = existing["job_id"]
                if existing["status"] == JOB_COMPLETE:
                    return (
                        f"♻️ Remediation data for {start_date} → {end_date} is already loaded "
                        f"(job {jid}). Not re-running (that would duplicate rows).\n\n"
                        f'See results with: check_rapid7_export_status(export_id="{jid}")'
                    )
                return (
                    f"⏳ A remediation load for {start_date} → {end_date} is already in progress "
                    f"(job {jid}).\n\n"
                    f'Check progress with: check_rapid7_export_status(export_id="{jid}")'
                )

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
        return redact_secret(f"✗ Error starting {export_type} export: {str(e)}")


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

    Thin wrapper over the orchestrator's implementation, kept on this module so
    tests can substitute it as the load seam. Returns (row_count, row_counts,
    stats, validation_warnings).
    """
    return orchestrator.download_and_load_files(
        _orchestrator_context(), export_type, status_info, api_key, on_downloaded=on_downloaded
    )


def _start_remediation_job(start_date: str, end_date: str) -> str:
    """Create and launch a multi-window remediation load job. Returns the job_id."""
    return orchestrator.start_remediation_job(_orchestrator_context(), start_date, end_date)


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
    ),
    tags={WRITE_TOOL_TAG},
    auth=_require_write_scope,
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
    precondition = _write_precondition()
    if precondition is not None:
        return precondition

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
            phase_detail=f"queued: {len(parquet_urls)} file(s) to download",
        )
        tracker.close()

        thread = threading.Thread(
            target=orchestrator.run_download_and_load,
            args=(_orchestrator_context(), export_id, export_type),
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
        return redact_secret(
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

    # Refuse before taking the lock or touching db: with no dataset published,
    # an empty result would be reported to the user as "you have no
    # vulnerabilities", which is worse than an explanation.
    precondition = _read_precondition()
    if precondition is not None:
        return precondition

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
        return f"Query executed successfully. {len(results)} rows returned.\n\n{result_text}{_freshness_note()}"
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

    # Refuse before taking the lock or touching db: with no dataset published,
    # an empty result would be reported to the user as "you have no
    # vulnerabilities", which is worse than an explanation.
    precondition = _read_precondition()
    if precondition is not None:
        return precondition

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

    # Refuse before taking the lock or touching db: with no dataset published,
    # an empty result would be reported to the user as "you have no
    # vulnerabilities", which is worse than an explanation.
    precondition = _read_precondition()
    if precondition is not None:
        return precondition

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
    ),
    tags={WRITE_TOOL_TAG},
    auth=_require_write_scope,
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

    precondition = _write_precondition()
    if precondition is not None:
        return precondition

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
        return redact_secret(f"✗ Error purging data: {str(e)}")
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


def _configure_logging() -> None:
    """Send the shared export and artifact log lines to stderr.

    Those modules log under ``rapid7.refresh`` so the refresh CLI can format
    them, but the server installs no handler of its own, and without one their
    INFO lines (export progress, artifact downloads) are silently dropped.
    """
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    refresh_logger = logging.getLogger("rapid7.refresh")
    refresh_logger.handlers.clear()
    refresh_logger.addHandler(handler)
    refresh_logger.setLevel(logging.INFO)
    refresh_logger.propagate = False


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
        print("  RAPID7_REGION     Your Rapid7 region: us, us2, us3, eu, ca, au, ap (default: us)")
        print("  DATA_DIR          Directory for database files (takes precedence when set)")
        print("  PLUGIN_DATA       Plugin-host data dir; used when DATA_DIR is unset (else ~/.rapid7_mcp)")
        print("  MCP_TRANSPORT     Transport protocol: 'stdio' (default) or 'http'")
        print("  MCP_HOST          HTTP bind address (default: 0.0.0.0)")
        print("  MCP_PORT          HTTP port (default: 8000)")
        print()
        print("  Inbound auth (required for HTTP transport; see docs/authentication.md):")
        print("  MCP_AUTH_JWKS_URI       IdP JWKS endpoint (enables auth when set)")
        print("  MCP_AUTH_ISSUER         Token issuer(s), comma-separated for several")
        print("  MCP_AUTH_AUDIENCE       Audience this server is registered as")
        print("  MCP_AUTH_REQUIRED_SCOPES  Optional required scopes, comma-separated")
        print()
        print("Example:")
        print("  rapid7-mcp-server /path/to/rapid7_bulk_export.db")
        print()
        print("The server communicates via stdio by default, or streamable HTTP")
        print("when MCP_TRANSPORT=http (for Docker / remote deployments).")
        print()
        print("See README.md for configuration details.")
        sys.exit(0)

    _configure_logging()

    # Ensure data directory exists, including the imports/ subdir that
    # load_rapid7_parquet reads from (its allowed root), so a fresh
    # DATA_DIR/PLUGIN_DATA has the path users are told to copy files into.
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    (_DATA_DIR / "imports").mkdir(parents=True, exist_ok=True)

    # Get database path from args or use default
    db_path = sys.argv[1] if len(sys.argv) > 1 else str(_DATA_DIR / "rapid7_bulk_export.db")

    # Fetch the current artifact before opening the database. Container Apps
    # cannot mount Blob, so the finished database is downloaded over HTTPS to
    # local disk and served read-only from there. No-op in local mode (no Blob
    # configured), so the stdio path is untouched.
    try:
        version = download_artifact(Path(db_path))
        if version is not None:
            print(f"Downloaded artifact version {version} to {db_path}", file=sys.stderr)
    except LookupError:
        # Hosted, but no complete version exists yet. A bootstrap condition, not a
        # fault: only the refresh job publishes an artifact, so a freshly deployed
        # environment has none until one has run. Start anyway and let the read
        # tools explain themselves — exiting here means the deployment can never
        # become healthy on its own, and the caller sees a timeout instead of a
        # reason.
        global _AWAITING_FIRST_REFRESH
        _AWAITING_FIRST_REFRESH = True
        print(
            "No artifact has been published yet; starting with no data. "
            "Queries will explain this until the refresh job completes and this "
            "app's revision is restarted.",
            file=sys.stderr,
        )
    except Exception as e:
        # Anything else — Blob unreachable, denied, or a corrupt download — is a
        # real fault and must stay loud. Failing closed here is correct; failing
        # closed on an absent artifact was not.
        print(f"Refusing to start: could not download the current artifact: {e}", file=sys.stderr)
        sys.exit(1)

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
        # Fail closed: an unauthenticated HTTP deployment is exactly the exposure
        # this exists to prevent, so refuse to start rather than serve openly.
        # stdio needs no such guard — the client owns the process.
        if _auth is None:
            print(
                "Refusing to start HTTP transport with no inbound authentication configured. "
                f"Set {ENV_JWKS_URI}, {ENV_ISSUER} and {ENV_AUDIENCE} (see docs/authentication.md), "
                "or use stdio for local, client-owned use.",
                file=sys.stderr,
            )
            sys.exit(1)
        host = os.environ.get("MCP_HOST", "0.0.0.0")  # nosec B104 - intentional for Docker
        port = int(os.environ.get("MCP_PORT", "8000"))
        print(f"Starting HTTP transport on {host}:{port}", file=sys.stderr)
        mcp.run(transport="http", host=host, port=port, show_banner=False)
    else:
        mcp.run(show_banner=False)


if __name__ == "__main__":
    main()
