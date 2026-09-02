"""
Export Tracker Module

This module manages a separate DuckDB database to track Rapid7 export metadata,
allowing reuse of exports from the same day instead of creating new ones.
"""

import json
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from .db_utils import duckdb_connection


class ExportTracker:
    """Tracks export metadata in a separate DuckDB database."""

    def __init__(self, db_path: str = "rapid7_bulk_export_tracking.db"):
        """
        Initialize the export tracker.

        Args:
            db_path: Path to the DuckDB database file for tracking exports
        """
        self.db_path = db_path
        new_file = not os.path.exists(db_path)
        self._initialize_db()
        if new_file:
            os.chmod(self.db_path, 0o600)

    def _initialize_db(self):
        """Initialize the export tracking database and create schema."""
        with duckdb_connection(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS exports (
                    export_id VARCHAR PRIMARY KEY,
                    export_date DATE NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    status VARCHAR NOT NULL,
                    file_count INTEGER,
                    row_count INTEGER,
                    parquet_urls VARCHAR[],
                    local_files VARCHAR[]
                )
            """)

            # Migrate schema: add columns for existing databases. Each ALTER is
            # idempotent — a second run raises because the column exists, which
            # we swallow.
            for column_ddl in (
                "ADD COLUMN export_type VARCHAR DEFAULT 'vulnerability'",
                # Human-readable progress detail for an in-flight load
                # (e.g. "3 / 8 files downloaded"). NULL when not applicable.
                "ADD COLUMN phase_detail VARCHAR",
                # Full summary or error text produced once a load reaches a
                # terminal state, so the status tools can echo it verbatim.
                "ADD COLUMN message VARCHAR",
                # Last phase-transition time. Kept separate from created_at so
                # a load's progress updates never overwrite its creation time.
                "ADD COLUMN updated_at TIMESTAMP",
            ):
                try:
                    conn.execute(f"ALTER TABLE exports {column_ddl}")
                except Exception:
                    # Column already exists, ignore
                    pass  # nosec B110

            # Create index on export_date and export_type for fast lookups
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_export_date_type
                ON exports(export_date, export_type)
            """)

            # Multi-chunk load jobs (e.g. a remediation range split into
            # <=31-day windows). One row groups the N per-chunk export IDs
            # under a single logical job so the whole range is polled and
            # resumed as one unit. `chunks` holds the ordered per-window plan
            # as JSON: [{start, end, export_id, status}].
            conn.execute("""
                CREATE TABLE IF NOT EXISTS export_jobs (
                    job_id VARCHAR PRIMARY KEY,
                    export_type VARCHAR NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status VARCHAR NOT NULL,
                    current_index INTEGER,
                    chunks VARCHAR NOT NULL,
                    message VARCHAR,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
            """)

    def get_today_export(self, export_type: str = "vulnerability") -> Optional[Dict[str, Any]]:
        """
        Get the most recent completed export from today.

        Args:
            export_type: Type of export to filter by (default: 'vulnerability')

        Returns:
            Dictionary with export metadata if found, None otherwise
        """
        today = date.today()

        # NOTE: opened read-write (not read_only) on purpose. DuckDB refuses a
        # read-only connection while a read-write connection to the same file
        # is open in-process, and the background load holds a read-write
        # handle while writing phase updates. A read-write handle for a
        # SELECT is harmless and avoids that config-mismatch conflict.
        with duckdb_connection(self.db_path) as conn:
            result = conn.execute(
                """
                SELECT
                    export_id,
                    export_date,
                    created_at,
                    status,
                    file_count,
                    row_count,
                    parquet_urls,
                    local_files,
                    export_type
                FROM exports
                WHERE export_date = ?
                  AND status = 'COMPLETE'
                  AND export_type = ?
                ORDER BY created_at DESC
                LIMIT 1
            """,
                [today, export_type],
            ).fetchone()

        if result:
            return {
                "export_id": result[0],
                "export_date": result[1],
                "created_at": result[2],
                "status": result[3],
                "file_count": result[4],
                "row_count": result[5],
                "parquet_urls": result[6],
                "local_files": result[7],
                "export_type": result[8],
            }

        return None

    def save_export(
        self,
        export_id: str,
        status: str,
        parquet_urls: List[str],
        local_files: Optional[List[str]] = None,
        row_count: Optional[int] = None,
        export_type: str = "vulnerability",
    ):
        """
        Save or update export metadata.

        Args:
            export_id: The Rapid7 export ID
            status: Export status (COMPLETE, FAILED, etc.)
            parquet_urls: List of Parquet file URLs
            local_files: List of local file paths (optional)
            row_count: Number of rows loaded (optional)
            export_type: Type of export (default: 'vulnerability')
        """
        today = date.today()
        now = datetime.now()

        with duckdb_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO exports (
                    export_id,
                    export_date,
                    created_at,
                    status,
                    file_count,
                    row_count,
                    parquet_urls,
                    local_files,
                    export_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (export_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    file_count = EXCLUDED.file_count,
                    row_count = EXCLUDED.row_count,
                    parquet_urls = EXCLUDED.parquet_urls,
                    local_files = EXCLUDED.local_files,
                    export_type = EXCLUDED.export_type
            """,
                [
                    export_id,
                    today,
                    now,
                    status,
                    len(parquet_urls) if parquet_urls else 0,
                    row_count,
                    parquet_urls,
                    local_files,
                    export_type,
                ],
            )

    def set_phase(
        self,
        export_id: str,
        status: str,
        phase_detail: Optional[str] = None,
        message: Optional[str] = None,
        row_count: Optional[int] = None,
    ):
        """Update only the live-phase fields of an already-tracked export.

        Used by the background download/load worker to record progress
        (DOWNLOADING, LOADING) and terminal outcomes (COMPLETE, FAILED)
        without rewriting the parquet URLs or other metadata that
        save_export owns. The row must already exist (start_rapid7_export
        inserts it at PENDING); if it does not, this is a no-op update.

        Args:
            export_id: The Rapid7 export ID.
            status: New status/phase value.
            phase_detail: Optional human-readable progress string.
            message: Optional terminal summary/error text.
            row_count: Optional loaded-row count (set on COMPLETE).
        """
        now = datetime.now()
        with duckdb_connection(self.db_path) as conn:
            conn.execute(
                """
                UPDATE exports
                SET status = ?,
                    phase_detail = ?,
                    message = ?,
                    row_count = COALESCE(?, row_count),
                    updated_at = ?
                WHERE export_id = ?
                """,
                [status, phase_detail, message, row_count, now, export_id],
            )

    def create_job(
        self,
        job_id: str,
        export_type: str,
        start_date: str,
        end_date: str,
        chunks: List[Dict[str, Any]],
        status: str,
    ) -> None:
        """Create a multi-chunk load job row.

        Args:
            job_id: Unique job identifier.
            export_type: Export type (currently only 'remediation' uses jobs).
            start_date: Overall range start (YYYY-MM-DD).
            end_date: Overall range end (YYYY-MM-DD).
            chunks: Ordered per-window plan; each dict has start, end,
                export_id (None until created), and status.
            status: Initial job status.
        """
        now = datetime.now()
        with duckdb_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO export_jobs (
                    job_id, export_type, start_date, end_date, status,
                    current_index, chunks, message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (job_id) DO UPDATE SET
                    export_type = EXCLUDED.export_type,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    status = EXCLUDED.status,
                    current_index = EXCLUDED.current_index,
                    chunks = EXCLUDED.chunks,
                    message = EXCLUDED.message,
                    updated_at = EXCLUDED.updated_at
                """,
                [job_id, export_type, start_date, end_date, status, 0, json.dumps(chunks), None, now, now],
            )

    def update_job(
        self,
        job_id: str,
        status: Optional[str] = None,
        current_index: Optional[int] = None,
        chunks: Optional[List[Dict[str, Any]]] = None,
        message: Optional[str] = None,
    ) -> None:
        """Update the mutable fields of a job. Omitted fields are left unchanged."""
        now = datetime.now()
        with duckdb_connection(self.db_path) as conn:
            conn.execute(
                """
                UPDATE export_jobs
                SET status = COALESCE(?, status),
                    current_index = COALESCE(?, current_index),
                    chunks = COALESCE(?, chunks),
                    message = COALESCE(?, message),
                    updated_at = ?
                WHERE job_id = ?
                """,
                [status, current_index, json.dumps(chunks) if chunks is not None else None, message, now, job_id],
            )

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Return a job row with `chunks` decoded to a list, or None."""
        with duckdb_connection(self.db_path) as conn:
            result = conn.execute(
                """
                SELECT job_id, export_type, start_date, end_date, status,
                       current_index, chunks, message, created_at, updated_at
                FROM export_jobs
                WHERE job_id = ?
                """,
                [job_id],
            ).fetchone()

        if not result:
            return None

        return {
            "job_id": result[0],
            "export_type": result[1],
            "start_date": result[2],
            "end_date": result[3],
            "status": result[4],
            "current_index": result[5],
            "chunks": json.loads(result[6]) if result[6] else [],
            "message": result[7],
            "created_at": result[8],
            "updated_at": result[9],
        }

    def has_active_work(self, active_export_statuses: List[str], active_job_statuses: List[str]) -> bool:
        """Return True if any export or job is in an active (in-flight) state.

        Used to refuse a purge while a background download/load or a
        multi-window job is still running — the caller's DB lock only covers
        the load phase, not the polling/downloading/between-window gaps.
        """
        with duckdb_connection(self.db_path) as conn:
            if active_export_statuses:
                placeholders = ", ".join("?" for _ in active_export_statuses)
                row = conn.execute(
                    f"SELECT COUNT(*) FROM exports WHERE status IN ({placeholders})",  # nosec B608
                    active_export_statuses,
                ).fetchone()
                if row and row[0] > 0:
                    return True
            if active_job_statuses:
                placeholders = ", ".join("?" for _ in active_job_statuses)
                row = conn.execute(
                    f"SELECT COUNT(*) FROM export_jobs WHERE status IN ({placeholders})",  # nosec B608
                    active_job_statuses,
                ).fetchone()
                if row and row[0] > 0:
                    return True
        return False

    def reconcile_interrupted(
        self, active_export_statuses: List[str], active_job_statuses: List[str], reason: str
    ) -> None:
        """Flip rows left in an active phase by a crash/restart to FAILED.

        Background workers are daemon threads with no resume, so an export
        interrupted mid-run would otherwise sit in DOWNLOADING/LOADING/RUNNING
        forever and block retry as "already in progress". Called once at
        startup.
        """
        now = datetime.now()
        with duckdb_connection(self.db_path) as conn:
            if active_export_statuses:
                placeholders = ", ".join("?" for _ in active_export_statuses)
                conn.execute(
                    f"UPDATE exports SET status = 'FAILED', message = ?, updated_at = ? "  # nosec B608
                    f"WHERE status IN ({placeholders})",
                    [reason, now, *active_export_statuses],
                )
            if active_job_statuses:
                placeholders = ", ".join("?" for _ in active_job_statuses)
                rows = conn.execute(
                    f"SELECT job_id, start_date, end_date, chunks FROM export_jobs "  # nosec B608
                    f"WHERE status IN ({placeholders})",
                    active_job_statuses,
                ).fetchall()
                for job_id, jstart, jend, chunks_json in rows:
                    chunks = json.loads(chunks_json) if chunks_json else []
                    loaded = [f"{c['start']} → {c['end']}" for c in chunks if c.get("status") == "loaded"]
                    missing = [f"{c['start']} → {c['end']}" for c in chunks if c.get("status") != "loaded"]
                    # Report per-window state so the user re-runs ONLY the
                    # missing windows, never the whole range (which would
                    # duplicate already-appended rows).
                    job_msg = (
                        f"✗ Interrupted by a server restart. "
                        f"Loaded windows (kept): {', '.join(loaded) or 'none'}. "
                        f"Missing windows: {', '.join(missing) or 'none'}. "
                        f"Re-run only the missing range(s) with "
                        f'start_rapid7_export(export_type="remediation", start_date=..., end_date=...).'
                    )
                    conn.execute(
                        "UPDATE export_jobs SET status = 'FAILED', message = ?, updated_at = ? WHERE job_id = ?",
                        [job_msg, now, job_id],
                    )

    def find_job_by_range(self, export_type: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
        """Return the most recent job for an exact export_type + date range, or None.

        Lets the remediation tool be idempotent: a repeated call for the same
        range can reuse a RUNNING job or return a COMPLETE job's result rather
        than starting a second worker that would append the windows again.
        """
        with duckdb_connection(self.db_path) as conn:
            result = conn.execute(
                """
                SELECT job_id FROM export_jobs
                WHERE export_type = ? AND start_date = ? AND end_date = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [export_type, start_date, end_date],
            ).fetchone()
        return self.get_job(result[0]) if result else None

    def get_export_by_id(self, export_id: str) -> Optional[Dict[str, Any]]:
        """
        Get export metadata by export ID.

        Args:
            export_id: The Rapid7 export ID

        Returns:
            Dictionary with export metadata if found, None otherwise
        """
        # Read-write handle for a SELECT (see get_today_export note): avoids a
        # DuckDB config-mismatch when the background load holds a write handle.
        with duckdb_connection(self.db_path) as conn:
            result = conn.execute(
                """
                SELECT
                    export_id,
                    export_date,
                    created_at,
                    status,
                    file_count,
                    row_count,
                    parquet_urls,
                    local_files,
                    export_type,
                    phase_detail,
                    message,
                    updated_at
                FROM exports
                WHERE export_id = ?
            """,
                [export_id],
            ).fetchone()

        if result:
            return {
                "export_id": result[0],
                "export_date": result[1],
                "created_at": result[2],
                "status": result[3],
                "file_count": result[4],
                "row_count": result[5],
                "parquet_urls": result[6],
                "local_files": result[7],
                "export_type": result[8],
                "phase_detail": result[9],
                "message": result[10],
                "updated_at": result[11],
            }

        return None

    def list_exports(self, limit: int = 10, export_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List recent exports.

        Args:
            limit: Maximum number of exports to return
            export_type: Optional export type to filter by

        Returns:
            List of export metadata dictionaries
        """
        sql = """
            SELECT export_id, export_date, created_at, status, file_count, row_count,
                   export_type, phase_detail
            FROM exports
        """
        params: list = []
        if export_type is not None:
            sql += " WHERE export_type = ?"
            params.append(export_type)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        # Read-write handle for a SELECT (see get_today_export note): avoids a
        # DuckDB config-mismatch when the background load holds a write handle.
        with duckdb_connection(self.db_path) as conn:
            results = conn.execute(sql, params).fetchall()

        return [
            {
                "export_id": row[0],
                "export_date": row[1],
                "created_at": row[2],
                "status": row[3],
                "file_count": row[4],
                "row_count": row[5],
                "export_type": row[6],
                "phase_detail": row[7],
            }
            for row in results
        ]

    def close(self):
        """No-op — connections are short-lived and released per-operation."""

    def purge(self):
        """Purge all tracking data by deleting the database file.

        Removes the database file and any associated WAL file from disk,
        then reinitializes with a fresh schema.
        """
        for suffix in ("", ".wal"):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

        self._initialize_db()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
