"""Shared DuckDB connection utilities for short-lived, concurrent-safe access."""

import os
import re
import sys
import time
from contextlib import contextmanager
from typing import Generator

import duckdb

DEFAULT_MEMORY_LIMIT = "4GB"

_MEMORY_LIMIT_PATTERN = re.compile(r"^\d+(\.\d+)?\s*(K|M|G|T)?i?B$", re.IGNORECASE)


def _resolve_memory_limit() -> str:
    """Return the configured DuckDB memory limit, or the default if unset or unparseable."""
    configured = os.environ.get("DUCKDB_MEMORY_LIMIT", "").strip()
    if not configured:
        return DEFAULT_MEMORY_LIMIT
    if not _MEMORY_LIMIT_PATTERN.match(configured):
        print(
            f"Warning: ignoring invalid DUCKDB_MEMORY_LIMIT '{configured}', using {DEFAULT_MEMORY_LIMIT}",
            file=sys.stderr,
        )
        return DEFAULT_MEMORY_LIMIT
    return configured


def _apply_resource_limits(conn: duckdb.DuckDBPyConnection, db_path: str) -> None:
    """Bound the buffer pool and give DuckDB an on-disk location to spill to.

    Left unset, DuckDB sizes its buffer pool at roughly 75% of the memory it can
    see, which is the container limit when one is set and host memory otherwise.
    That is too high to survive a large load, because memory_limit bounds the
    buffer pool rather than process RSS: measured peak RSS runs about 1.8x the
    limit with two threads and about 3.4x with ten, since each worker carries its
    own buffers. Capping threads is therefore as load-bearing as the limit
    itself. The temp directory sits alongside the database file so spill lands on
    the same writable volume.
    """
    conn.execute(f"SET memory_limit = '{_resolve_memory_limit()}'")  # nosec B608
    threads = os.environ.get("DUCKDB_THREADS", "").strip()
    if threads.isdigit() and int(threads) > 0:
        conn.execute(f"SET threads = {int(threads)}")  # nosec B608
    temp_directory = f"{db_path}.tmp".replace("'", "''")
    conn.execute(f"SET temp_directory = '{temp_directory}'")  # nosec B608


def connect_with_retry(db_path: str, read_only: bool = False, max_retries: int = 5) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, retrying with exponential backoff on lock errors.

    Handles transient write-lock contention when multiple processes compete
    for the same database file.

    Args:
        db_path: Path to the DuckDB database file.
        read_only: Open in read-only mode (allows concurrent readers).
        max_retries: Maximum number of attempts before raising.
    """
    delay = 0.1
    for attempt in range(max_retries):
        try:
            return duckdb.connect(db_path, read_only=read_only)
        except duckdb.IOException as e:
            if "Could not set lock on file" not in str(e) or attempt == max_retries - 1:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 2.0)
    raise RuntimeError("unreachable")  # pragma: no cover


@contextmanager
def duckdb_connection(
    db_path: str,
    read_only: bool = False,
    disable_external_access: bool = False,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context manager for a short-lived DuckDB connection.

    Opens the connection, applies the memory limit and spill directory,
    optionally disables external filesystem access, yields it, then closes it on
    exit. Uses connect_with_retry to handle transient write-lock contention from
    concurrent processes.

    Args:
        db_path: Path to the DuckDB database file.
        read_only: Open in read-only mode (concurrent-safe; cannot write).
        disable_external_access: Block read_parquet/read_csv/glob from user
            SQL queries (enforced at the DuckDB engine level).
    """
    conn = connect_with_retry(db_path, read_only=read_only)
    try:
        # Must precede disable_external_access: once external access is off,
        # DuckDB rejects changes to temp_directory.
        _apply_resource_limits(conn, db_path)
        if disable_external_access:
            conn.execute("SET enable_external_access = false")
        yield conn
    finally:
        conn.close()
