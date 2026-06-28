"""Shared DuckDB connection utilities for short-lived, concurrent-safe access."""

import time
from contextlib import contextmanager
from typing import Generator

import duckdb


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

    Opens the connection, optionally disables external filesystem access,
    yields it, then closes it on exit. Uses connect_with_retry to handle
    transient write-lock contention from concurrent processes.

    Args:
        db_path: Path to the DuckDB database file.
        read_only: Open in read-only mode (concurrent-safe; cannot write).
        disable_external_access: Block read_parquet/read_csv/glob from user
            SQL queries (enforced at the DuckDB engine level).
    """
    conn = connect_with_retry(db_path, read_only=read_only)
    try:
        if disable_external_access:
            conn.execute("SET enable_external_access = false")
        yield conn
    finally:
        conn.close()
