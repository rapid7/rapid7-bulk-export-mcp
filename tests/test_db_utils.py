"""Tests for DuckDB connection resource limits."""

import pytest

from src.db_utils import DEFAULT_MEMORY_LIMIT, _resolve_memory_limit, duckdb_connection


def _current_setting(conn, name):
    return conn.execute(f"SELECT current_setting('{name}')").fetchone()[0]


@pytest.mark.parametrize(
    "configured,expected",
    [
        (None, DEFAULT_MEMORY_LIMIT),
        ("", DEFAULT_MEMORY_LIMIT),
        ("4GB", "4GB"),
        ("1.5GiB", "1.5GiB"),
        ("2 GB", "2 GB"),
        ("not-a-size", DEFAULT_MEMORY_LIMIT),
        ("8", DEFAULT_MEMORY_LIMIT),
        ("8GB; DROP TABLE assets", DEFAULT_MEMORY_LIMIT),
    ],
)
def test_resolve_memory_limit(monkeypatch, configured, expected):
    if configured is None:
        monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
    else:
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", configured)

    assert _resolve_memory_limit() == expected


def test_invalid_limit_warns_and_falls_back(monkeypatch, capsys):
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "plenty")

    assert _resolve_memory_limit() == DEFAULT_MEMORY_LIMIT
    assert "ignoring invalid DUCKDB_MEMORY_LIMIT" in capsys.readouterr().err


def test_connection_applies_memory_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "1GiB")
    db_path = str(tmp_path / "test.db")

    with duckdb_connection(db_path) as conn:
        assert _current_setting(conn, "memory_limit") == "1.0 GiB"


def test_connection_applies_temp_directory(tmp_path):
    db_path = str(tmp_path / "test.db")

    with duckdb_connection(db_path) as conn:
        assert _current_setting(conn, "temp_directory") == f"{db_path}.tmp"


def test_limits_applied_to_read_only_connection(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "2GiB")
    db_path = str(tmp_path / "test.db")

    with duckdb_connection(db_path) as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

    with duckdb_connection(db_path, read_only=True) as conn:
        assert _current_setting(conn, "memory_limit") == "2.0 GiB"
        assert _current_setting(conn, "temp_directory") == f"{db_path}.tmp"


@pytest.mark.parametrize("configured,expected", [("2", 2), ("1", 1)])
def test_threads_override_applied(tmp_path, monkeypatch, configured, expected):
    monkeypatch.setenv("DUCKDB_THREADS", configured)
    db_path = str(tmp_path / "test.db")

    with duckdb_connection(db_path) as conn:
        assert _current_setting(conn, "threads") == expected


@pytest.mark.parametrize("configured", ["", "0", "-1", "many", "2; DROP TABLE assets"])
def test_invalid_threads_leaves_duckdb_default(tmp_path, monkeypatch, configured):
    db_path = str(tmp_path / "test.db")

    with duckdb_connection(db_path) as conn:
        default = _current_setting(conn, "threads")

    monkeypatch.setenv("DUCKDB_THREADS", configured)
    with duckdb_connection(db_path) as conn:
        assert _current_setting(conn, "threads") == default


def test_limits_applied_before_external_access_disabled(tmp_path, monkeypatch):
    """disable_external_access must not block the temp_directory setting."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "3GiB")
    db_path = str(tmp_path / "test.db")

    with duckdb_connection(db_path) as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

    with duckdb_connection(db_path, read_only=True, disable_external_access=True) as conn:
        assert _current_setting(conn, "memory_limit") == "3.0 GiB"
        assert _current_setting(conn, "temp_directory") == f"{db_path}.tmp"
