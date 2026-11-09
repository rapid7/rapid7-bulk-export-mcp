"""Tests for the foreground refresh CLI (rapid7-refresh).

The CLI is a thin wiring shell over the orchestrator, so these patch
``refresh_all`` / ``load_config`` on the cli module and assert only the wiring:
exit codes, type selection, and that a failed window is reported non-zero.
"""

from pathlib import Path

from click.testing import CliRunner

from src import cli
from src.orchestrator import RefreshResult, WindowResult


def _ok_result(rows=5):
    return RefreshResult(
        ok=True, windows=[WindowResult(kind="vulnerability", ok=True, row_count=rows)], total_rows=rows
    )


def _run(monkeypatch, tmp_path, args, result=None, config_error=False):
    captured = {}

    def fake_refresh(types, *, config, db_path, data_dir, start_date, end_date):
        captured["types"] = list(types)
        captured["db_path"] = db_path
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return result if result is not None else _ok_result()

    def fake_load_config():
        if config_error:
            raise ValueError("RAPID7_API_KEY not found")
        return {"api_key": "k", "region": "us", "endpoint": "https://x"}

    monkeypatch.setattr(cli, "refresh_all", fake_refresh)
    monkeypatch.setattr(cli, "load_config", fake_load_config)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    return CliRunner().invoke(cli.main, args), captured


def test_success_exits_zero_and_echoes_db_path(monkeypatch, tmp_path):
    result, captured = _run(monkeypatch, tmp_path, ["--type", "vulnerability"])
    assert result.exit_code == 0, result.output
    assert captured["types"] == ["vulnerability"]
    # The finished artifact path is echoed on stdout for a downstream publish step.
    assert str(tmp_path / "rapid7_bulk_export.db") in result.output


def test_failed_window_exits_non_zero(monkeypatch, tmp_path):
    failing = RefreshResult(
        ok=False,
        windows=[
            WindowResult(kind="vulnerability", ok=True, row_count=1),
            WindowResult(kind="policy", ok=False, error="platform 500"),
        ],
        total_rows=1,
    )
    result, _ = _run(monkeypatch, tmp_path, ["--type", "vulnerability", "--type", "policy"], result=failing)
    assert result.exit_code == 1
    assert "policy" in result.output


def test_no_types_defaults_to_all(monkeypatch, tmp_path):
    result, captured = _run(monkeypatch, tmp_path, [])
    assert result.exit_code == 0
    assert captured["types"] == ["vulnerability", "policy", "asset_software", "remediation"]


def test_config_error_exits_two(monkeypatch, tmp_path):
    result, _ = _run(monkeypatch, tmp_path, ["--type", "vulnerability"], config_error=True)
    assert result.exit_code == 2


def test_explicit_db_path_is_passed_through(monkeypatch, tmp_path):
    target = tmp_path / "artifact" / "new.db"
    result, captured = _run(monkeypatch, tmp_path, ["--type", "vulnerability", "--db-path", str(target)])
    assert result.exit_code == 0
    assert captured["db_path"] == str(target)


def test_remediation_dates_forwarded(monkeypatch, tmp_path):
    result, captured = _run(
        monkeypatch,
        tmp_path,
        ["--type", "remediation", "--start-date", "2026-01-01", "--end-date", "2026-02-01"],
    )
    assert result.exit_code == 0
    assert captured["start_date"] == "2026-01-01"
    assert captured["end_date"] == "2026-02-01"


def test_invalid_type_is_rejected(monkeypatch, tmp_path):
    result, _ = _run(monkeypatch, tmp_path, ["--type", "bogus"])
    assert result.exit_code != 0
    assert "bogus" in result.output


def test_data_dir_is_created(monkeypatch, tmp_path):
    nested = tmp_path / "made" / "here"
    monkeypatch.setattr(cli, "refresh_all", lambda *a, **kw: _ok_result())
    monkeypatch.setattr(cli, "load_config", lambda: {"api_key": "k"})
    monkeypatch.setenv("DATA_DIR", str(nested))
    result = CliRunner().invoke(cli.main, ["--type", "vulnerability"])
    assert result.exit_code == 0
    assert Path(nested).is_dir()
