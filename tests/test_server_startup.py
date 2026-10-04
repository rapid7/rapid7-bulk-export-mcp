"""Tests for the server's artifact-download readiness gate at startup.

main() blocks in mcp.run(); these patch it out so only the pre-serve download
gate is exercised. No Azure, no network — the artifact fetch is stubbed.
"""

import logging

import pytest

from src import mcp_server


@pytest.fixture(autouse=True)
def _neutralize_startup(monkeypatch, tmp_path):
    """Stop main() before it serves, and keep its filesystem work in a tmp dir."""
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path)
    # mcp.run blocks forever; replace it with a recorder so main() returns.
    ran = {}
    monkeypatch.setattr(mcp_server.mcp, "run", lambda **kw: ran.update(kw))
    # A previous crash reconcile touches the tracker; make it inert.
    monkeypatch.setattr(mcp_server, "initialize_database", lambda *a, **k: None)

    class _NoopTracker:
        def reconcile_interrupted(self, *a, **k):
            pass

        def close(self):
            pass

    monkeypatch.setattr(mcp_server, "_tracker", lambda: _NoopTracker())
    monkeypatch.setenv("MCP_TRANSPORT", "stdio")
    # main() reads sys.argv[1] as an optional db path; keep pytest's argv out.
    monkeypatch.setattr(mcp_server.sys, "argv", ["rapid7-mcp-server"])
    return ran


def test_startup_downloads_current_artifact(monkeypatch):
    """When an artifact is available, startup downloads it before serving."""
    fetched = {}

    def _download(dest):
        fetched["dest"] = str(dest)
        return "20260101T000000000000Z"

    monkeypatch.setattr(mcp_server, "download_artifact", _download)

    mcp_server.main()

    assert fetched["dest"].endswith("rapid7_bulk_export.db")


def test_startup_starts_with_no_data_when_nothing_is_published(monkeypatch, _neutralize_startup):
    """No artifact yet is a BOOTSTRAP condition, not a fault — the server must start.

    Only the refresh job publishes an artifact, so a freshly deployed environment
    legitimately has none. Exiting here meant a deployment could never become healthy
    on its own, and the caller saw a connector timeout rather than an explanation.
    """
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", False)

    def _nothing_published(dest):
        raise LookupError("no complete artifact version is available to download")

    monkeypatch.setattr(mcp_server, "download_artifact", _nothing_published)

    mcp_server.main()

    # It served rather than exiting...
    assert _neutralize_startup == {"show_banner": False}
    # ...and recorded the state the read tools key off, so they can explain
    # themselves instead of returning empty results.
    assert mcp_server._AWAITING_FIRST_REFRESH is True


def test_startup_still_refuses_when_blob_is_unreachable(monkeypatch):
    """A genuine fault must stay loud.

    Unreachable, denied or corrupt storage is not a bootstrap condition, and starting
    anyway would serve a replica that can never obtain data while looking healthy.
    """

    def _boom(dest):
        raise RuntimeError("This request is not authorized to perform this operation.")

    monkeypatch.setattr(mcp_server, "download_artifact", _boom)

    with pytest.raises(SystemExit) as exc:
        mcp_server.main()
    assert exc.value.code == 1


def test_startup_local_mode_is_unchanged(monkeypatch, _neutralize_startup):
    """With no Blob configured, download is a no-op and the server starts normally."""
    monkeypatch.setattr(mcp_server, "download_artifact", lambda dest: None)

    mcp_server.main()

    # main() reached mcp.run(): local behaviour intact.
    assert _neutralize_startup == {"show_banner": False}


def test_startup_local_mode_never_arms_the_no_data_guard(monkeypatch, _neutralize_startup):
    """A real local startup, with no Blob configured, leaves the read tools unguarded.

    Uses the real download_artifact rather than a stub, so this fails if the
    local path ever reaches the bootstrap branch and starts refusing queries.
    """
    monkeypatch.delenv("ARTIFACT_BLOB_ACCOUNT_URL", raising=False)
    monkeypatch.delenv("ARTIFACT_BLOB_CONTAINER", raising=False)
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", False)

    mcp_server.main()

    assert mcp_server._AWAITING_FIRST_REFRESH is False
    assert mcp_server._read_precondition() is None


def test_startup_sends_export_progress_to_stderr(monkeypatch, capsys):
    """Export progress logged by the shared modules must reach stderr under the server.

    Nothing else installs a handler in the server process, so without this the
    INFO-level poll lines are dropped and a long export looks hung.
    """
    monkeypatch.setattr(mcp_server, "download_artifact", lambda dest: None)

    mcp_server.main()
    logging.getLogger("rapid7.refresh.export").info("export exp-1 status=RUNNING poll=1 elapsed=0s")

    assert "export exp-1 status=RUNNING" in capsys.readouterr().err
