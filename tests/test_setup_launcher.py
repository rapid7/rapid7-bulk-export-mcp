"""Claude opens the organization setup in a terminal window, or the folder holding a file to double-click."""

import stat
import sys
from pathlib import Path

import pytest

from src import mcp_server, setup_launcher


@pytest.fixture
def launcher(tmp_path, monkeypatch):
    """A fake extension: a Python and a rapid7-orgs launcher side by side in a folder with a space."""
    bin_dir = tmp_path / "Application Support" / ".venv" / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "python").write_text("")
    (bin_dir / "rapid7-orgs").write_text("")
    monkeypatch.setattr(setup_launcher.sys, "executable", str(bin_dir / "python"))
    for name in ("RAPID7_ORGS_FILE", "RAPID7_API_KEY", "MCP_TRANSPORT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RAPID7_REGION", "eu")
    return bin_dir / "rapid7-orgs"


@pytest.fixture
def opened(monkeypatch):
    calls = []
    monkeypatch.setattr(setup_launcher.subprocess, "Popen", lambda args, **kwargs: calls.append(args))
    return calls


def _as(monkeypatch, system):
    monkeypatch.setattr(setup_launcher.platform, "system", lambda: system)


def test_on_macos_the_file_runs_the_menu_and_only_its_owner_can_run_it(tmp_path, launcher, monkeypatch):
    _as(monkeypatch, "Darwin")

    path = setup_launcher.write_setup_file(tmp_path / "data")

    assert path.name == "Set up Rapid7 organizations.command"
    text = path.read_text()
    assert text.startswith("#!/bin/sh\n")
    assert f"'{launcher}' menu" in text, "the path with a space is quoted"
    assert "RAPID7_REGION=eu" in text
    assert stat.S_IMODE(path.stat().st_mode) == 0o700


def test_on_windows_the_file_is_a_cmd_that_runs_the_exe(tmp_path, launcher, monkeypatch):
    _as(monkeypatch, "Windows")
    (launcher.parent / "rapid7-orgs.exe").write_text("")

    path = setup_launcher.write_setup_file(tmp_path / "data")

    assert path.name == "Set up Rapid7 organizations.cmd"
    assert f'"{launcher.parent / "rapid7-orgs.exe"}" menu' in path.read_text()
    assert "set RAPID7_REGION=eu" in path.read_text()


def test_the_file_uses_the_orgs_file_the_server_uses(tmp_path, launcher, monkeypatch):
    _as(monkeypatch, "Darwin")
    monkeypatch.setenv("RAPID7_ORGS_FILE", "/Users/someone/My Orgs/orgs.json")

    text = setup_launcher.write_setup_file(tmp_path / "data").read_text()

    assert "--orgs-file '/Users/someone/My Orgs/orgs.json'" in text


def test_the_file_never_holds_a_key(tmp_path, launcher, monkeypatch):
    _as(monkeypatch, "Darwin")
    monkeypatch.setenv("RAPID7_API_KEY", "secret-desktop-key")

    assert "secret-desktop-key" not in setup_launcher.write_setup_file(tmp_path / "data").read_text()


@pytest.mark.parametrize(
    ("system", "command"),
    [("Darwin", ["open", "-a", "Terminal"]), ("Windows", ["cmd", "/c", "start", ""])],
)
def test_the_window_opens_with_the_platform_command(tmp_path, launcher, monkeypatch, opened, system, command):
    _as(monkeypatch, system)
    if system == "Windows":
        (launcher.parent / "rapid7-orgs.exe").write_text("")
    path = setup_launcher.write_setup_file(tmp_path / "data")

    assert setup_launcher.open_window(path) is True
    assert opened == [command + [str(path)]]


def test_linux_opens_the_folder_because_it_has_no_standard_terminal(tmp_path, launcher, monkeypatch, opened):
    _as(monkeypatch, "Linux")
    path = setup_launcher.write_setup_file(tmp_path / "data")

    assert setup_launcher.open_window(path) is False


def test_the_tool_opens_the_window_and_tells_claude_what_to_say(tmp_path, launcher, monkeypatch, opened):
    _as(monkeypatch, "Darwin")
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path / "data")

    reply = mcp_server.open_rapid7_org_setup()

    assert opened and opened[0][:3] == ["open", "-a", "Terminal"]
    assert "A Terminal window has opened" in reply
    assert "never type or paste a key into this chat" in reply.lower()


def test_when_no_window_opens_the_tool_opens_the_folder_and_names_the_file(tmp_path, launcher, monkeypatch, opened):
    _as(monkeypatch, "Linux")
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path / "data")

    reply = mcp_server.open_rapid7_org_setup()

    assert "Set up Rapid7 organizations.sh" in reply and str(tmp_path / "data") in reply


@pytest.mark.parametrize("hosted", ["http", "storage"])
def test_a_hosted_server_refuses_because_there_is_no_desktop_to_open(tmp_path, launcher, monkeypatch, opened, hosted):
    _as(monkeypatch, "Darwin")
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path / "data")
    if hosted == "http":
        monkeypatch.setenv("MCP_TRANSPORT", "http")
    else:
        monkeypatch.setattr(mcp_server, "storage_configured", lambda: True)

    reply = mcp_server.open_rapid7_org_setup()

    assert reply.startswith("✗") and opened == [] and not (tmp_path / "data").exists()


def test_without_an_installed_launcher_the_file_runs_the_module_from_the_project(tmp_path, monkeypatch):
    _as(monkeypatch, "Darwin")
    monkeypatch.setattr(setup_launcher.sys, "executable", str(tmp_path / "python"))

    text = setup_launcher.write_setup_file(tmp_path / "data").read_text()

    assert "-m src.orgs_cli menu" in text and str(Path(setup_launcher.__file__).resolve().parent.parent) in text
    assert sys.executable  # the real interpreter is untouched


def test_with_the_default_orgs_file_the_tool_says_nothing_else_needs_setting(tmp_path, launcher, monkeypatch, opened):
    _as(monkeypatch, "Darwin")
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(setup_launcher.os, "environ", {**setup_launcher.os.environ})

    reply = mcp_server.open_rapid7_org_setup()

    assert "nothing else needs setting" in reply and "set Organizations file" not in reply
