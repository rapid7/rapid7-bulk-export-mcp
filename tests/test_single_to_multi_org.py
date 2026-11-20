"""A single-org setup grows into multi-org: the original key becomes the first org, and nothing else changes."""

import json
from pathlib import Path

import pytest

from src import config, mcp_server
from src.config import load_org_configs

MANIFEST = Path(__file__).resolve().parent.parent / "manifest.json"


@pytest.fixture
def no_keychain(monkeypatch):
    monkeypatch.setattr(config, "_get_key_from_keychain", lambda name: None)


def _orgs_file(tmp_path, orgs):
    path = tmp_path / "orgs.json"
    path.write_text(json.dumps({"orgs": orgs}))
    return str(path)


def test_an_org_entry_can_reuse_the_single_org_key(tmp_path, monkeypatch, no_keychain):
    orgs = [
        {"label": "primary", "key_ref": "RAPID7_API_KEY", "region": "us"},
        {"label": "second", "key_ref": "R7_KEY_SECOND", "region": "eu"},
    ]
    monkeypatch.setenv("RAPID7_ORGS_FILE", _orgs_file(tmp_path, orgs))
    monkeypatch.setenv("RAPID7_API_KEY", "key-primary")
    monkeypatch.setenv("R7_KEY_SECOND", "key-second")

    configs = load_org_configs()

    assert [(c["label"], c["api_key"], c["region"]) for c in configs] == [
        ("primary", "key-primary", "us"),
        ("second", "key-second", "eu"),
    ]


def test_with_an_orgs_file_the_write_tools_need_no_single_key(tmp_path, monkeypatch, no_keychain):
    monkeypatch.setenv("RAPID7_ORGS_FILE", _orgs_file(tmp_path, [{"label": "a", "key_ref": "R7_KEY_A"}]))
    monkeypatch.delenv("RAPID7_API_KEY", raising=False)

    assert mcp_server._write_precondition() is None


def test_without_an_orgs_file_the_write_tools_still_need_the_single_key(monkeypatch, no_keychain):
    monkeypatch.delenv("RAPID7_ORGS_FILE", raising=False)
    monkeypatch.delenv("RAPID7_API_KEY", raising=False)

    assert mcp_server._write_precondition() == mcp_server._NO_KEY_MESSAGE


def test_desktop_lets_an_orgs_file_replace_the_single_key():
    settings = json.loads(MANIFEST.read_text())["user_config"]

    assert settings["api_key"]["required"] is False
    assert settings["api_key"]["sensitive"] is True
    assert "RAPID7_API_KEY" in settings["orgs_file"]["description"]
    assert json.loads(MANIFEST.read_text())["server"]["mcp_config"]["env"]["RAPID7_API_KEY"] == (
        "${user_config.api_key}"
    ), "Desktop still passes the key to the server, so an orgs file entry can reference it"
