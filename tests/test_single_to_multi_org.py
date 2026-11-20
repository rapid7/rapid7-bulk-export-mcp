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


import importlib.util  # noqa: E402
from unittest.mock import patch  # noqa: E402

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "add_rapid7_org.py"
_spec = importlib.util.spec_from_file_location("add_rapid7_org_carry", _SCRIPT)
setup_script = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(setup_script)


def _run(tmp_path, answers, monkeypatch, extra=()):
    """Run the setup script with scripted answers, a fake Keychain, and no single key in the terminal."""
    store = {}
    monkeypatch.delenv("RAPID7_API_KEY", raising=False)

    def _set_prompted(key_ref):
        store[key_ref] = f"key-for-{key_ref}"
        return True

    path = tmp_path / "orgs.json"
    with (
        patch("builtins.input", side_effect=answers),
        patch.object(setup_script, "keychain_get", side_effect=store.get),
        patch.object(setup_script, "keychain_set_prompted", side_effect=_set_prompted),
    ):
        code = setup_script.main(["--orgs-file", str(path), "--region", "us", *extra])
    return code, path, store


def test_the_first_new_org_keeps_the_existing_key_as_the_first_entry(tmp_path, monkeypatch):
    answers = ["", "fogcutter", "", "second", "eu", ""]

    code, path, store = _run(tmp_path, answers, monkeypatch)

    assert code == 0
    assert json.loads(path.read_text())["orgs"] == [
        {"label": "fogcutter", "key_ref": "RAPID7_API_KEY", "region": "us"},
        {"label": "second", "key_ref": "R7_KEY_SECOND", "region": "eu"},
    ]
    assert "RAPID7_API_KEY" not in store, "the existing key is referenced, never stored again"
    assert "key-for" not in path.read_text(), "the file holds no key"


def test_keeping_the_key_but_adding_nothing_writes_no_file(tmp_path, monkeypatch):
    code, path, _ = _run(tmp_path, ["y", "", "", ""], monkeypatch)

    assert not path.exists(), "with no second org, the customer stays single-org"


def test_declining_starts_the_file_with_the_new_org_only(tmp_path, monkeypatch):
    _, path, _ = _run(tmp_path, ["n", "second", "eu", ""], monkeypatch)

    assert [o["label"] for o in json.loads(path.read_text())["orgs"]] == ["second"]


def test_an_existing_orgs_file_is_never_offered_the_carry_over(tmp_path, monkeypatch):
    (tmp_path / "orgs.json").write_text(json.dumps({"orgs": [{"label": "a", "key_ref": "R7_KEY_A", "region": "us"}]}))

    _, path, _ = _run(tmp_path, ["b", "us", ""], monkeypatch)

    assert [o["label"] for o in json.loads(path.read_text())["orgs"]] == ["a", "b"]


def test_a_new_org_cannot_take_the_kept_orgs_label(tmp_path, monkeypatch):
    _, path, _ = _run(tmp_path, ["", "primary", "", "primary", "other", "eu", ""], monkeypatch)

    assert [o["label"] for o in json.loads(path.read_text())["orgs"]] == ["primary", "other"]


def test_no_carry_over_flag_skips_the_question(tmp_path, monkeypatch):
    _, path, _ = _run(tmp_path, ["second", "eu", ""], monkeypatch, extra=["--no-carry-over"])

    assert [o["label"] for o in json.loads(path.read_text())["orgs"]] == ["second"]


def test_check_reports_the_kept_key_as_set_in_desktop_when_the_terminal_cannot_see_it(monkeypatch, capsys):
    monkeypatch.delenv("RAPID7_API_KEY", raising=False)
    orgs = [
        {"label": "primary", "key_ref": "RAPID7_API_KEY", "region": "us"},
        {"label": "second", "key_ref": "R7_KEY_SECOND", "region": "eu"},
    ]

    with patch.object(setup_script, "keychain_get", side_effect={"R7_KEY_SECOND": "key-second-0001"}.get):
        usable = setup_script.verify(orgs)

    output = capsys.readouterr().out
    assert usable is True
    assert "set in Claude Desktop" in output and "MISSING" not in output


def test_next_steps_say_to_keep_the_desktop_key_when_an_org_uses_it(tmp_path, monkeypatch, capsys):
    _run(tmp_path, ["", "fogcutter", "", "second", "eu", ""], monkeypatch)

    assert "Keep the Rapid7 API Key set in Claude Desktop: 'fogcutter' uses it." in capsys.readouterr().out
