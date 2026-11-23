"""With no orgs file named, <data folder>/orgs.json is used when it exists, so setup needs no setting."""

import json

import pytest

from src import config, mcp_server, orgs_cli
from src.config import load_org_configs, load_org_labels, orgs_file_path


@pytest.fixture
def default_file(tmp_path, monkeypatch):
    path = tmp_path / "orgs.json"
    monkeypatch.setattr(config, "default_orgs_file", lambda: path)
    monkeypatch.setattr(config, "_get_key_from_keychain", lambda name: None)
    for name in ("RAPID7_ORGS_FILE", "R7_KEY_A"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RAPID7_API_KEY", "single-key")
    monkeypatch.setenv("RAPID7_REGION", "us")
    return path


def _write(path, orgs):
    path.write_text(json.dumps({"orgs": orgs}))


def test_no_default_file_leaves_single_org_unchanged(default_file):
    assert orgs_file_path() is None
    [single] = load_org_configs()
    assert (single["label"], single["api_key"]) == (None, "single-key")


def test_an_existing_default_file_is_used_with_no_setting(default_file, monkeypatch):
    _write(default_file, [{"label": "a", "key_ref": "R7_KEY_A", "region": "eu"}])
    monkeypatch.setenv("R7_KEY_A", "key-a")

    assert orgs_file_path() == str(default_file)
    assert [(c["label"], c["region"]) for c in load_org_configs()] == [("a", "eu")]
    assert load_org_labels() == ["a"]


@pytest.mark.parametrize("unset", ["", "   ", "${user_config.orgs_file}"])
def test_an_empty_or_unfilled_setting_falls_back_to_the_default(default_file, monkeypatch, unset):
    _write(default_file, [{"label": "a", "key_ref": "R7_KEY_A"}])
    monkeypatch.setenv("RAPID7_ORGS_FILE", unset)

    assert orgs_file_path() == str(default_file)


def test_a_named_file_wins_over_the_default(default_file, tmp_path, monkeypatch):
    _write(default_file, [{"label": "a", "key_ref": "R7_KEY_A"}])
    named = tmp_path / "other.json"
    _write(named, [{"label": "b", "key_ref": "R7_KEY_A"}])
    monkeypatch.setenv("RAPID7_ORGS_FILE", str(named))

    assert orgs_file_path() == str(named)


def test_a_default_file_with_no_orgs_means_single_org(default_file):
    _write(default_file, [])

    [single] = load_org_configs()
    assert single["label"] is None and load_org_labels() == []


def test_a_named_file_with_no_orgs_is_still_an_error(default_file, tmp_path, monkeypatch):
    named = tmp_path / "named.json"
    _write(named, [])
    monkeypatch.setenv("RAPID7_ORGS_FILE", str(named))

    with pytest.raises(ValueError, match="non-empty 'orgs' array"):
        load_org_configs()


def test_the_default_file_alone_unlocks_the_write_tools(default_file, monkeypatch):
    _write(default_file, [{"label": "a", "key_ref": "R7_KEY_A"}])
    monkeypatch.delenv("RAPID7_API_KEY")

    assert mcp_server._write_precondition() is None


def test_the_default_follows_the_data_folder(monkeypatch, tmp_path):
    monkeypatch.undo()
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))

    assert config.default_orgs_file() == tmp_path / "data" / "orgs.json"
    assert orgs_cli.orgs_path() == str(tmp_path / "data" / "orgs.json"), "the command uses the same file"
