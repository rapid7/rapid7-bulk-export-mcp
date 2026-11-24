"""A single-org setup grows into multi-org: the original key becomes the first org, and nothing else changes."""

import json
from datetime import datetime
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


from src.duckdb_loader import VulnerabilityDatabase  # noqa: E402
from tests.test_org_labels import _assets  # noqa: E402
from tests.test_org_load_metadata import _org_file  # noqa: E402


def _counts(db):
    return {
        row["org"]: row["n"] for row in db.query('SELECT "orgId" AS org, COUNT(*) AS n FROM vulnerabilities GROUP BY 1')
    }


def _org_rows(tmp_path, org_id, n, tag):
    """n findings for one org, in a file named by tag so loads never share a file."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    base = pq.read_table(_org_file(tmp_path, org_id, datetime(2026, 9, 1)))
    rows = pa.concat_tables([base] * n).set_column(2, "vulnId", pa.array([f"V{i}" for i in range(n)]))
    path = tmp_path / f"{org_id}-{tag}.parquet"
    pq.write_table(rows, path)
    return str(path)


@pytest.fixture
def db(tmp_path):
    database = VulnerabilityDatabase(str(tmp_path / "data.db"))
    yield database
    database.close()


def test_the_first_multi_org_load_replaces_the_original_orgs_single_org_rows(db, tmp_path):
    db.load_parquet_files_by_prefix({"asset_vulnerability": [_org_rows(tmp_path, "ORG-A", 5, "single")]})
    assert _counts(db) == {"ORG-A": 5}

    db.load_parquet_files_by_prefix(
        {"asset_vulnerability": [_org_rows(tmp_path, "ORG-A", 3, "multi")], "asset": [_assets(tmp_path, "ORG-A")]},
        org_scoped=True,
        org_label="primary",
    )
    db.load_parquet_files_by_prefix(
        {"asset_vulnerability": [_org_rows(tmp_path, "ORG-B", 2, "multi")], "asset": [_assets(tmp_path, "ORG-B")]},
        org_scoped=True,
        org_label="second",
    )

    assert _counts(db) == {"ORG-A": 3, "ORG-B": 2}, "the original org's 5 single-org rows are replaced, not added to"
    assert db.get_org_labels() == {"ORG-A": "primary", "ORG-B": "second"}


def test_going_back_to_single_org_leaves_only_the_original_orgs_snapshot(db, tmp_path):
    for org, n, label in (("ORG-A", 3, "primary"), ("ORG-B", 2, "second")):
        db.load_parquet_files_by_prefix(
            {"asset_vulnerability": [_org_rows(tmp_path, org, n, "multi")]}, org_scoped=True, org_label=label
        )

    db.load_parquet_files_by_prefix({"asset_vulnerability": [_org_rows(tmp_path, "ORG-A", 4, "single")]})

    assert _counts(db) == {"ORG-A": 4}, "a single-org snapshot load replaces every org's rows"


def test_removing_the_orgs_file_returns_the_same_key_to_single_org_use(tmp_path, monkeypatch, no_keychain):
    monkeypatch.setenv("RAPID7_API_KEY", "key-primary")
    monkeypatch.setenv("RAPID7_REGION", "us")
    monkeypatch.setenv(
        "RAPID7_ORGS_FILE", _orgs_file(tmp_path, [{"label": "primary", "key_ref": "RAPID7_API_KEY", "region": "us"}])
    )
    [as_org] = load_org_configs()

    monkeypatch.delenv("RAPID7_ORGS_FILE")
    [single] = load_org_configs()

    assert (single["label"], single["api_key"], single["endpoint"]) == (None, as_org["api_key"], as_org["endpoint"])
