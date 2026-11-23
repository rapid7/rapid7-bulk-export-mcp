"""rapid7-orgs walks a customer through adding organizations, and never shows or files a key."""

import json
from pathlib import Path

import pytest

from src import keystore, orgs_cli
from src.region_detect import Detection

KEYS = {"key-us-0001": "us", "key-eu-0002": "eu", "key-ca-0003": "ca"}


@pytest.fixture
def env(tmp_path, monkeypatch, capsys):
    """A fake key store, a fake platform that knows each key's region, and scripted typing."""
    stored = {}
    monkeypatch.setattr(keystore, "available", lambda: True)
    monkeypatch.setattr(keystore, "name", lambda: "test key store")
    monkeypatch.setattr(keystore, "store", lambda ref, key: stored.__setitem__(ref, key))
    monkeypatch.setattr(keystore, "read", lambda ref: stored.get(ref))
    monkeypatch.setattr(keystore, "remove", lambda ref: stored.pop(ref, None) is not None)
    monkeypatch.setattr(
        orgs_cli,
        "detect_region",
        lambda key: Detection(KEYS[key]) if key in KEYS else Detection(None, [], "No region accepted this key."),
    )
    for name in ("RAPID7_ORGS_FILE", "RAPID7_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RAPID7_REGION", "us")
    path = tmp_path / "orgs.json"

    def run(typed=(), keys=(), *args):
        typed, keys = list(typed), list(keys)
        monkeypatch.setattr("builtins.input", lambda prompt="": print(prompt, end="") or typed.pop(0))
        monkeypatch.setattr(orgs_cli.getpass, "getpass", lambda prompt="": keys.pop(0))
        code = orgs_cli.main(["--orgs-file", str(path), *args])
        out = capsys.readouterr()
        return code, out.out + out.err

    run.path, run.stored = path, stored
    return run


def _orgs(env):
    return json.loads(env.path.read_text())["orgs"]


def test_the_walkthrough_keeps_the_desktop_org_and_adds_each_new_one_in_its_detected_region(env):
    typed = ["", "", "", "fogcutter", "", "james eu", "", ""]
    code, out = env(typed, ["key-us-0001", "key-eu-0002"])

    assert code == 0
    assert _orgs(env) == [
        {"label": "primary", "key_ref": "RAPID7_API_KEY", "region": "us"},
        {"label": "fogcutter", "key_ref": "R7_KEY_FOGCUTTER", "region": "us"},
        {"label": "james-eu", "key_ref": "R7_KEY_JAMES_EU", "region": "eu"},
    ]
    assert env.stored == {"R7_KEY_FOGCUTTER": "key-us-0001", "R7_KEY_JAMES_EU": "key-eu-0002"}
    assert "The key works in region eu." in out
    assert f"Set Organizations file to: {env.path}" in out


def test_no_key_ever_appears_in_the_file_or_on_screen(env):
    _, out = env(["n", "fogcutter", "", ""], ["key-us-0001"])

    assert "key-us-0001" not in env.path.read_text()
    assert "key-us-0001" not in out


def test_a_key_no_region_accepts_stores_nothing_and_asks_again(env):
    code, out = env(["n", "bad", "good", "", ""], ["not-a-key", "key-ca-0003"])

    assert "No region accepted this key. Nothing was added." in out
    assert [o["label"] for o in _orgs(env)] == ["good"]
    assert env.stored == {"R7_KEY_GOOD": "key-ca-0003"}


def test_a_key_already_in_the_list_is_refused(env):
    _, out = env(["n", "one", "", "two", ""], ["key-us-0001", "key-us-0001"])

    assert "That key is already used by 'one'." in out
    assert [o["label"] for o in _orgs(env)] == ["one"]


def test_a_name_already_in_the_list_is_refused_before_any_key_is_asked_for(env):
    _, out = env(["n", "one", "", "one", ""], ["key-us-0001"])

    assert "'one' is already in the list." in out


def test_answering_no_to_the_region_stores_nothing(env):
    _, out = env(["n", "one", "n", ""], ["key-us-0001"])

    assert env.stored == {} and not env.path.exists()
    assert "Nothing was added." in out


def test_a_computer_with_no_key_store_is_told_to_use_environment_variables(env, monkeypatch):
    monkeypatch.setattr(keystore, "available", lambda: False)

    code, out = env()

    assert code == 1 and "environment variable" in out


def test_list_shows_names_regions_and_key_state_only(env):
    env(["", "", "", "fogcutter", "", ""], ["key-us-0001"])

    code, out = env((), (), "list")

    assert code == 0
    assert "primary" in out and "key set in Claude Desktop" in out
    assert "fogcutter" in out and "key stored" in out and "key-us-0001" not in out


def test_remove_deletes_the_org_and_its_stored_key_but_never_the_desktop_key(env):
    env(["", "", "", "fogcutter", "", ""], ["key-us-0001"])

    env((), (), "remove", "fogcutter")
    code, out = env((), (), "remove", "primary")

    assert code == 0 and _orgs(env) == [] and env.stored == {}
    assert "clear the Organizations file setting" in out


def test_check_finds_a_missing_key_a_wrong_region_and_a_shared_key(env):
    env.path.write_text(
        json.dumps(
            {
                "orgs": [
                    {"label": "good", "key_ref": "R7_KEY_GOOD", "region": "us"},
                    {"label": "moved", "key_ref": "R7_KEY_MOVED", "region": "us"},
                    {"label": "twin", "key_ref": "R7_KEY_TWIN", "region": "us"},
                    {"label": "gone", "key_ref": "R7_KEY_GONE", "region": "us"},
                ]
            }
        )
    )
    env.stored.update({"R7_KEY_GOOD": "key-us-0001", "R7_KEY_MOVED": "key-eu-0002", "R7_KEY_TWIN": "key-us-0001"})

    code, out = env((), (), "check")

    assert code == 1
    assert "key works" in out and "WRONG REGION: the key works in eu" in out
    assert "SAME KEY AS good" in out and "KEY MISSING" in out
    assert "3 problem(s) to fix." in out


def test_the_command_installs_with_the_server():
    pyproject = (Path(__file__).resolve().parent.parent / "pyproject.toml").read_text()

    assert 'rapid7-orgs = "src.orgs_cli:main"' in pyproject.split("[project.scripts]")[1]


def test_the_menu_lists_the_orgs_then_runs_the_chosen_task(env):
    env(["n", "fogcutter", "", ""], ["key-us-0001"])

    code, out = env(["2", "fogcutter", "y", "5", ""], (), "menu")

    assert code == 0
    assert "fogcutter" in out and "1. Add organizations" in out and "2. Remove an organization" in out
    assert _orgs(env) == [] and env.stored == {}


def test_the_menu_add_choice_starts_the_walkthrough(env):
    code, out = env(["1", "n", "emea", "", "", "5", ""], ["key-eu-0002"], "menu")

    assert code == 0 and [o["label"] for o in _orgs(env)] == ["emea"]


def test_the_menu_asks_before_removing_and_keeps_the_org_on_no(env):
    env(["n", "fogcutter", "", ""], ["key-us-0001"])

    _, out = env(["2", "fogcutter", "n", "5", ""], (), "menu")

    assert [o["label"] for o in _orgs(env)] == ["fogcutter"] and "Kept 'fogcutter'" in out


def test_the_menu_waits_for_return_before_closing(env):
    code, out = env(["5", ""], (), "menu")

    assert code == 0 and "Press Return to close this window" in out


def test_clear_removes_every_org_and_its_stored_keys_after_typing_clear(env):
    env(["", "", "", "one", "", "two", "", ""], ["key-us-0001", "key-eu-0002"])

    code, out = env(["clear"], (), "clear")

    assert code == 0 and not env.path.exists() and env.stored == {}
    assert "This removes all 3 organizations" in out and "deletes their 2 stored keys" in out
    assert "The Rapid7 API Key in Claude Desktop is not changed." in out


def test_clear_keeps_everything_unless_the_word_is_typed(env):
    env(["n", "one", "", ""], ["key-us-0001"])

    code, out = env(["y"], (), "clear")

    assert code == 1 and env.path.exists() and env.stored == {"R7_KEY_ONE": "key-us-0001"}
    assert "Nothing was removed." in out


def test_the_menu_can_remove_every_org(env):
    env(["n", "one", "", ""], ["key-us-0001"])

    env(["3", "clear", "5", ""], (), "menu")

    assert not env.path.exists() and env.stored == {}


def test_with_the_default_file_the_next_steps_need_no_setting(env, monkeypatch):
    monkeypatch.setattr(orgs_cli.config, "default_orgs_file", lambda: env.path)
    monkeypatch.setattr(orgs_cli.config, "orgs_file_path", lambda: None)

    _, out = env(["n", "one", "", ""], ["key-us-0001"], "--orgs-file", "")

    assert "Claude Desktop uses this file by itself" in out
    assert "Set Organizations file" not in out
