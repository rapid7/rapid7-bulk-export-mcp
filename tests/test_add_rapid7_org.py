"""Unit tests for the organization setup script."""

import importlib.util
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "add_rapid7_org.py"
_spec = importlib.util.spec_from_file_location("add_rapid7_org", _SCRIPT)
setup_script = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(setup_script)


class TestNormalizeLabel:
    """Tests for normalize_label()."""

    def test_lowercases_and_strips(self):
        assert setup_script.normalize_label("  Japan  ") == "japan"

    def test_accepts_dashes_and_underscores(self):
        assert setup_script.normalize_label("northern-retail_2") == "northern-retail_2"

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            setup_script.normalize_label("   ")

    @pytest.mark.parametrize("bad", ["-leading", "_leading", "has space", "has.dot", "has/slash"])
    def test_rejects_unusable_labels(self, bad):
        with pytest.raises(ValueError, match="not usable"):
            setup_script.normalize_label(bad)


class TestKeyRefFor:
    """Tests for key_ref_for()."""

    def test_uppercases_with_prefix(self):
        assert setup_script.key_ref_for("japan") == "R7_KEY_JAPAN"

    def test_replaces_dashes_with_underscores(self):
        assert setup_script.key_ref_for("northern-retail") == "R7_KEY_NORTHERN_RETAIL"

    def test_is_stable_for_equivalent_labels(self):
        assert setup_script.key_ref_for("a-b") == setup_script.key_ref_for("a_b")


class TestValidateRegion:
    """Tests for validate_region()."""

    def test_falls_back_to_default_when_blank(self):
        assert setup_script.validate_region("", "eu") == "eu"

    def test_lowercases_input(self):
        assert setup_script.validate_region("EU", "us") == "eu"

    def test_accepts_every_known_region(self):
        for region in setup_script.REGION_ENDPOINTS:
            assert setup_script.validate_region(region, "us") == region

    def test_rejects_unknown_region(self):
        with pytest.raises(ValueError, match="Invalid region 'moon'"):
            setup_script.validate_region("moon", "us")

    def test_rejects_unknown_default(self):
        with pytest.raises(ValueError, match="Invalid region 'moon'"):
            setup_script.validate_region("", "moon")


class TestMergeOrg:
    """Tests for merge_org()."""

    def test_appends_a_new_label(self):
        orgs = [{"label": "us", "key_ref": "R7_KEY_US"}]
        merged = setup_script.merge_org(orgs, {"label": "eu", "key_ref": "R7_KEY_EU"})

        assert [org["label"] for org in merged] == ["us", "eu"]

    def test_replaces_an_existing_label(self):
        orgs = [{"label": "us", "key_ref": "R7_KEY_US", "region": "us"}]
        merged = setup_script.merge_org(orgs, {"label": "us", "key_ref": "R7_KEY_US", "region": "us2"})

        assert len(merged) == 1
        assert merged[0]["region"] == "us2"

    def test_does_not_mutate_the_input(self):
        orgs = [{"label": "us", "key_ref": "R7_KEY_US"}]
        setup_script.merge_org(orgs, {"label": "eu", "key_ref": "R7_KEY_EU"})

        assert len(orgs) == 1


class TestMask:
    """Tests for mask()."""

    def test_shows_only_the_last_four_characters(self):
        assert setup_script.mask("abcdefghij") == "...ghij"

    def test_handles_a_short_secret(self):
        assert setup_script.mask("ab") == "...(short)"

    def test_handles_none(self):
        assert setup_script.mask(None) == "...(short)"


class TestLoadOrgs:
    """Tests for load_orgs()."""

    def test_returns_empty_list_when_file_is_absent(self, tmp_path):
        assert setup_script.load_orgs(str(tmp_path / "missing.json")) == []

    def test_reads_an_existing_orgs_array(self, tmp_path):
        path = tmp_path / "orgs.json"
        path.write_text(json.dumps({"orgs": [{"label": "us", "key_ref": "R7_KEY_US"}]}))

        assert setup_script.load_orgs(str(path)) == [{"label": "us", "key_ref": "R7_KEY_US"}]

    def test_rejects_invalid_json(self, tmp_path):
        path = tmp_path / "orgs.json"
        path.write_text("{not json")

        with pytest.raises(ValueError, match="not valid JSON"):
            setup_script.load_orgs(str(path))

    def test_rejects_a_document_without_orgs(self, tmp_path):
        path = tmp_path / "orgs.json"
        path.write_text(json.dumps({"organizations": []}))

        with pytest.raises(ValueError, match="must contain an 'orgs' array"):
            setup_script.load_orgs(str(path))

    def test_rejects_orgs_that_is_not_an_array(self, tmp_path):
        path = tmp_path / "orgs.json"
        path.write_text(json.dumps({"orgs": {"label": "us"}}))

        with pytest.raises(ValueError, match="not an array"):
            setup_script.load_orgs(str(path))


class TestWriteOrgs:
    """Tests for write_orgs()."""

    def test_writes_the_expected_schema(self, tmp_path):
        path = tmp_path / "nested" / "orgs.json"
        orgs = [{"label": "us", "key_ref": "R7_KEY_US", "region": "us"}]

        setup_script.write_orgs(str(path), orgs)

        assert json.loads(path.read_text()) == {"orgs": orgs}

    def test_creates_the_parent_directory(self, tmp_path):
        path = tmp_path / "a" / "b" / "orgs.json"

        setup_script.write_orgs(str(path), [])

        assert path.exists()

    def test_is_readable_only_by_the_owner(self, tmp_path):
        path = tmp_path / "orgs.json"

        setup_script.write_orgs(str(path), [])

        assert oct(path.stat().st_mode)[-3:] == "600"

    def test_never_writes_a_key(self, tmp_path):
        path = tmp_path / "orgs.json"

        setup_script.write_orgs(str(path), [{"label": "us", "key_ref": "R7_KEY_US", "region": "us"}])

        assert "api_key" not in path.read_text()

    def test_leaves_no_temporary_file_behind(self, tmp_path):
        path = tmp_path / "orgs.json"

        setup_script.write_orgs(str(path), [])

        assert [p.name for p in tmp_path.iterdir()] == ["orgs.json"]

    def test_overwrites_an_existing_file(self, tmp_path):
        path = tmp_path / "orgs.json"
        setup_script.write_orgs(str(path), [{"label": "old", "key_ref": "R7_KEY_OLD"}])

        setup_script.write_orgs(str(path), [{"label": "new", "key_ref": "R7_KEY_NEW"}])

        assert json.loads(path.read_text())["orgs"][0]["label"] == "new"


class TestVerify:
    """Tests for verify()."""

    def test_reports_false_for_an_empty_list(self):
        assert setup_script.verify([]) is False

    def test_passes_when_every_key_resolves_and_is_distinct(self):
        orgs = [
            {"label": "us", "key_ref": "R7_KEY_US", "region": "us"},
            {"label": "eu", "key_ref": "R7_KEY_EU", "region": "eu"},
        ]
        keys = {"R7_KEY_US": "key-us-0001", "R7_KEY_EU": "key-eu-0002"}

        with patch.object(setup_script, "keychain_get", side_effect=keys.get):
            assert setup_script.verify(orgs) is True

    def test_fails_when_a_key_is_missing(self):
        orgs = [
            {"label": "us", "key_ref": "R7_KEY_US", "region": "us"},
            {"label": "eu", "key_ref": "R7_KEY_EU", "region": "eu"},
        ]
        keys = {"R7_KEY_US": "key-us-0001"}

        with patch.object(setup_script, "keychain_get", side_effect=keys.get):
            assert setup_script.verify(orgs) is False

    def test_fails_when_two_orgs_share_one_key(self):
        orgs = [
            {"label": "us", "key_ref": "R7_KEY_US", "region": "us"},
            {"label": "eu", "key_ref": "R7_KEY_EU", "region": "eu"},
        ]

        with patch.object(setup_script, "keychain_get", return_value="same-key-0001"):
            assert setup_script.verify(orgs) is False

    def test_never_prints_a_whole_key(self, capsys):
        orgs = [{"label": "us", "key_ref": "R7_KEY_US", "region": "us"}]

        with patch.object(setup_script, "keychain_get", return_value="supersecret9999"):
            setup_script.verify(orgs)

        assert "supersecret9999" not in capsys.readouterr().out

    def test_prefers_the_environment_over_the_keychain(self):
        orgs = [{"label": "us", "key_ref": "R7_KEY_US", "region": "us"}]

        with patch.dict(os.environ, {"R7_KEY_US": "from-env-0001"}):
            with patch.object(setup_script, "keychain_get", return_value=None) as keychain:
                assert setup_script.verify(orgs) is True
                keychain.assert_not_called()


class TestPromptOne:
    """Tests for prompt_one()."""

    def test_returns_none_when_the_label_is_blank(self):
        with patch("builtins.input", return_value="  "):
            assert setup_script.prompt_one("us", False) is None

    def test_builds_an_entry_from_the_prompts(self):
        with patch("builtins.input", side_effect=["Japan", "eu"]):
            with patch.object(setup_script, "keychain_get", side_effect=[None, "key-jp-0001"]):
                with patch.object(setup_script, "keychain_set_prompted", return_value=True):
                    entry = setup_script.prompt_one("us", False)

        assert entry == {"label": "japan", "key_ref": "R7_KEY_JAPAN", "region": "eu"}

    def test_uses_the_default_region_when_blank(self):
        with patch("builtins.input", side_effect=["japan", ""]):
            with patch.object(setup_script, "keychain_get", side_effect=[None, "key-jp-0001"]):
                with patch.object(setup_script, "keychain_set_prompted", return_value=True):
                    entry = setup_script.prompt_one("us2", False)

        assert entry["region"] == "us2"

    def test_raises_when_security_fails(self):
        with patch("builtins.input", side_effect=["japan", "us"]):
            with patch.object(setup_script, "keychain_get", return_value=None):
                with patch.object(setup_script, "keychain_set_prompted", return_value=False):
                    with pytest.raises(ValueError, match="could not store the key"):
                        setup_script.prompt_one("us", False)

    def test_raises_when_the_key_cannot_be_read_back(self):
        with patch("builtins.input", side_effect=["japan", "us"]):
            with patch.object(setup_script, "keychain_get", return_value=None):
                with patch.object(setup_script, "keychain_set_prompted", return_value=True):
                    with pytest.raises(ValueError, match="could not read it back"):
                        setup_script.prompt_one("us", False)

    def test_raises_on_an_invalid_region(self):
        with patch("builtins.input", side_effect=["japan", "moon"]):
            with pytest.raises(ValueError, match="Invalid region 'moon'"):
                setup_script.prompt_one("us", False)

    def test_reads_the_key_from_stdin_when_asked(self):
        with patch("builtins.input", side_effect=["japan", "us"]):
            with patch("sys.stdin.readline", return_value="pasted-key-0001\n"):
                with patch.object(setup_script, "keychain_get", side_effect=[None, "pasted-key-0001"]):
                    with patch.object(setup_script, "keychain_set_value", return_value=True) as store:
                        entry = setup_script.prompt_one("us", True)

        store.assert_called_once_with("R7_KEY_JAPAN", "pasted-key-0001")
        assert entry["label"] == "japan"

    def test_raises_when_stdin_is_empty(self):
        with patch("builtins.input", side_effect=["japan", "us"]):
            with patch("sys.stdin.readline", return_value="\n"):
                with patch.object(setup_script, "keychain_get", return_value=None):
                    with pytest.raises(ValueError, match="No key arrived on stdin"):
                        setup_script.prompt_one("us", True)


class TestLoaderCompatibility:
    """The file the script writes must load in the real server loader."""

    def test_written_file_loads_in_load_org_configs(self, tmp_path):
        from src.config import load_org_configs

        path = tmp_path / "orgs.json"
        orgs = [
            {"label": "japan", "key_ref": "R7_KEY_JAPAN", "region": "us"},
            {"label": "europe", "key_ref": "R7_KEY_EUROPE", "region": "eu"},
        ]
        setup_script.write_orgs(str(path), orgs)

        env = {
            "RAPID7_ORGS_FILE": str(path),
            "R7_KEY_JAPAN": "key-jp-0001",
            "R7_KEY_EUROPE": "key-eu-0002",
        }
        with patch.dict(os.environ, env):
            loaded = load_org_configs()

        assert [org["label"] for org in loaded] == ["japan", "europe"]
        assert [org["region"] for org in loaded] == ["us", "eu"]
