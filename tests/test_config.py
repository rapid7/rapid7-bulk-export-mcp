"""Unit tests for the configuration module."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from src.config import REGION_ENDPOINTS, _get_key_from_keychain, load_config, load_org_configs


class TestLoadConfig:
    """Tests for the load_config() function."""

    def test_load_config_with_valid_environment(self):
        """Test that load_config returns correct values with valid environment variables."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "test-api-key-123", "RAPID7_REGION": "us"}):
            config = load_config()

            assert config["api_key"] == "test-api-key-123"
            assert config["region"] == "us"
            assert config["endpoint"] == "https://us.api.insight.rapid7.com/export/graphql"

    def test_load_config_all_regions(self):
        """Test that load_config works with all valid regions."""
        for region, expected_endpoint in REGION_ENDPOINTS.items():
            with patch.dict(os.environ, {"RAPID7_API_KEY": "test-key", "RAPID7_REGION": region}):
                config = load_config()

                assert config["region"] == region
                assert config["endpoint"] == expected_endpoint

    def test_missing_api_key_raises_error(self):
        """Test that missing RAPID7_API_KEY raises ValueError."""
        with patch.dict(os.environ, {"RAPID7_REGION": "us"}, clear=True):
            with pytest.raises(ValueError, match="RAPID7_API_KEY not found"):
                load_config()

    def test_missing_region_defaults_to_us(self):
        """Test that missing RAPID7_REGION defaults to 'us'."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "test-key"}, clear=True):
            config = load_config()
            assert config["region"] == "us"
            assert config["endpoint"] == REGION_ENDPOINTS["us"]

    def test_invalid_region_raises_error(self):
        """Test that invalid region raises ValueError with helpful message."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "test-key", "RAPID7_REGION": "invalid-region"}):
            with pytest.raises(ValueError, match="Invalid region: invalid-region"):
                load_config()

    def test_invalid_region_lists_valid_regions(self):
        """Test that invalid region error message lists all valid regions."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "test-key", "RAPID7_REGION": "xyz"}):
            with pytest.raises(ValueError, match="Valid regions are:"):
                load_config()

    def test_empty_api_key_raises_error(self):
        """Test that empty RAPID7_API_KEY raises ValueError."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "", "RAPID7_REGION": "us"}):
            with pytest.raises(ValueError, match="RAPID7_API_KEY not found"):
                load_config()

    def test_empty_region_raises_invalid_region_error(self):
        """Test that empty RAPID7_REGION raises ValueError for invalid region."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "test-key", "RAPID7_REGION": ""}):
            with pytest.raises(ValueError, match="Invalid region:"):
                load_config()

    def test_config_returns_all_required_keys(self):
        """Test that config dictionary contains all required keys."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "test-key", "RAPID7_REGION": "eu"}):
            config = load_config()

            assert "api_key" in config
            assert "region" in config
            assert "endpoint" in config
            assert len(config) == 3  # Ensure no extra keys


class TestKeychainFallback:
    """Tests for macOS Keychain credential fallback."""

    @patch("src.config.platform.system", return_value="Darwin")
    @patch("src.config.subprocess.run")
    def test_keychain_fallback_when_env_not_set(self, mock_run, mock_system):
        """Test that Keychain is used when env var is missing."""
        mock_run.return_value = MagicMock(stdout="keychain-api-key-123\n", returncode=0)

        with patch.dict(os.environ, {"RAPID7_REGION": "us"}, clear=True):
            config = load_config()

        assert config["api_key"] == "keychain-api-key-123"
        mock_run.assert_called_once()

    @patch("src.config.platform.system", return_value="Darwin")
    @patch("src.config.subprocess.run")
    def test_env_var_takes_precedence_over_keychain(self, mock_run, mock_system):
        """Test that environment variable is preferred over Keychain."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "env-key", "RAPID7_REGION": "us"}):
            config = load_config()

        assert config["api_key"] == "env-key"
        mock_run.assert_not_called()

    @patch("src.config.platform.system", return_value="Linux")
    def test_keychain_skipped_on_non_macos(self, mock_system):
        """Test that Keychain lookup is skipped on non-macOS systems."""
        result = _get_key_from_keychain("RAPID7_API_KEY")
        assert result is None

    @patch("src.config.platform.system", return_value="Darwin")
    @patch("src.config.subprocess.run")
    def test_keychain_returns_none_on_failure(self, mock_run, mock_system):
        """Test that Keychain failure returns None gracefully."""
        import subprocess

        mock_run.side_effect = subprocess.CalledProcessError(44, "security")

        result = _get_key_from_keychain("RAPID7_API_KEY")
        assert result is None


class TestLoadOrgConfigs:
    """Tests for load_org_configs(), which supplies one credential per organization."""

    @staticmethod
    def _write_orgs(tmp_path, orgs):
        path = tmp_path / "orgs.json"
        path.write_text(json.dumps({"orgs": orgs}), encoding="utf-8")
        return str(path)

    def test_falls_back_to_single_org_when_file_unset(self):
        """With no orgs file, the single-org config is returned as a one-entry list."""
        with patch.dict(os.environ, {"RAPID7_API_KEY": "single-key", "RAPID7_REGION": "eu"}, clear=True):
            configs = load_org_configs()

        assert len(configs) == 1
        assert configs[0]["label"] is None
        assert configs[0]["api_key"] == "single-key"
        assert configs[0]["region"] == "eu"

    def test_loads_one_credential_per_org(self, tmp_path):
        """Each org resolves its own key and endpoint."""
        orgs_file = self._write_orgs(
            tmp_path,
            [
                {"label": "northern-retail", "key_ref": "R7_KEY_NR", "region": "us"},
                {"label": "payments", "key_ref": "R7_KEY_PAY", "region": "eu"},
            ],
        )
        env = {
            "RAPID7_ORGS_FILE": orgs_file,
            "R7_KEY_NR": "key-nr",
            "R7_KEY_PAY": "key-pay",
        }
        with patch.dict(os.environ, env, clear=True):
            configs = load_org_configs()

        assert [c["label"] for c in configs] == ["northern-retail", "payments"]
        assert [c["api_key"] for c in configs] == ["key-nr", "key-pay"]
        assert configs[1]["endpoint"] == REGION_ENDPOINTS["eu"]

    def test_region_defaults_to_environment(self, tmp_path):
        """An org without a region inherits RAPID7_REGION."""
        orgs_file = self._write_orgs(tmp_path, [{"label": "one", "key_ref": "R7_KEY_ONE"}])
        env = {"RAPID7_ORGS_FILE": orgs_file, "R7_KEY_ONE": "key-one", "RAPID7_REGION": "ca"}
        with patch.dict(os.environ, env, clear=True):
            configs = load_org_configs()

        assert configs[0]["region"] == "ca"

    def test_duplicate_label_is_rejected(self, tmp_path):
        """Labels key the export cache, so a repeat would silently overwrite an org."""
        orgs_file = self._write_orgs(
            tmp_path,
            [
                {"label": "same", "key_ref": "R7_KEY_A"},
                {"label": "same", "key_ref": "R7_KEY_B"},
            ],
        )
        env = {"RAPID7_ORGS_FILE": orgs_file, "R7_KEY_A": "a", "R7_KEY_B": "b"}
        with patch.dict(os.environ, env, clear=True), pytest.raises(ValueError, match="reuses the label"):
            load_org_configs()

    def test_inline_api_key_is_rejected(self, tmp_path):
        """Keys must not be written into the orgs file."""
        orgs_file = self._write_orgs(tmp_path, [{"label": "one", "key_ref": "R7_KEY_ONE", "api_key": "oops"}])
        with (
            patch.dict(os.environ, {"RAPID7_ORGS_FILE": orgs_file}, clear=True),
            pytest.raises(ValueError, match="inline api_key"),
        ):
            load_org_configs()

    @patch("src.config._get_key_from_keychain", return_value=None)
    def test_unresolved_key_names_the_org(self, _mock_keychain, tmp_path):
        """A missing key fails the whole load and names which org is short.

        Loading a subset of the tenant would report part of the portfolio as all of it.
        """
        orgs_file = self._write_orgs(
            tmp_path,
            [
                {"label": "present", "key_ref": "R7_KEY_PRESENT"},
                {"label": "absent", "key_ref": "R7_KEY_ABSENT"},
            ],
        )
        env = {"RAPID7_ORGS_FILE": orgs_file, "R7_KEY_PRESENT": "here"}
        with patch.dict(os.environ, env, clear=True), pytest.raises(ValueError) as excinfo:
            load_org_configs()

        assert "absent" in str(excinfo.value)
        assert "here" not in str(excinfo.value), "error text must not leak a resolved key"

    def test_two_labels_sharing_one_key_are_rejected(self, tmp_path):
        """Two labels resolving to the same key would export one org twice.

        Both loads cover the same tenant, the second overwrites the first, and the
        report looks complete while an org is missing from it.
        """
        orgs_file = self._write_orgs(
            tmp_path,
            [
                {"label": "payments", "key_ref": "R7_KEY_PAY"},
                {"label": "retail", "key_ref": "R7_KEY_RETAIL"},
            ],
        )
        env = {"RAPID7_ORGS_FILE": orgs_file, "R7_KEY_PAY": "same-key", "R7_KEY_RETAIL": "same-key"}
        with patch.dict(os.environ, env, clear=True), pytest.raises(ValueError) as excinfo:
            load_org_configs()

        assert "same API key" in str(excinfo.value)
        assert "same-key" not in str(excinfo.value), "error text must not leak the key"

    def test_invalid_region_is_rejected(self, tmp_path):
        """An unknown region has no endpoint, so it cannot be exported from."""
        orgs_file = self._write_orgs(tmp_path, [{"label": "one", "key_ref": "R7_KEY_ONE", "region": "mars"}])
        env = {"RAPID7_ORGS_FILE": orgs_file, "R7_KEY_ONE": "key-one"}
        with patch.dict(os.environ, env, clear=True), pytest.raises(ValueError, match="invalid region"):
            load_org_configs()

    def test_empty_orgs_array_is_rejected(self, tmp_path):
        """An empty list is a misconfiguration, not a valid zero-org tenant."""
        orgs_file = self._write_orgs(tmp_path, [])
        with (
            patch.dict(os.environ, {"RAPID7_ORGS_FILE": orgs_file}, clear=True),
            pytest.raises(ValueError, match="non-empty 'orgs' array"),
        ):
            load_org_configs()
