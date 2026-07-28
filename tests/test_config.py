"""Unit tests for the configuration module."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.config import REGION_ENDPOINTS, _get_key_from_keychain, load_config


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
