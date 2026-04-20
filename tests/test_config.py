"""Unit tests for the configuration module."""

import os
from unittest.mock import patch

import pytest

from src.config import REGION_ENDPOINTS, load_config


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
            with pytest.raises(ValueError, match="RAPID7_API_KEY environment variable is not set"):
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
            with pytest.raises(ValueError, match="RAPID7_API_KEY environment variable is not set"):
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
