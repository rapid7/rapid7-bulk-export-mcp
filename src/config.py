"""Configuration module for Rapid7 Vulnerability Export.

This module handles loading and validating configuration from environment variables.
"""

import os
import platform
import subprocess  # nosec B404
from typing import Dict, Optional

USER_AGENT = "r7:bulk-export-mcp"


# Region to endpoint mapping as specified in the design document
REGION_ENDPOINTS = {
    "us": "https://us.api.insight.rapid7.com/export/graphql",
    "us2": "https://us2.api.insight.rapid7.com/export/graphql",
    "us3": "https://us3.api.insight.rapid7.com/export/graphql",
    "eu": "https://eu.api.insight.rapid7.com/export/graphql",
    "ca": "https://ca.api.insight.rapid7.com/export/graphql",
    "au": "https://au.api.insight.rapid7.com/export/graphql",
    "ap": "https://ap.api.insight.rapid7.com/export/graphql",
}


def _get_key_from_keychain(service_name: str) -> Optional[str]:
    """Retrieve a password from macOS Keychain.

    Uses the `security find-generic-password` command to look up a stored
    credential by service name. Only available on macOS.

    Args:
        service_name: The service name used when storing the credential.

    Returns:
        The password string if found, None otherwise.
    """
    if platform.system() != "Darwin":
        return None

    try:
        result = subprocess.run(  # nosec B603 B607
            ["security", "find-generic-password", "-s", service_name, "-w"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        value = result.stdout.strip()
        return value if value else None
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return None


def load_config() -> Dict[str, str]:
    """Load and validate configuration from environment variables.

    Reads the RAPID7_API_KEY and RAPID7_REGION environment variables,
    validates them, and constructs the appropriate API endpoint URL.

    On macOS, if RAPID7_API_KEY is not found in the environment, falls back
    to reading from the macOS Keychain. Store credentials with:

        security add-generic-password -s RAPID7_API_KEY -a rapid7 -w <your-key>

    Returns:
        dict: Configuration dictionary containing:
            - api_key (str): The API key for authentication
            - region (str): The region identifier
            - endpoint (str): The full API endpoint URL

    Raises:
        ValueError: If RAPID7_API_KEY is not set
        ValueError: If RAPID7_REGION is not set
        ValueError: If region is not in the valid list
    """
    # Read API key from environment
    api_key = os.environ.get("RAPID7_API_KEY")

    # Fall back to macOS Keychain if not in environment
    if not api_key:
        api_key = _get_key_from_keychain("RAPID7_API_KEY")

    if not api_key:
        raise ValueError(
            "RAPID7_API_KEY not found. Set the environment variable or, on macOS, "
            "store it in Keychain: "
            "security add-generic-password -s RAPID7_API_KEY -a rapid7 -w <your-key>"
        )

    # Read region from environment (default to 'us')
    region = os.environ.get("RAPID7_REGION", "us")

    # Validate region and get endpoint
    if region not in REGION_ENDPOINTS:
        valid_regions = ", ".join(sorted(REGION_ENDPOINTS.keys()))
        raise ValueError(f"Invalid region: {region}. Valid regions are: {valid_regions}")

    endpoint = REGION_ENDPOINTS[region]

    return {
        "api_key": api_key,
        "region": region,
        "endpoint": endpoint,
    }
