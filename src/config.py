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


def _resolve_api_key() -> Optional[str]:
    """Resolve the Rapid7 API key from the environment, then the Keychain.

    Single source of truth for where the key comes from, so the presence check
    and the loader cannot drift apart on precedence. Returns None when neither
    source has it, rather than raising, so callers can distinguish "absent" from
    "invalid" without catching an exception.
    """
    api_key = os.environ.get("RAPID7_API_KEY")
    if not api_key:
        api_key = _get_key_from_keychain("RAPID7_API_KEY")
    return api_key or None


def key_configured() -> bool:
    """Report whether a Rapid7 API key is available, without exposing it.

    Lets the network-facing replica decide, at request time, that it holds no
    credential and refuse the write tools deliberately — the separation control
    from R8 — instead of letting an unconfigured key surface as an obscure error
    deep in the API client. Deliberately returns only a boolean so a caller can
    gate behaviour without ever handling the value.
    """
    return _resolve_api_key() is not None


def redact_secret(text: str) -> str:
    """Replace the configured API key, if present, with a fixed placeholder.

    Tool error paths return ``str(e)`` to the model and the transcript, and an
    exception can carry request material that includes the credential. Scrubbing
    the known key value before it is returned closes that leak at the boundary
    where the string leaves the process, regardless of which layer raised. A
    resolution failure here must never turn into a leak, so any error while
    resolving the key is treated as "nothing to redact" and the text is returned
    unchanged only when no key could be found.
    """
    try:
        api_key = _resolve_api_key()
    except Exception:
        # Refusing to leak beats propagating: an error resolving the key must
        # not turn scrubbing into a crash on the path where a string is leaving
        # the process. Nothing to redact against, so return the text as-is.
        api_key = None
    if not api_key:
        return text
    return text.replace(api_key, "***REDACTED***")


def load_config() -> Dict[str, str]:
    """Load and validate configuration from environment variables.

    Reads the RAPID7_API_KEY and RAPID7_REGION environment variables,
    validates them, and constructs the appropriate API endpoint URL.

    On macOS, if RAPID7_API_KEY is not found in the environment, falls back
    to reading from the macOS Keychain. This is a local-development convenience
    only — it is irrelevant in a container and is not a deployment mechanism.
    In a hosted deployment the key is delivered from a managed secret store to
    the refresh job alone. Store it locally with:

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
    # Resolve the key from the environment, falling back to the macOS Keychain.
    api_key = _resolve_api_key()

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
