"""Configuration module for Rapid7 Vulnerability Export.

This module handles loading and validating configuration from environment variables.
"""

import json
import os
import platform
import subprocess  # nosec B404
from typing import Any, Dict, List, Optional

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


ORGS_FILE_ENV = "RAPID7_ORGS_FILE"


def _resolve_secret(key_ref: str) -> Optional[str]:
    """Resolve a named credential from the environment, then the macOS Keychain."""
    return os.environ.get(key_ref) or _get_key_from_keychain(key_ref)


def _validate_org_entry(entry: Any, index: int) -> None:
    """Validate one org entry from the orgs file."""
    if not isinstance(entry, dict):
        raise ValueError(f"orgs[{index}] must be an object")
    if "api_key" in entry:
        raise ValueError(
            f"orgs[{index}] contains an inline api_key. Keys must not be written to this file. "
            f"Use key_ref to name an environment variable or Keychain entry instead."
        )
    for field in ("label", "key_ref"):
        value = entry.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"orgs[{index}] is missing a non-empty '{field}'")


def load_org_configs() -> List[Dict[str, str]]:
    """Load one API credential per organization.

    The bulk export API scopes every export to the single org named in the calling
    token, so multi-org reporting needs one organization API key per org. This reads
    the org list from the JSON file named by RAPID7_ORGS_FILE:

        {
          "orgs": [
            {"label": "northern-retail", "key_ref": "R7_KEY_NORTHERN_RETAIL", "region": "us"},
            {"label": "payments",        "key_ref": "R7_KEY_PAYMENTS"}
          ]
        }

    'key_ref' names where the key lives, never the key itself. Each ref resolves from
    the environment first, then the macOS Keychain. 'region' is optional and falls back
    to RAPID7_REGION, then 'us'.

    When RAPID7_ORGS_FILE is unset, returns the single-org configuration as a
    one-entry list with a label of None, so callers need only one code path.

    Returns:
        List of dicts with keys: label, api_key, region, endpoint.

    Raises:
        ValueError: If the file is unreadable or malformed, a label is duplicated,
            an entry carries an inline api_key, a region is unknown, or any key
            cannot be resolved. Failing here is deliberate: a partially resolved
            org list would silently report a subset of the tenant as the whole.
    """
    orgs_path = os.environ.get(ORGS_FILE_ENV)
    if not orgs_path:
        single = load_config()
        return [{**single, "label": None}]

    try:
        with open(os.path.expanduser(orgs_path), encoding="utf-8") as handle:
            document = json.load(handle)
    except OSError as e:
        raise ValueError(f"Cannot read {ORGS_FILE_ENV} at '{orgs_path}': {e}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"{ORGS_FILE_ENV} at '{orgs_path}' is not valid JSON: {e}") from e

    entries = document.get("orgs") if isinstance(document, dict) else None
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"{ORGS_FILE_ENV} at '{orgs_path}' must contain a non-empty 'orgs' array")

    default_region = os.environ.get("RAPID7_REGION", "us")
    configs: List[Dict[str, str]] = []
    seen_labels: Dict[str, int] = {}
    seen_keys: Dict[str, str] = {}
    unresolved: List[str] = []

    for index, entry in enumerate(entries):
        _validate_org_entry(entry, index)
        label = entry["label"].strip()
        if label in seen_labels:
            raise ValueError(
                f"orgs[{index}] reuses the label '{label}' already used by orgs[{seen_labels[label]}]. "
                f"Labels key the export cache and the loaded data, so they must be unique."
            )
        seen_labels[label] = index

        region = entry.get("region") or default_region
        if region not in REGION_ENDPOINTS:
            valid_regions = ", ".join(sorted(REGION_ENDPOINTS.keys()))
            raise ValueError(f"orgs[{index}] ('{label}') has invalid region '{region}'. Valid: {valid_regions}")

        api_key = _resolve_secret(entry["key_ref"])
        if not api_key:
            unresolved.append(f"{label} (key_ref: {entry['key_ref']})")
            continue

        # Two labels resolving to one key means two exports of the same org. The
        # second load would overwrite the first, and the report would show a
        # complete portfolio while an org was missing from it entirely.
        if api_key in seen_keys:
            raise ValueError(
                f"orgs[{index}] ('{label}') resolves to the same API key as '{seen_keys[api_key]}'. "
                f"Each org needs its own organization key, otherwise both exports cover one org."
            )
        seen_keys[api_key] = label

        configs.append(
            {
                "label": label,
                "api_key": api_key,
                "region": region,
                "endpoint": REGION_ENDPOINTS[region],
            }
        )

    if unresolved:
        raise ValueError(
            "Could not resolve an API key for: "
            + "; ".join(unresolved)
            + ". Set each key_ref as an environment variable or, on macOS, store it in Keychain: "
            "security add-generic-password -s <key_ref> -a rapid7 -w <your-key>"
        )

    return configs
