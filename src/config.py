"""Configuration module for Rapid7 Vulnerability Export.

This module handles loading and validating configuration from environment variables.
"""

import os
from typing import Dict

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


def load_config() -> Dict[str, str]:
    """Load and validate configuration from environment variables.

    Reads the RAPID7_API_KEY and RAPID7_REGION environment variables,
    validates them, and constructs the appropriate API endpoint URL.

    Returns:
        dict: Configuration dictionary containing:
            - api_key (str): The API key for authentication
            - region (str): The region identifier
            - endpoint (str): The full API endpoint URL

    Raises:
        ValueError: If RAPID7_API_KEY is not set
        ValueError: If RAPID7_REGION is not set
        ValueError: If region is not in the valid list

    Requirements:
        - 1.1: Read API key from RAPID7_API_KEY environment variable
        - 1.2: Read region from RAPID7_REGION environment variable
        - 1.3: Terminate with error if RAPID7_API_KEY is not set
        - 1.4: Terminate with error if RAPID7_REGION is not set
        - 1.5: Construct API endpoint URL based on region value
    """
    # Read API key from environment
    api_key = os.environ.get("RAPID7_API_KEY")
    if not api_key:
        raise ValueError("RAPID7_API_KEY environment variable is not set")

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
