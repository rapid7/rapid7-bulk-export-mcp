"""
InsightIDR Investigations Manager

Business logic for listing, retrieving, and closing InsightIDR
investigations, built on top of the REST transport in insightidr_client.py.

API reference: docs.rapid7.com/insightidr/insightidr-rest-api/
(Investigations v2 — preview).

Author: rozumeyroman@gmail.com
"""

from typing import Any, Dict, Optional

from .insightidr_client import INVESTIGATIONS_ACCEPT_VERSION, send_idr_request

VALID_STATUSES = ("OPEN", "INVESTIGATING", "WAITING", "CLOSED")
VALID_DISPOSITIONS = ("BENIGN", "MALICIOUS", "NOT_APPLICABLE")

_INVESTIGATIONS_PATH = "/idr/v2/investigations"


def list_investigations(
    config: Dict[str, str],
    status: Optional[str] = None,
    priorities: Optional[str] = None,
    assignee_email: Optional[str] = None,
    size: int = 20,
    index: int = 0,
) -> Dict[str, Any]:
    """List investigations matching the given filters.

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        status: Filter by status, e.g. "OPEN".
        priorities: Comma-separated list of priorities, e.g. "CRITICAL,HIGH".
        assignee_email: Only investigations assigned to this user.
        size: Page size, 1-100.
        index: 0-based page index.

    Returns:
        The raw API response: {"data": [...], "metadata": {...}}.
    """
    params: Dict[str, Any] = {"size": size, "index": index}
    if status:
        params["status"] = status
    if priorities:
        params["priorities"] = priorities
    if assignee_email:
        params["assignee.email"] = assignee_email

    return send_idr_request(
        "GET",
        config["idr_base"],
        _INVESTIGATIONS_PATH,
        config["api_key"],
        params=params,
        accept_version=INVESTIGATIONS_ACCEPT_VERSION,
    )


def get_investigation(config: Dict[str, str], investigation_id: str) -> Dict[str, Any]:
    """Get a single investigation by ID or RRN."""
    return send_idr_request(
        "GET",
        config["idr_base"],
        f"{_INVESTIGATIONS_PATH}/{investigation_id}",
        config["api_key"],
        accept_version=INVESTIGATIONS_ACCEPT_VERSION,
    )


def list_investigation_alerts(
    config: Dict[str, str], investigation_id: str, size: int = 20, index: int = 0
) -> Dict[str, Any]:
    """List alerts associated with an investigation — the underlying evidence."""
    return send_idr_request(
        "GET",
        config["idr_base"],
        f"{_INVESTIGATIONS_PATH}/{investigation_id}/alerts",
        config["api_key"],
        params={"size": size, "index": index},
        accept_version=INVESTIGATIONS_ACCEPT_VERSION,
    )


def close_investigation(config: Dict[str, str], investigation_id: str, disposition: str) -> Dict[str, Any]:
    """Close a single investigation by ID, setting its disposition.

    Uses the per-investigation status endpoint
    (/investigations/{id}/status/CLOSED) rather than the filter-based
    bulk_close API, so closing always targets one explicit, known ID
    instead of "every investigation matching these criteria."

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        investigation_id: The investigation ID or RRN to close.
        disposition: One of VALID_DISPOSITIONS — required by the API
            whenever an investigation is set to CLOSED.

    Raises:
        ValueError: If disposition is not one of VALID_DISPOSITIONS.
    """
    if disposition not in VALID_DISPOSITIONS:
        raise ValueError(f"Invalid disposition: {disposition}. Valid values are: {', '.join(VALID_DISPOSITIONS)}")

    return send_idr_request(
        "PUT",
        config["idr_base"],
        f"{_INVESTIGATIONS_PATH}/{investigation_id}/status/CLOSED",
        config["api_key"],
        json_body={"disposition": disposition},
        accept_version=INVESTIGATIONS_ACCEPT_VERSION,
    )
