"""
InsightIDR Investigations Manager

Business logic for listing, retrieving, and closing InsightIDR
investigations, built on top of the REST transport in insightidr_client.py.

API reference: docs.rapid7.com/insightidr/insightidr-rest-api/
(Investigations v2 — preview; Comments v1 — stable).

Author: rozumeyroman@gmail.com
"""

from typing import Any, Dict, List, Optional

from .insightidr_client import INVESTIGATIONS_ACCEPT_VERSION, send_idr_request

VALID_STATUSES = ("OPEN", "INVESTIGATING", "WAITING", "CLOSED")
VALID_DISPOSITIONS = ("BENIGN", "MALICIOUS", "NOT_APPLICABLE")
VALID_COMMENT_VISIBILITIES = ("INTERNAL", "PUBLIC")

_INVESTIGATIONS_PATH = "/idr/v2/investigations"
_COMMENTS_PATH = "/idr/v1/comments"


def list_investigations(
    config: Dict[str, str],
    status: Optional[str] = None,
    priorities: Optional[str] = None,
    assignee_email: Optional[str] = None,
    sources: Optional[str] = None,
    tags: Optional[str] = None,
    size: int = 20,
    index: int = 0,
) -> Dict[str, Any]:
    """List investigations matching the given filters.

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        status: Comma-separated statuses, e.g. "OPEN,INVESTIGATING". Sent as
            the API's "statuses" query param (plural).
        priorities: Comma-separated list of priorities, e.g. "CRITICAL,HIGH".
        assignee_email: Only investigations assigned to this user.
        sources: Comma-separated sources, e.g. "MANUAL,HUNT,ALERT".
        tags: Comma-separated tags; only investigations with ALL given tags
            are included.
        size: Page size, 1-100.
        index: 0-based page index.

    Returns:
        The raw API response: {"data": [...], "metadata": {...}}.
    """
    params: Dict[str, Any] = {"size": size, "index": index}
    if status:
        params["statuses"] = status
    if priorities:
        params["priorities"] = priorities
    if assignee_email:
        params["assignee.email"] = assignee_email
    if sources:
        params["sources"] = sources
    if tags:
        params["tags"] = tags

    return send_idr_request(
        "GET",
        config["idr_base"],
        _INVESTIGATIONS_PATH,
        config["api_key"],
        params=params,
        accept_version=INVESTIGATIONS_ACCEPT_VERSION,
    )


def list_known_assignees(config: Dict[str, str], max_pages: int = 5, page_size: int = 100) -> List[Dict[str, str]]:
    """Collect the unique assignees observed across existing investigations.

    The Investigations API has no endpoint to list everyone eligible to be
    assigned (that requires the separate, permission-gated
    /account/v1/users API). This is a best-effort substitute: it scans
    investigation history for people who have been assigned before. It is
    NOT an exhaustive list of eligible users — a valid new assignee may
    simply never have been assigned anything yet.

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        max_pages: Safety cap on how many pages of investigations to scan.
        page_size: Investigations per page (API max is 100).

    Returns:
        A list of {"email": ..., "name": ...} dicts, sorted by email,
        deduplicated.
    """
    seen: Dict[str, str] = {}
    page = 0
    total_pages = 1

    while page < max_pages and page < total_pages:
        response = list_investigations(config, size=page_size, index=page)
        for investigation in response.get("data", []):
            assignee = investigation.get("assignee")
            if assignee and assignee.get("email"):
                seen[assignee["email"]] = assignee.get("name") or ""

        total_pages = response.get("metadata", {}).get("total_pages", 1)
        page += 1

    return [{"email": email, "name": name} for email, name in sorted(seen.items())]


def assign_investigation(config: Dict[str, str], investigation_id: str, user_email: str) -> Dict[str, Any]:
    """Assign a user to an investigation by email.

    The target user must be a platform administrator, product
    administrator, or read/write user with access to InsightIDR or
    InsightUBA. This function does not pre-validate eligibility — the API
    itself returns 400/403/404 for an ineligible or unknown email.
    """
    return send_idr_request(
        "PUT",
        config["idr_base"],
        f"{_INVESTIGATIONS_PATH}/{investigation_id}/assignee",
        config["api_key"],
        json_body={"user_email_address": user_email},
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


def get_alert_evidence(config: Dict[str, str], alert_id: str, size: int = 20, index: int = 0) -> Dict[str, Any]:
    """Get the underlying event evidence for a single alert.

    This is the source event data that triggered the alert — e.g. for an
    authentication alert: user, account, source IP, geolocation, and the
    raw log payload. Uses the alert-triage API
    (docs.rapid7.com/insightidr/api/alert-triage/), a different Command
    Platform path (/idr/at/) from the Investigations v2 API.

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        alert_id: The alert ID/RRN, as returned in the "data" list from
            list_investigation_alerts.
        size: Page size.
        index: 0-based page index.
    """
    return send_idr_request(
        "GET",
        config["idr_base"],
        f"/idr/at/alerts/{alert_id}/evidences",
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


def list_comments(
    config: Dict[str, str],
    target_rrn: str,
    size: int = 20,
    index: int = 0,
    sort_direction: str = "DESC",
) -> Dict[str, Any]:
    """List comments on a target resource (e.g. an investigation), newest first by default.

    The Comments API is stable v1 — unlike Investigations v2, no
    Accept-version header is required.

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        target_rrn: The RRN of the resource comments are attached to (e.g.
            an investigation's rrn — use the full rrn from
            list_investigations/get_investigation, not a short id; the
            Comments API's `target` param is documented as an RRN).
        size: Page size, 1-100.
        index: 0-based page index.
        sort_direction: "ASC" or "DESC" by creation time.

    Returns:
        The raw API response: {"data": [...], "metadata": {...}}.
    """
    params = {"target": target_rrn, "size": size, "index": index, "sortDirection": sort_direction}
    return send_idr_request(
        "GET",
        config["idr_base"],
        _COMMENTS_PATH,
        config["api_key"],
        params=params,
    )


def create_comment(config: Dict[str, str], target_rrn: str, body: str) -> Dict[str, Any]:
    """Create a comment on a target resource (e.g. an investigation).

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        target_rrn: The RRN of the resource to comment on (same caveat as
            list_comments — use the full rrn, not a short id).
        body: The comment text.

    Returns:
        The created Comment record, including its assigned rrn and
        default visibility (visibility is set by the API on creation —
        there is no way to specify it in this call; use a separate
        visibility-update call if this server adds one later).
    """
    return send_idr_request(
        "POST",
        config["idr_base"],
        _COMMENTS_PATH,
        config["api_key"],
        json_body={"target": target_rrn, "body": body},
    )


def list_investigation_product_alerts(config: Dict[str, str], investigation_id: str) -> List[Dict[str, Any]]:
    """List alerts from OTHER Rapid7 products (not InsightIDR itself) linked to this investigation.

    E.g. Threat Command (external threat intel) or Insight Agent (endpoint)
    alerts, if the org has an active license for those products and this
    investigation originated from or is linked to one of their alerts.
    Separate from list_investigation_alerts, which only covers InsightIDR's
    own alerts.

    Returns an empty list if no product alerts are linked — that's a
    normal, expected result for most investigations, not an error.
    """
    return send_idr_request(
        "GET",
        config["idr_base"],
        f"{_INVESTIGATIONS_PATH}/{investigation_id}/rapid7-product-alerts",
        config["api_key"],
        accept_version=INVESTIGATIONS_ACCEPT_VERSION,
    )
