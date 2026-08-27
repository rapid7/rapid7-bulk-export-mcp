"""
InsightIDR Log Search Manager

Business logic for querying log data, managing saved queries, and listing
logs/logsets, built on top of the REST transport in insightidr_client.py.

Separate API family from Investigations/Comments: /log_search path prefix,
no Accept-version header, and query endpoints are async/pollable (an LEQL
query can return 202 with a polling link instead of immediate results).

See rapid7-insightidr-skill/references/log-search-api.md for the full API
reference (no official OpenAPI spec exists for this API).

Author: rozumeyroman@gmail.com
"""

import time
from typing import Any, Dict, List, Optional

from .insightidr_client import send_idr_request

_LOG_SEARCH_BASE = "/log_search"

_DEFAULT_MAX_WAIT_SECONDS = 15.0
_DEFAULT_POLL_INTERVAL_SECONDS = 1.5


def _build_during(time_range: Optional[str], from_ms: Optional[int], to_ms: Optional[int]) -> Dict[str, Any]:
    if time_range:
        return {"time_range": time_range}
    return {"from": from_ms, "to": to_ms}


def _poll_until_ready(
    config: Dict[str, str],
    initial_result: Dict[str, Any],
    max_wait_seconds: float,
    poll_interval_seconds: float,
) -> Dict[str, Any]:
    """Follow a query's self-link until it returns a final result or max_wait_seconds elapses.

    A pending (202) response already contains an "events" key — an empty
    list, not an absent key — so completion is signaled by the "links"
    self-href disappearing, not by "events" being present.
    """
    result = initial_result
    elapsed = 0.0

    while elapsed < max_wait_seconds:
        poll_url = next(
            (link["href"] for link in result.get("links", []) if link.get("rel", "").lower() == "self"),
            None,
        )
        if not poll_url:
            break
        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds
        # poll_url is already a full absolute URL — pass it as `path` with
        # an empty base_url rather than re-deriving idr_base + a relative path.
        result = send_idr_request("GET", "", poll_url, config["api_key"])

    return result


def list_logs(config: Dict[str, str]) -> Dict[str, Any]:
    """List all logs configured for this account."""
    return send_idr_request("GET", config["idr_base"], f"{_LOG_SEARCH_BASE}/management/logs", config["api_key"])


def list_logsets(config: Dict[str, str]) -> Dict[str, Any]:
    """List all logsets (named groupings of logs) configured for this account."""
    return send_idr_request("GET", config["idr_base"], f"{_LOG_SEARCH_BASE}/management/logsets", config["api_key"])


def query_logs(
    config: Dict[str, str],
    log_ids: List[str],
    statement: str,
    time_range: Optional[str] = None,
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
    max_wait_seconds: float = _DEFAULT_MAX_WAIT_SECONDS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
) -> Dict[str, Any]:
    """Run a LEQL query against one or more logs and return matching events.

    Args:
        config: Config dict from load_config() (needs api_key, idr_base).
        log_ids: One or more log IDs to query (from list_logs).
        statement: A LEQL statement, e.g. "where(user)" or
            "where(result=FAILED) groupby(source_ip)".
        time_range: A relative window like "Last 1 Day", "Today", "Last 7
            Days". Takes precedence over from_ms/to_ms if given.
        from_ms: Start of an explicit time window, Unix ms. Used only if
            time_range is not given.
        to_ms: End of an explicit time window, Unix ms.
        max_wait_seconds: How long to keep polling a slow query before
            giving up and returning whatever's available.
        poll_interval_seconds: Delay between polls.

    Returns:
        The raw API response. On success this includes an "events" list;
        if the query is still processing after max_wait_seconds, the
        response won't have "events" yet — check for that key rather than
        assuming success.
    """
    body = {
        "logs": log_ids,
        "leql": {"statement": statement, "during": _build_during(time_range, from_ms, to_ms)},
    }
    initial = send_idr_request(
        "POST", config["idr_base"], f"{_LOG_SEARCH_BASE}/query/logs", config["api_key"], json_body=body
    )
    return _poll_until_ready(config, initial, max_wait_seconds, poll_interval_seconds)


def list_saved_queries(config: Dict[str, str]) -> Dict[str, Any]:
    """List all saved queries for this account."""
    return send_idr_request("GET", config["idr_base"], f"{_LOG_SEARCH_BASE}/query/saved_queries", config["api_key"])


def create_saved_query(
    config: Dict[str, str],
    name: str,
    log_ids: List[str],
    statement: str,
    time_range: Optional[str] = None,
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a saved query.

    The request body must be wrapped in a top-level "saved_query" key.
    """
    body = {
        "saved_query": {
            "name": name,
            "logs": log_ids,
            "leql": {"statement": statement, "during": _build_during(time_range, from_ms, to_ms)},
        }
    }
    return send_idr_request(
        "POST", config["idr_base"], f"{_LOG_SEARCH_BASE}/query/saved_queries", config["api_key"], json_body=body
    )


def delete_saved_query(config: Dict[str, str], saved_query_id: str) -> Dict[str, Any]:
    """Delete a saved query by ID. Returns an empty dict on success (API returns 204)."""
    return send_idr_request(
        "DELETE",
        config["idr_base"],
        f"{_LOG_SEARCH_BASE}/query/saved_queries/{saved_query_id}",
        config["api_key"],
    )


def run_saved_query(
    config: Dict[str, str],
    saved_query_id: str,
    max_wait_seconds: float = _DEFAULT_MAX_WAIT_SECONDS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
) -> Dict[str, Any]:
    """Run a previously saved query and return matching events.

    Note the path is /query/saved_query/{id} (singular) — a different,
    real path from /query/saved_queries (plural, used for list/create).
    """
    initial = send_idr_request(
        "GET",
        config["idr_base"],
        f"{_LOG_SEARCH_BASE}/query/saved_query/{saved_query_id}",
        config["api_key"],
    )
    return _poll_until_ready(config, initial, max_wait_seconds, poll_interval_seconds)
