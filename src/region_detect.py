"""Find which Rapid7 region an API key belongs to.

A key works only at its own region's endpoint. Every region is sent one read-only status
query for an export that does not exist. The home region answers "not found", and every
other region refuses the key with HTTP 401. The query creates nothing.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import List, Optional

import requests

from .config import REGION_ENDPOINTS
from .graphql_client import RegionUnavailableError, send_graphql_request

TIMEOUT_SECONDS = (5, 10)
"""Shorter than an export's timeouts, so a region that is down costs seconds, not a minute."""

_QUERY = 'query { export(id: "rapid7-orgs-region-check") { id } }'


@dataclass
class Detection:
    """The key's region, or None with the reason no single region could be named."""

    region: Optional[str]
    unavailable: List[str] = field(default_factory=list)
    problem: str = ""


def _accepts(endpoint: str, api_key: str) -> str:
    try:
        send_graphql_request(endpoint, api_key, _QUERY, timeout=TIMEOUT_SECONDS)
    except ValueError:
        # A GraphQL error ("export not found") means the region accepted the key.
        return "accepted"
    except RegionUnavailableError:
        return "unavailable"
    except requests.RequestException:
        return "refused"
    return "accepted"


def detect_region(api_key: str) -> Detection:
    """Ask every region at once whether it accepts api_key."""
    regions = list(REGION_ENDPOINTS)
    with ThreadPoolExecutor(max_workers=len(regions)) as pool:
        outcomes = dict(zip(regions, pool.map(lambda r: _accepts(REGION_ENDPOINTS[r], api_key), regions)))

    accepted = [r for r in regions if outcomes[r] == "accepted"]
    unavailable = [r for r in regions if outcomes[r] == "unavailable"]
    if len(accepted) == 1:
        return Detection(accepted[0], unavailable)
    if len(accepted) > 1:
        return Detection(None, unavailable, f"The key was accepted in more than one region: {', '.join(accepted)}.")
    if unavailable:
        return Detection(
            None,
            unavailable,
            f"No available region accepted this key, and {', '.join(unavailable)} is unavailable right now. "
            f"If the organization is in {', '.join(unavailable)}, try again later.",
        )
    return Detection(None, [], "No region accepted this key. Check that it is a current Organization API key.")
