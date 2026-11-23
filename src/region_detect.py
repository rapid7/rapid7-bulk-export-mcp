"""Find which Rapid7 region an API key belongs to.

A key works only at its own region's endpoint. Every region is sent one read-only status
query for an export that does not exist. The home region answers "not found", and every
other region refuses the key with HTTP 401. The query creates nothing.
"""

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

import requests

from .config import REGION_ENDPOINTS
from .graphql_client import RegionUnavailableError, send_graphql_request

TIMEOUT_SECONDS = (5, 10)
"""Shorter than an export's timeouts, so a region that is down costs seconds, not a minute."""

GRACE_SECONDS = 2.0
"""How long to wait for the other regions once one region accepts the key."""

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
    """Ask every region at once whether it accepts api_key.

    Once one region accepts, the others get GRACE_SECONDS to answer before the result is
    decided, so a region that is down costs seconds only for a key that belongs to it.
    """
    regions = list(REGION_ENDPOINTS)
    answers: "queue.Queue" = queue.Queue()
    for region in regions:
        # Daemon threads: a region that is slow to answer never holds up the command or its exit.
        threading.Thread(
            target=lambda r=region: answers.put((r, _accepts(REGION_ENDPOINTS[r], api_key))), daemon=True
        ).start()

    outcomes = {}
    give_up = time.monotonic() + sum(TIMEOUT_SECONDS) + 5
    accepted_by = None
    while len(outcomes) < len(regions):
        limit = min(give_up, accepted_by + GRACE_SECONDS) if accepted_by else give_up
        wait = limit - time.monotonic()
        if wait <= 0:
            break
        try:
            region, outcome = answers.get(timeout=wait)
        except queue.Empty:
            break
        outcomes[region] = outcome
        if outcome == "accepted" and accepted_by is None:
            accepted_by = time.monotonic()

    accepted = [r for r in regions if outcomes.get(r) == "accepted"]
    # A region still silent is treated as unavailable: it answered neither yes nor no in time.
    unavailable = [r for r in regions if outcomes.get(r, "unavailable") == "unavailable"]
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
