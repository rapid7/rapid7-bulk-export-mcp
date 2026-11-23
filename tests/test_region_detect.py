"""A key's region is the one endpoint that accepts it. Every other region refuses it with HTTP 401."""

import pytest
import requests
import responses

from src import graphql_client, region_detect
from src.config import REGION_ENDPOINTS
from src.graphql_client import RegionUnavailableError


def _platform(home=None, down=(), refuses_all=False):
    """Stand in for every region: home accepts the key, down regions are unavailable, the rest refuse it."""

    def _send(endpoint, api_key, query, variables=None, timeout=None):
        region = next(name for name, url in REGION_ENDPOINTS.items() if url == endpoint)
        if region in down:
            raise RegionUnavailableError(f"{region} answered HTTP 504")
        if region == home and not refuses_all:
            raise ValueError("GraphQL errors: export not found")
        response = requests.Response()
        response.status_code = 401
        raise requests.HTTPError("401", response=response)

    return _send


def test_the_one_region_that_accepts_the_key_is_its_region(monkeypatch):
    monkeypatch.setattr(region_detect, "send_graphql_request", _platform(home="eu"))

    result = region_detect.detect_region("key")

    assert (result.region, result.unavailable) == ("eu", [])


def test_a_down_region_is_reported_but_does_not_hide_the_home_region(monkeypatch):
    monkeypatch.setattr(region_detect, "send_graphql_request", _platform(home="us", down=("me1",)))

    result = region_detect.detect_region("key")

    assert (result.region, result.unavailable) == ("us", ["me1"])


def test_no_region_accepting_the_key_while_one_is_down_names_the_down_region(monkeypatch):
    monkeypatch.setattr(region_detect, "send_graphql_request", _platform(home="me1", down=("me1",)))

    result = region_detect.detect_region("key")

    assert result.region is None
    assert "me1" in result.problem and "unavailable" in result.problem


def test_a_key_no_region_accepts_is_reported_as_not_valid(monkeypatch):
    monkeypatch.setattr(region_detect, "send_graphql_request", _platform(refuses_all=True))

    result = region_detect.detect_region("key")

    assert result.region is None
    assert "No region accepted this key" in result.problem


def test_two_regions_accepting_one_key_is_refused_rather_than_guessed(monkeypatch):
    def _send(endpoint, api_key, query, variables=None, timeout=None):
        if endpoint in (REGION_ENDPOINTS["us"], REGION_ENDPOINTS["eu"]):
            raise ValueError("GraphQL errors: export not found")
        response = requests.Response()
        response.status_code = 401
        raise requests.HTTPError("401", response=response)

    monkeypatch.setattr(region_detect, "send_graphql_request", _send)

    result = region_detect.detect_region("key")

    assert result.region is None and "more than one region" in result.problem


def test_every_region_is_asked_once_with_the_short_timeout_and_a_read_only_query(monkeypatch):
    calls = []

    def _send(endpoint, api_key, query, variables=None, timeout=None):
        calls.append((endpoint, query, timeout))
        raise ValueError("GraphQL errors: export not found")

    monkeypatch.setattr(region_detect, "send_graphql_request", _send)
    region_detect.detect_region("key")

    assert sorted(c[0] for c in calls) == sorted(REGION_ENDPOINTS.values())
    assert all(c[2] == region_detect.TIMEOUT_SECONDS == (5, 10) for c in calls)
    assert all(c[1].startswith("query {") and "mutation" not in c[1] for c in calls)


@responses.activate
def test_the_client_uses_a_timeout_it_is_given_and_its_default_otherwise(monkeypatch):
    seen = []
    real_post = graphql_client.requests.post

    def _post(*args, timeout, **kwargs):
        seen.append(timeout)
        return real_post(*args, timeout=timeout, **kwargs)

    endpoint = REGION_ENDPOINTS["us"]
    responses.add(responses.POST, endpoint, json={"data": {}})
    monkeypatch.setattr(graphql_client.requests, "post", _post)

    graphql_client.send_graphql_request(endpoint, "key", "query { x }")
    graphql_client.send_graphql_request(endpoint, "key", "query { x }", timeout=(5, 10))

    assert seen == [graphql_client.TIMEOUT_SECONDS, (5, 10)]


@pytest.mark.parametrize("status", [403, 500])
def test_any_other_refusal_counts_as_not_this_region(monkeypatch, status):
    def _send(endpoint, api_key, query, variables=None, timeout=None):
        if endpoint == REGION_ENDPOINTS["ca"]:
            raise ValueError("GraphQL errors: export not found")
        response = requests.Response()
        response.status_code = status
        raise requests.HTTPError(str(status), response=response)

    monkeypatch.setattr(region_detect, "send_graphql_request", _send)

    assert region_detect.detect_region("key").region == "ca"


def test_a_slow_region_does_not_hold_up_a_key_another_region_accepted(monkeypatch):
    import threading
    import time

    release = threading.Event()

    def _send(endpoint, api_key, query, variables=None, timeout=None):
        if endpoint == REGION_ENDPOINTS["me1"]:
            release.wait(5)
            raise RegionUnavailableError("me1 did not respond in time")
        return _platform(home="us")(endpoint, api_key, query, variables, timeout)

    monkeypatch.setattr(region_detect, "send_graphql_request", _send)
    monkeypatch.setattr(region_detect, "GRACE_SECONDS", 0.2)

    started = time.monotonic()
    result = region_detect.detect_region("key")
    elapsed = time.monotonic() - started
    release.set()

    assert (result.region, result.unavailable) == ("us", ["me1"])
    assert elapsed < 2, "the answer comes once the home region accepts, not when the slow region times out"
