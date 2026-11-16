"""Tests for the get_rapid7_dashboard_card MCP tool."""

import inspect
import json
import threading
from datetime import timezone

import pytest

from src import mcp_server
from src.dashboard_queries import CARDS, run_card
from tests.test_dashboard_queries import _load


def _utc(when):
    return when.replace(tzinfo=timezone.utc).isoformat()


@pytest.fixture
def loaded(tmp_path, monkeypatch):
    database = _load(tmp_path)
    monkeypatch.setattr(mcp_server, "db", database)
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", False)
    yield database
    database.close()


@pytest.mark.parametrize("card", list(CARDS))
def test_returns_each_card_as_json(loaded, card):
    result = json.loads(mcp_server.get_rapid7_dashboard_card(card))

    assert result["card"] == card
    assert result["orgs"] == ["ORG-A", "ORG-B"]
    assert set(result["results"]) == {name for name, _ in CARDS[card].queries}


def test_unknown_card_names_the_valid_cards(loaded):
    result = mcp_server.get_rapid7_dashboard_card("no_such_card")

    assert result.startswith("✗ Unknown card 'no_such_card'")
    assert "portfolio_summary" in result


def test_refuses_while_a_load_holds_the_lock(loaded):
    """_db_lock is re-entrant, so the lock must be held from another thread, as a real load does."""
    held, release = threading.Event(), threading.Event()

    def _load_in_progress():
        with mcp_server._db_lock:
            held.set()
            release.wait(timeout=10)

    loader = threading.Thread(target=_load_in_progress)
    loader.start()
    held.wait(timeout=10)
    try:
        result = mcp_server.get_rapid7_dashboard_card("portfolio_summary")
    finally:
        release.set()
        loader.join(timeout=10)

    assert result.startswith("⏳")


def test_refuses_before_the_first_refresh_in_a_hosted_deployment(loaded, monkeypatch):
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", True)

    assert mcp_server.get_rapid7_dashboard_card("portfolio_summary") == mcp_server._NO_DATA_MESSAGE


def test_refuses_with_no_data_loaded(monkeypatch):
    monkeypatch.setattr(mcp_server, "db", None)
    monkeypatch.setattr(mcp_server, "_AWAITING_FIRST_REFRESH", False)

    assert mcp_server.get_rapid7_dashboard_card("portfolio_summary").startswith("Error: No data loaded")


def test_docstring_lists_exactly_the_registered_cards():
    """The AI picks a card from the docstring, so a card added to CARDS must be listed there."""
    doc = inspect.getdoc(mcp_server.get_rapid7_dashboard_card)
    listed = doc.split("card: One of:", 1)[1].split("Returns:", 1)[0]

    assert {line.strip()[2:].split(":", 1)[0] for line in listed.splitlines() if line.strip().startswith("- ")} == set(
        CARDS
    )


def test_numbers_are_the_card_query_results(loaded):
    result = json.loads(mcp_server.get_rapid7_dashboard_card("org_comparison"))

    assert result["results"] == run_card(loaded, "org_comparison")["results"]


def test_hosted_output_stays_json_and_ages_the_cards_own_data_date(loaded, monkeypatch):
    monkeypatch.setattr(mcp_server, "storage_configured", lambda: True)

    result = json.loads(mcp_server.get_rapid7_dashboard_card("portfolio_summary"))

    assert result["as_of"] == _utc(loaded.get_load_metadata()["vulnerabilities"])
    assert result["data_loaded"] in ("less than a minute ago", "1 minute ago")


def test_local_output_carries_no_data_age(loaded, monkeypatch):
    monkeypatch.setattr(mcp_server, "storage_configured", lambda: False)

    assert "data_loaded" not in json.loads(mcp_server.get_rapid7_dashboard_card("portfolio_summary"))


def test_a_query_failure_is_redacted_and_releases_the_lock(loaded, monkeypatch):
    secret = "test-not-a-real-key"
    monkeypatch.setenv("RAPID7_API_KEY", secret)

    def _failing_run_card(db, card):
        raise RuntimeError(f"connection failed with key {secret}")

    monkeypatch.setattr(mcp_server, "run_card", _failing_run_card)

    result = mcp_server.get_rapid7_dashboard_card("portfolio_summary")

    assert result.startswith("✗ Error building dashboard card 'portfolio_summary'")
    assert secret not in result
    assert _free_for_another_thread(mcp_server._db_lock)


def _free_for_another_thread(lock) -> bool:
    """An RLock always re-enters for its holder, so only another thread can see it still held."""
    outcome = []

    def _try():
        got = lock.acquire(blocking=False)
        outcome.append(got)
        if got:
            lock.release()

    probe = threading.Thread(target=_try)
    probe.start()
    probe.join(timeout=10)
    return outcome == [True]


def test_coverage_checks_configured_orgs_on_a_server_with_no_keys(loaded, tmp_path, monkeypatch):
    """A read-only server holds no keys, but coverage still needs to know which orgs to expect."""
    orgs_file = tmp_path / "orgs.json"
    orgs_file.write_text(json.dumps({"orgs": [{"label": "fogcutter", "key_ref": "R7_KEY_NOT_SET"}]}))
    monkeypatch.setenv("RAPID7_ORGS_FILE", str(orgs_file))
    monkeypatch.delenv("R7_KEY_NOT_SET", raising=False)
    monkeypatch.setattr("src.config._get_key_from_keychain", lambda _: None)

    coverage = mcp_server.get_rapid7_org_coverage()

    assert "Configured organizations: 1" in coverage
