"""Tests for the get_rapid7_dashboard_card MCP tool."""

import inspect
import json
import threading

import pytest

from src import mcp_server
from src.dashboard_queries import CARDS
from tests.test_dashboard_queries import _load


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
    listed = doc.split("card: One of", 1)[1].split("Returns:", 1)[0]

    assert {name.strip(" ,.\n") for name in listed.split(",")} == set(CARDS)


def test_tool_is_not_behind_the_write_scope():
    source = inspect.getsource(mcp_server.get_rapid7_dashboard_card)

    assert "WRITE_TOOL_TAG" not in source
    assert "_write_precondition" not in source
