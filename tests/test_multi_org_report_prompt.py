"""Tests for the multi_org_report prompt, which clients offer as a command."""

import anyio

from src import mcp_server
from src.dashboard_queries import CARDS


def _served_prompt():
    prompts = {p.name: p for p in anyio.run(mcp_server.mcp._local_provider._list_prompts)}
    assert "multi_org_report" in prompts, f"prompt not registered, found {sorted(prompts)}"
    return prompts["multi_org_report"]


def _served_text():
    return _served_prompt().fn()


def test_the_report_command_is_registered_with_a_title_and_description():
    prompt = _served_prompt()

    assert prompt.title == "Multi-Org Exposure Report"
    assert "most recently loaded exports" in prompt.description


def test_it_names_every_registered_card():
    """A card added to CARDS must appear in the report without editing the prompt."""
    text = _served_text()

    for name, card in CARDS.items():
        assert name in text and card.title in text, name


def test_it_checks_coverage_before_any_card():
    text = _served_text()

    assert text.index("get_rapid7_org_coverage()") < text.index("get_rapid7_dashboard_card(card)")


def test_it_uses_the_last_loaded_exports_and_never_writes_its_own_sql():
    text = _served_text()

    assert "Do not start or download any export" in text
    assert "Do not call query_rapid7" in text


def test_it_drops_portfolio_totals_when_coverage_warns():
    assert "leave out every portfolio total" in _served_text()
