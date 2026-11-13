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


def test_the_server_builds_the_report_and_the_ai_does_not_retype_it():
    text = _served_text()

    assert text.index("generate_rapid7_dashboard_report()") < text.index("get_rapid7_dashboard_card(card)")
    assert "Do not retype the report's figures" in text


def test_it_uses_the_last_loaded_exports_and_never_writes_its_own_sql():
    text = _served_text()

    assert "Do not start or download any export" in text
    assert "Do not call query_rapid7" in text


def test_it_tells_the_user_the_report_left_out_portfolio_totals_when_coverage_warns():
    assert "the report leaves out every portfolio total" in _served_text()


def test_it_gives_the_user_the_clickable_link():
    assert "clickable link on its Open it line" in _served_text()
