"""The assistant must warn before answering with a figure the Rapid7 export does not supply."""

from pathlib import Path

import anyio
import pytest

from src import mcp_server

SKILL = Path(__file__).resolve().parents[1] / "skills" / "rapid7-bulk-export" / "SKILL.md"
MISSING = ["CISA KEV", "0 to 1000", "trends", "MTTR"]


def _served(kind, name):
    listed = (
        mcp_server.mcp._local_provider._list_tools if kind == "tool" else mcp_server.mcp._local_provider._list_prompts
    )
    return {item.name: item for item in anyio.run(listed)}[name]


def test_the_report_command_tells_the_assistant_to_warn():
    text = _served("prompt", "multi_org_report").fn()

    assert mcp_server.UNSUPPORTED_AGGREGATIONS_WARNING in text


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(mcp_server.UNSUPPORTED_AGGREGATIONS_WARNING, id="report command"),
        pytest.param(_served("tool", "query_rapid7").description, id="query_rapid7 description"),
        pytest.param(SKILL.read_text(encoding="utf-8"), id="skill"),
    ],
)
def test_every_place_names_each_missing_figure_and_forbids_silent_approximation(text):
    for figure in MISSING:
        assert figure in text, figure
    assert "never approximate" in text.lower()
    assert "stand-in" in text


def test_the_skill_keeps_remediation_open_for_a_single_org_setup():
    """Remediation is missing only per organisation in multi-org mode, so single-org analysis must not be blocked."""
    skill = SKILL.read_text(encoding="utf-8")
    section = skill.split("## Figures the export cannot supply", 1)[1].split("\n## ", 1)[0]

    assert "Not available per organization in multi-org mode" in section
    assert "In a single-organization setup the\n  `vulnerability_remediation` table does supply these" in section


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(mcp_server.UNSUPPORTED_AGGREGATIONS_WARNING, id="report command"),
        pytest.param(_served("tool", "query_rapid7").description, id="query_rapid7 description"),
        pytest.param(SKILL.read_text(encoding="utf-8"), id="skill"),
    ],
)
def test_kev_comes_only_from_the_cards_never_from_has_exploits(text):
    """The cards can supply KEV from CISA's catalogue. Anywhere else, KEV must not be guessed."""
    flat = " ".join(text.split())
    assert "KEV figures when they are not null" in flat or "use those card figures" in flat
    assert "never derive KEV from hasExploits" in flat or "never derive it from `hasExploits`" in flat
