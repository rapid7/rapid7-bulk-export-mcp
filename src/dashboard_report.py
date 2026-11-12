"""Render the multi-org dashboard cards as one self-contained HTML document.

The server renders the report, not the AI, so every run of the same data gives the same
document and no number is ever retyped. Every value is HTML-escaped because the data
carries host names, titles and solution text that reach a browser.
"""

from decimal import Decimal
from html import escape
from typing import Any, Dict, Iterable, List, Optional

_STYLE = """
body { font-family: -apple-system, "Segoe UI", Roboto, sans-serif; margin: 2rem; color: #1b1f24; }
h1 { margin-bottom: 0.25rem; }
.meta, .definition, .note { color: #57606a; }
.warning { background: #fff4e5; border-left: 4px solid #d9822b; padding: 0.75rem 1rem; }
section { margin: 2rem 0; }
table { border-collapse: collapse; margin: 0.5rem 0 1rem; }
th, td { border: 1px solid #d0d7de; padding: 0.3rem 0.6rem; text-align: left; }
td.number { text-align: right; font-variant-numeric: tabular-nums; }
th { background: #f6f8fa; }
"""


def _label(key: str) -> str:
    if key == "orgId":
        return "Organisation"
    words = []
    for part in key.replace("_", " ").split():
        words.append(part if any(ch.isupper() for ch in part[1:]) else part.lower())
    text = " ".join(words)
    return text[:1].upper() + text[1:]


def _cell(value: Any) -> str:
    if isinstance(value, bool):
        return f"<td>{'yes' if value else 'no'}</td>"
    if isinstance(value, int):
        return f'<td class="number">{value:,}</td>'
    if isinstance(value, (float, Decimal)):
        return f'<td class="number">{_decimal_text(value)}</td>'
    if value is None:
        return "<td></td>"
    return f"<td>{escape(str(value))}</td>"


def _decimal_text(value: Any) -> str:
    """Thousands separators, trailing zeros dropped: 617381.3 and 0.9422 stay as written."""
    text = f"{Decimal(str(value)):,f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def _table(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return '<p class="note">No rows.</p>'
    columns = list(rows[0].keys())
    head = "".join(f"<th>{escape(_label(c))}</th>" for c in columns)
    body = "".join("<tr>" + "".join(_cell(row.get(c)) for c in columns) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def is_cross_org(rows: Iterable[Dict[str, Any]]) -> bool:
    """True when a result aggregates across organisations, so it has no orgId column."""
    first = next(iter(rows), None)
    return first is not None and "orgId" not in first


def _card_section(card: Dict[str, Any], coverage_complete: bool) -> str:
    dates = sorted(set(card.get("as_of_by_org", {}).values())) or [card["as_of"]]
    date_text = dates[0] if len(dates) == 1 else f"from {dates[0]} to {dates[-1]}, by organisation"
    orgs = ", ".join(map(str, card["orgs"]))
    parts = [
        f'<section id="{escape(card["card"])}">',
        f"<h2>{escape(card['title'])}</h2>",
        f'<p class="definition">{escape(card["definition"])}</p>',
        f'<p class="meta">Data date (UTC): {escape(date_text)}. Organisations: {escape(orgs)}.</p>',
    ]
    for name, rows in card["results"].items():
        parts.append(f"<h3>{escape(_label(name))}</h3>")
        if not coverage_complete and is_cross_org(rows):
            parts.append('<p class="note">Left out: this combines organisations, and coverage is incomplete.</p>')
        else:
            parts.append(_table(rows))
    parts.append("</section>")
    return "\n".join(parts)


def render_report(
    cards: List[Dict[str, Any]],
    generated_at: str,
    coverage_complete: bool,
    coverage_message: Optional[str] = None,
) -> str:
    """Render the dashboard cards as one HTML document that opens offline.

    Args:
        cards: run_card() results, in display order.
        generated_at: When the report was built, as a UTC ISO timestamp.
        coverage_complete: False when an organisation is missing or stale, which leaves
            out every result that combines organisations.
        coverage_message: Shown at the top when coverage is incomplete.
    """
    orgs = sorted({str(org) for card in cards for org in card["orgs"]})
    header = [
        "<!doctype html>",
        '<html lang="en"><head><meta charset="utf-8">',
        "<title>Multi-Org Exposure Report</title>",
        f"<style>{_STYLE}</style></head><body>",
        "<h1>Multi-Org Exposure Report</h1>",
        f'<p class="meta">Generated {escape(generated_at)} (UTC) for {len(orgs)} organisation(s). '
        "Every figure comes from a fixed, tested query.</p>",
    ]
    if not coverage_complete:
        header.append(
            '<div class="warning"><strong>Coverage is incomplete.</strong> '
            f"{escape(coverage_message or 'An organisation is missing or was not loaded today.')} "
            "Figures that combine organisations are left out.</div>"
        )
    body = [_card_section(card, coverage_complete) for card in cards]
    return "\n".join(header + body + ["</body></html>"])
