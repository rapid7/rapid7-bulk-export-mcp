"""Render the multi-org dashboard cards as one self-contained HTML document.

The server renders the report, not the AI, so every run of the same data gives the same
document and no number is ever retyped. Every value is HTML-escaped because the data
carries host names, titles and solution text that reach a browser.

The look follows the RDS Callisto dark theme: a small token layer, Inter embedded as
base64, and the Rapid7 logo as inline SVG, so the file opens offline. Inter is under the
SIL Open Font License, which ships beside the font files in assets/.
"""

import base64
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

_ASSETS = Path(__file__).parent / "assets"

SERIES_COLOURS = ("#77b8ec", "#ff3d82", "#7262c4")
"""RDS categorical data-viz colours, used in order so each org keeps one colour."""

HEAT_RAMP = (
    ("#ffe100", "#181d1f"),
    ("#ffb800", "#181d1f"),
    ("#ff9800", "#181d1f"),
    ("#b72935", "#ffffff"),
    ("#982c35", "#ffffff"),
)
"""RDS Callisto dark heat ramp, warning to critical, as (background, text) per quintile."""

_STYLE = """
:root {
  --rds-bg-default: #181d1f; --rds-bg-paper: #242b2e; --rds-bg-subtle: rgba(248,250,250,0.04);
  --rds-divider: rgba(248,250,250,0.12);
  --rds-text-primary: #ffffff; --rds-text-secondary: rgba(248,250,250,0.66);
  --rds-text-tertiary: rgba(248,250,250,0.4);
  --rds-brand-accent: #e85e25;
  --rds-status-critical: #ee414f; --rds-status-high: #ff903d; --rds-status-medium: #ffb800;
  --rds-status-low: #78a3c6; --rds-status-healthy: #4edaca;
  --rds-fs-display2: 20px; --rds-fs-h4: 16px; --rds-fs-body1: 14px; --rds-fs-subtitle1: 13px;
  --rds-fs-body2: 12px; --rds-fs-overline: 11px;
  --rds-font-body: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  --rds-font-code: ui-monospace, SFMono-Regular, Menlo, monospace;
}
* { box-sizing: border-box; }
body { font-family: var(--rds-font-body); font-size: var(--rds-fs-body1); color: var(--rds-text-primary);
  background: var(--rds-bg-default); padding: 32px; line-height: 1.6; margin: 0; }
.wrap { max-width: 1120px; margin: 0 auto; }
.topbar { margin: 0 0 20px; }
.topbar svg { height: 22px; width: auto; color: var(--rds-text-primary); display: block; }
h1 { font-size: var(--rds-fs-display2); font-weight: 500; margin: 0 0 4px; letter-spacing: -0.2px; }
h3 { font-size: var(--rds-fs-body2); font-weight: 500; color: var(--rds-text-secondary); margin: 14px 0 6px; }
.sub { font-size: var(--rds-fs-subtitle1); color: var(--rds-text-secondary); margin: 0 0 12px; }
.strip { background: var(--rds-bg-subtle); border-radius: 8px; padding: 9px 14px;
  font-size: var(--rds-fs-subtitle1); color: var(--rds-text-secondary); margin-bottom: 14px; }
.strip.warning { border-left: 3px solid var(--rds-status-critical); color: var(--rds-text-primary); }
.card { background: var(--rds-bg-paper); border: 1px solid var(--rds-divider); border-radius: 10px;
  padding: 14px 16px; margin-bottom: 16px; }
.sec { font-size: var(--rds-fs-body2); color: var(--rds-text-secondary); font-weight: 500; margin: 0 0 10px; }
.hint { font-size: var(--rds-fs-overline); color: var(--rds-text-tertiary); margin: 0 0 8px; }
.inlinenote { font-size: var(--rds-fs-overline); color: var(--rds-text-tertiary); margin-top: 6px; line-height: 1.5; }
.tiles { display: flex; gap: 12px; flex-wrap: wrap; }
.tile { flex: 1 1 0; min-width: 140px; background: var(--rds-bg-subtle); border-radius: 8px; padding: 12px; }
.tile .k { font-size: var(--rds-fs-overline); color: var(--rds-text-secondary); }
.tile .v { font-size: var(--rds-fs-display2); font-weight: 500; line-height: 1.25; font-variant-numeric: tabular-nums; }
.tile .n { font-size: var(--rds-fs-overline); color: var(--rds-text-tertiary); margin-top: 2px; }
.scroll { overflow-x: auto; }
table.cmp { width: 100%; border-collapse: collapse; font-size: var(--rds-fs-body2); }
table.cmp th, table.cmp td { padding: 6px 8px; border-bottom: 1px solid var(--rds-divider); text-align: left; }
table.cmp thead th { font-weight: 500; color: var(--rds-text-secondary); font-size: var(--rds-fs-overline); }
table.cmp .n { text-align: right; font-variant-numeric: tabular-nums; }
table.mx { border-spacing: 2px; border-collapse: separate; font-size: var(--rds-fs-body2); }
table.mx th { font-weight: 500; color: var(--rds-text-secondary); font-size: var(--rds-fs-overline);
  text-align: right; padding: 2px 8px; }
table.mx th.rh { text-align: left; }
table.mx td { text-align: right; padding: 5px 9px; border-radius: 4px; font-variant-numeric: tabular-nums; }
table.mx td.rl { background: var(--rds-bg-subtle); text-align: left; font-size: var(--rds-fs-overline); }
.swatch { display: inline-block; width: 8px; height: 8px; border-radius: 2px; margin-right: 6px; vertical-align: 1px; }
"""

_LEFT_OUT = '<p class="inlinenote">Left out: this combines organisations, and coverage is incomplete.</p>'


@lru_cache(maxsize=1)
def _font_faces() -> str:
    rules = []
    for weight in (400, 500):
        data = base64.b64encode((_ASSETS / f"inter-latin-{weight}-normal.woff2").read_bytes()).decode("ascii")
        rules.append(
            f'@font-face{{font-family:"Inter";font-style:normal;font-weight:{weight};font-display:swap;'
            f'src:url(data:font/woff2;base64,{data}) format("woff2");}}'
        )
    return "\n".join(rules)


@lru_cache(maxsize=1)
def _logo() -> str:
    return (_ASSETS / "rapid7-logo.svg").read_text(encoding="utf-8").strip()


_LABELS = {"orgId": "Organisation", "by_org": "By organisation"}


def _label(key: str) -> str:
    if key in _LABELS:
        return _LABELS[key]
    words = []
    for part in key.replace("_", " ").split():
        words.append(part if any(ch.isupper() for ch in part[1:]) else part.lower())
    text = " ".join(words)
    return text[:1].upper() + text[1:]


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _number_text(value: Any) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    return _decimal_text(value)


def _decimal_text(value: Any) -> str:
    """Thousands separators, trailing zeros dropped: 617381.3 and 0.9422 stay as written."""
    number = Decimal(str(value))
    if not number.is_finite():
        return ""
    text = f"{number.copy_abs() if number.is_zero() else number:,f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if _is_number(value):
        return _number_text(value)
    return escape(str(value))


def _when(value: str) -> str:
    """An ISO timestamp as "2026-11-09 21:58 UTC"; any other text is returned unchanged."""
    try:
        moment = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return value
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M UTC")


def _single_value_tiles(rows: List[Dict[str, Any]]) -> Optional[str]:
    """One combined row of numbers reads better as tiles than as a one-row table."""
    if len(rows) != 1 or "orgId" in rows[0] or not all(_is_number(v) for v in rows[0].values()):
        return None
    return _tiles([(_label(key), value, "") for key, value in rows[0].items()])


def _org_cell(org: Any, colours: Dict[str, str]) -> str:
    colour = colours.get(str(org))
    swatch = f'<span class="swatch" style="background:{colour}"></span>' if colour else ""
    return f"<td>{swatch}{_text(org)}</td>"


def _table(rows: List[Dict[str, Any]], colours: Dict[str, str]) -> str:
    if not rows:
        return '<p class="hint">No rows.</p>'
    tiles = _single_value_tiles(rows)
    if tiles is not None:
        return tiles
    columns = list(rows[0].keys())
    numeric = {c for c in columns if all(_is_number(r.get(c)) or r.get(c) is None for r in rows)}
    number_head = ' class="n"'
    head = "".join(f"<th{number_head if c in numeric else ''}>{escape(_label(c))}</th>" for c in columns)
    body = []
    for row in rows:
        cells = []
        for c in columns:
            if c == "orgId":
                cells.append(_org_cell(row.get(c), colours))
            elif c in numeric:
                cells.append(f'<td class="n">{_text(row.get(c))}</td>')
            else:
                cells.append(f"<td>{_text(row.get(c))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return (
        f'<div class="scroll"><table class="cmp"><thead><tr>{head}</tr></thead>'
        f"<tbody>{''.join(body)}</tbody></table></div>"
    )


def _tiles(items: Sequence[Tuple[str, Any, str]]) -> str:
    tiles = "".join(
        f'<div class="tile"><div class="k">{escape(k)}</div><div class="v">{_text(v)}</div>'
        f'<div class="n">{escape(note)}</div></div>'
        for k, v, note in items
    )
    return f'<div class="tiles">{tiles}</div>'


def _portfolio_tiles(rows: List[Dict[str, Any]], colours: Dict[str, str]) -> str:
    if not rows:
        return '<p class="hint">No rows.</p>'
    row = rows[0]
    assets = row.get("assets_with_findings")
    across = f"across {_number_text(assets)} assets" if _is_number(assets) else ""
    return _tiles(
        [
            ("Active findings", row.get("active_total"), across),
            ("Active critical", row.get("critical"), ""),
            ("Active high", row.get("high"), ""),
            ("Active medium", row.get("medium"), ""),
        ]
    )


def _headline_tiles(rows: List[Dict[str, Any]], colours: Dict[str, str]) -> str:
    if not rows:
        return '<p class="hint">No rows.</p>'
    row = rows[0]
    return _tiles(
        [
            ("Weaponised critical findings", row.get("findings"), ""),
            ("Assets affected", row.get("assets"), ""),
            ("Distinct vulnerabilities", row.get("distinct_vulns"), ""),
            ("Median days open", row.get("median_days_open"), f"mean {_text(row.get('mean_days_open'))}"),
        ]
    )


def _heat_table(rows: List[Dict[str, Any]], colours: Dict[str, str]) -> str:
    """Age band by severity, each cell shaded by the quintile of its count among all cells."""
    if not rows:
        return '<p class="hint">No rows.</p>'
    severities = [c for c in rows[0] if c != "band"]
    values = sorted(v for r in rows for c in severities if _is_number(v := r.get(c)))

    def shade(value: Any) -> str:
        if not _is_number(value) or not values:
            return "<td></td>"
        below = sum(1 for v in values if v < value)
        step = below * (len(HEAT_RAMP) - 1) // max(1, len(values) - 1)
        background, text = HEAT_RAMP[step]
        return f'<td style="background:{background};color:{text}">{_text(value)}</td>'

    head = "".join(f"<th>{escape(_label(c))}</th>" for c in severities)
    body = "".join(
        f'<tr><td class="rl">{_text(r.get("band"))}</td>' + "".join(shade(r.get(c)) for c in severities) + "</tr>"
        for r in rows
    )
    return (
        f'<table class="mx"><thead><tr><th class="rh">Days active</th>{head}</tr></thead><tbody>{body}</tbody></table>'
    )


_FEATURED: Dict[Tuple[str, str], Callable[[List[Dict[str, Any]], Dict[str, str]], str]] = {
    ("portfolio_summary", "portfolio"): _portfolio_tiles,
    ("unresolved_age_severity", "by_band"): _heat_table,
    ("weaponised_critical", "headline"): _headline_tiles,
}


def _card_section(card: Dict[str, Any], number: int, coverage_complete: bool, colours: Dict[str, str]) -> str:
    dates = [_when(d) for d in sorted(set(card.get("as_of_by_org", {}).values())) or [card["as_of"]]]
    date_text = dates[0] if len(dates) == 1 else f"from {dates[0]} to {dates[-1]}, by organisation"
    orgs = ", ".join(map(str, card["orgs"]))
    parts = [
        f'<section class="card" id="{escape(card["card"])}">',
        f'<p class="sec">{number} &middot; {escape(card["title"])}</p>',
        f'<p class="hint">Data date: {escape(date_text)}. Organisations: {escape(orgs)}.</p>',
    ]
    per_org = set(card.get("per_org", ()))
    featured_first = sorted(card["results"].items(), key=lambda item: (card["card"], item[0]) not in _FEATURED)
    for name, rows in featured_first:
        parts.append(f"<h3>{escape(_label(name))}</h3>")
        if not coverage_complete and name not in per_org:
            parts.append(_LEFT_OUT)
        else:
            render = _FEATURED.get((card["card"], name), _table)
            parts.append(render(rows, colours))
    parts.append(f'<p class="inlinenote">{escape(card["definition"])}</p>')
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
    colours = {org: SERIES_COLOURS[i % len(SERIES_COLOURS)] for i, org in enumerate(orgs)}
    if coverage_complete:
        coverage = '<div class="strip">Organisation coverage check: complete.</div>'
    else:
        coverage = (
            '<div class="strip warning"><strong>Coverage is incomplete.</strong> '
            f"{escape(coverage_message or 'An organisation is missing or was not loaded today.')} "
            "Figures that combine organisations are left out.</div>"
        )
    header = [
        "<!doctype html>",
        '<html lang="en" data-rds-brand="callisto" data-rds-mode="dark"><head><meta charset="utf-8">',
        "<title>Multi-Org Exposure Report</title>",
        f'<style>{_font_faces()}\n{_STYLE}</style></head><body><div class="wrap">',
        f'<div class="topbar">{_logo()}</div>',
        "<h1>Multi-Org Exposure Report</h1>",
        f'<p class="sub">Generated {escape(_when(generated_at))} &middot; {len(orgs)} organisation(s) &middot; '
        "every figure comes from a fixed, tested query</p>",
        coverage,
    ]
    body = [_card_section(card, i, coverage_complete, colours) for i, card in enumerate(cards, start=1)]
    return "\n".join(header + body + ["</div></body></html>"])
