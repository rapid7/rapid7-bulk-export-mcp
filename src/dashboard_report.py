"""Render the multi-org dashboard cards as one self-contained HTML document.

The server renders the report, not the AI, so every run of the same data gives the same
document and no number is ever retyped. Every value is HTML-escaped because the data
carries host names, titles and solution text that reach a browser.

The look follows the RDS Callisto dark theme: a small token layer, Inter embedded as
base64, and the Rapid7 logo as inline SVG, so the file opens offline. Inter is under the
SIL Open Font License, which ships beside the font files in assets/.
"""

import base64
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .report_charts import dot_plot, line_chart

_ASSETS = Path(__file__).parent / "assets"

LEAD_COLOURS = ("#77b8ec", "#ff3d82", "#7262c4")
"""The UX prototype's colours for its three lead organisations, the highest risk scores, in rank order."""
MUTED_COLOUR = "rgba(248,250,250,0.38)"
"""The prototype's colour for every other organisation, wherever it appears."""

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
table.mx { width: 100%; max-width: 660px; table-layout: fixed; border-spacing: 2px; border-collapse: separate;
  font-size: var(--rds-fs-body2); font-variant-numeric: tabular-nums; }
table.mx th { font-weight: 500; color: var(--rds-text-secondary); font-size: var(--rds-fs-overline);
  text-align: center; padding: 2px; }
table.mx th.rh { width: 24%; }
table.mx td { text-align: center; padding: 8px 2px; border-radius: 4px; }
table.mx th .range { display: block; font-weight: 400; font-size: 10px; color: var(--rds-text-tertiary); }
.headline { margin: 0 0 10px; font-size: 13px; }
.scope { margin: -4px 0 10px; font-size: var(--rds-fs-overline); color: var(--rds-status-high); }
.headline .lab { color: var(--rds-text-secondary); font-size: 13px; font-weight: 400; margin: 0; }
.headline strong { font-weight: 500; margin-left: 6px; }
table.mx td.rl { background: none; text-align: right; padding-right: 8px; color: var(--rds-text-secondary);
  font-size: var(--rds-fs-overline); }
.legend { display: flex; flex-wrap: wrap; gap: 14px; margin: 6px 0 4px; }
.legend span { font-size: var(--rds-fs-overline); color: var(--rds-text-secondary);
  display: inline-flex; align-items: center; gap: 5px; }
.legend i { width: 14px; height: 2.5px; border-radius: 2px; display: inline-block; }
details { margin-top: 6px; }
details.about { margin-top: 10px; }
summary { font-size: var(--rds-fs-overline); color: var(--rds-text-secondary); cursor: pointer; }
.chart { position: relative; }
.chart .pt { cursor: pointer; }
.chart .series:not(.marked) .mk { opacity: 0; }
.chart .series:not(.lead) .ln { opacity: 0.9; }
.chart.focusing .ln { opacity: 0.15; }
.chart.focusing .mk { opacity: 0; }
.chart.focusing .series.focus .ln { opacity: 1; stroke-width: 3.2; }
.chart.focusing .series.focus.marked .mk { opacity: 1; }
.chart .mk.on { opacity: 1; }
.chart .legend span { cursor: pointer; }
.chart .legend span.selected { color: var(--rds-text-primary); }
.chart.filtering g.series:not(.focus) { display: none; }
.chart.focusing .legend span:not(.focus) { opacity: 0.4; }
#tip { position: fixed; display: none; pointer-events: none; z-index: 10; white-space: nowrap;
  background: #ffffff; color: #181d1f; border-radius: 4px; padding: 8px 10px; font-size: 12px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.4); }
#tip .tip-heading { color: #5b676c; font-size: 11px; margin-bottom: 3px; }
#tip .tip-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.swatch { display: inline-block; width: 8px; height: 8px; border-radius: 2px; margin-right: 6px; vertical-align: 1px; }
"""

_SCRIPT = """
(function () {
  var tip = document.createElement("div");
  tip.id = "tip";
  tip.setAttribute("role", "tooltip");
  document.body.appendChild(tip);
  var pinned = null;
  var selected = new Map();
  var hovered = new Map();
  function chartOf(el) { return el.closest(".chart"); }
  function seriesOf(el) { return el.closest("[data-series]"); }
  function picked(chart) { return selected.get(chart) || new Set(); }
  function render(chart) {
    var names = new Set(picked(chart));
    if (hovered.get(chart)) { names.add(hovered.get(chart)); }
    chart.classList.toggle("focusing", names.size > 0);
    chart.classList.toggle("filtering", picked(chart).size > 0);
    chart.querySelectorAll("[data-series]").forEach(function (el) {
      el.classList.toggle("focus", names.has(el.getAttribute("data-series")));
    });
    chart.querySelectorAll(".legend span").forEach(function (el) {
      var on = picked(chart).has(el.getAttribute("data-series"));
      el.classList.toggle("selected", on);
      el.setAttribute("aria-pressed", on ? "true" : "false");
    });
  }
  function focus(chart, name) {
    if (!chart) { return; }
    hovered.set(chart, name);
    render(chart);
  }
  function axisTop(peak, counts) {
    var steps = [1, 2, 2.5, 5, 10];
    var raw = counts ? Math.max(peak, 0) / 4 : peak;
    if (counts && raw <= 1) { return 4; }
    if (!counts && raw <= 0) { return 1; }
    var power = Math.pow(10, Math.floor(Math.log10(raw)));
    for (var i = 0; i < steps.length; i++) {
      var size = steps[i] * power;
      if ((!counts || Number.isInteger(size)) && raw <= size) { return counts ? 4 * size : size; }
    }
    return counts ? 40 * power : 10 * power;
  }
  function rescale(chart) {
    var svg = chart.querySelector("svg[data-top]");
    if (!svg) { return; }
    var names = picked(chart);
    var top = Number(svg.getAttribute("data-top"));
    if (names.size) {
      var peak = 0;
      chart.querySelectorAll("g.series").forEach(function (g) {
        if (!names.has(g.getAttribute("data-series"))) { return; }
        g.querySelectorAll(".pt").forEach(function (pt) { peak = Math.max(peak, Number(pt.getAttribute("data-y"))); });
      });
      top = axisTop(peak, svg.getAttribute("data-counts") === "1");
    }
    var y0 = Number(svg.getAttribute("data-y0"));
    var y1 = Number(svg.getAttribute("data-y1"));
    chart.querySelectorAll("g.series").forEach(function (g) {
      var coords = [];
      g.querySelectorAll(".pt").forEach(function (pt) {
        var y = (y1 - (y1 - y0) * Number(pt.getAttribute("data-y")) / top).toFixed(2);
        pt.setAttribute("cy", y);
        var mark = pt.previousElementSibling;
        if (mark && mark.classList.contains("mk")) { mark.setAttribute("cy", y); }
        coords.push(pt.getAttribute("cx") + "," + y);
      });
      var ln = g.querySelector(".ln");
      if (ln) { ln.setAttribute("points", coords.join(" ")); }
    });
    svg.querySelectorAll(".ytick").forEach(function (tick, i) {
      if (!tick.hasAttribute("data-label")) { tick.setAttribute("data-label", tick.textContent); }
      tick.textContent = names.size
        ? Number((top * i / 4).toFixed(2)).toLocaleString("en-GB")
        : tick.getAttribute("data-label");
    });
  }
  function toggle(el) {
    var chart = chartOf(el);
    var names = picked(chart);
    var name = el.getAttribute("data-series");
    if (names.has(name)) { names.delete(name); } else { names.add(name); }
    selected.set(chart, names);
    render(chart);
    rescale(chart);
  }
  function clearSelections() {
    selected.forEach(function (names, chart) { names.clear(); render(chart); rescale(chart); });
  }
  function line(text, className) {
    var el = document.createElement("div");
    el.className = className;
    el.textContent = text;
    return el;
  }
  function show(point) {
    var group = seriesOf(point);
    tip.replaceChildren();
    tip.appendChild(line(point.getAttribute("data-heading"), "tip-heading"));
    var body = document.createElement("div");
    var dot = document.createElement("span");
    dot.className = "tip-dot";
    dot.style.background = group.getAttribute("data-colour");
    var value = document.createElement("b");
    value.textContent = point.getAttribute("data-value");
    body.append(dot, group.getAttribute("data-series") + " · " + point.getAttribute("data-metric") + ": ", value);
    tip.appendChild(body);
    tip.style.display = "block";
    document.querySelectorAll(".chart .mk.on").forEach(function (mark) { mark.classList.remove("on"); });
    var mark = point.previousElementSibling;
    if (mark && mark.classList.contains("mk")) { mark.classList.add("on"); }
    var box = point.getBoundingClientRect();
    var left = box.left + box.width / 2 + 14;
    if (left + tip.offsetWidth > window.innerWidth - 8) { left = box.left - tip.offsetWidth - 14; }
    tip.style.left = Math.max(8, left) + "px";
    tip.style.top = Math.max(8, box.top + box.height / 2 - tip.offsetHeight / 2) + "px";
  }
  function hide() {
    tip.style.display = "none";
    document.querySelectorAll(".chart .mk.on").forEach(function (mark) { mark.classList.remove("on"); });
  }
  function release() {
    if (!pinned) { return; }
    var chart = chartOf(pinned);
    pinned = null;
    hide();
    focus(chart, null);
  }
  document.querySelectorAll(".chart .pt").forEach(function (point) {
    var title = point.querySelector("title");
    if (title) { title.remove(); }
    point.setAttribute("tabindex", "0");
    function on() { show(point); focus(chartOf(point), seriesOf(point).getAttribute("data-series")); }
    point.addEventListener("mouseenter", function () { if (!pinned) { on(); } });
    point.addEventListener("mouseleave", function () {
      if (!pinned) { hide(); focus(chartOf(point), null); }
    });
    point.addEventListener("click", function (event) {
      event.stopPropagation();
      var again = pinned === point;
      release();
      if (!again) { pinned = point; on(); }
    });
  });
  document.querySelectorAll(".chart .ln, .chart .legend span").forEach(function (el) {
    el.addEventListener("mouseenter", function () {
      if (!pinned) { focus(chartOf(el), seriesOf(el).getAttribute("data-series")); }
    });
    el.addEventListener("mouseleave", function () { if (!pinned) { focus(chartOf(el), null); } });
  });
  document.querySelectorAll(".chart .legend span").forEach(function (el) {
    el.setAttribute("role", "button");
    el.setAttribute("tabindex", "0");
    el.setAttribute("aria-pressed", "false");
    el.addEventListener("click", function (event) { event.stopPropagation(); toggle(el); });
    el.addEventListener("keydown", function (event) {
      if (event.key === "Enter" || event.key === " ") { event.preventDefault(); toggle(el); }
    });
  });
  document.addEventListener("click", release);
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") { release(); clearSelections(); }
  });
})();
"""
"""Hover tooltips and line focus, as the UX prototype has. Clicking a legend label keeps
that series selected, hides the others, and rescales the y axis to the selection, until it
is clicked again or Escape is pressed.
Fixed code: it reads each chart point's own <title> and writes only textContent, so no data
is ever parsed as HTML."""

_LEFT_OUT_TEXT = "Left out: this combines organisations, and coverage is incomplete."


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


_LABELS = {
    "orgId": "Organisation",
    "by_org": "By organisation",
    "portfolio": "Portfolio totals",
    "by_band": "Days active by severity",
    "reintroduced_critical_high": "Remediated Critical and High Vulnerabilities (reintroduced stands in)",
    "total_known_exploit_findings": "Total CISA KEV findings (known exploits stand in)",
    "total_kev_findings": "Total CISA KEV findings",
    "kev_per_assessed_asset": "CISA KEV per assessed asset",
}


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


_WHOLE_NUMBERS = {"risk_score", "mean_risk", "median_risk", "max_risk", "median_of_org_means"}
"""Risk scores shown as whole numbers, as the prototype shows them."""


def _whole(value: Any) -> Any:
    """Round half up, as readers expect. Scores arrive rounded to one decimal, so an exact .x5 can be off by one."""
    if not _is_number(value) or not math.isfinite(float(value)):
        return value
    return int(Decimal(str(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))


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
    if len(rows) != 1 or not rows[0] or "orgId" in rows[0] or not all(_is_number(v) for v in rows[0].values()):
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
                value = _whole(row.get(c)) if c in _WHOLE_NUMBERS else row.get(c)
                cells.append(f'<td class="n">{_text(value)}</td>')
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


def _tiles_or_table(
    rows: List[Dict[str, Any]], colours: Dict[str, str], items: Sequence[Tuple[str, str]], notes: Dict[str, str]
) -> str:
    """Tiles from the one row of a combined result, or the plain table if a column is missing."""
    if not rows:
        return '<p class="hint">No rows.</p>'
    row = rows[0]
    if len(rows) != 1 or any(key not in row for _, key in items):
        return _table(rows, colours)
    return _tiles([(label, row[key], notes.get(key, "")) for label, key in items])


def _note(prefix: str, value: Any, suffix: str = "") -> str:
    return f"{prefix} {_number_text(value)}{suffix}" if _is_number(value) else ""


def _kev_source(kev: Optional[Dict[str, Any]]) -> str:
    return f"CISA KEV catalogue {kev.get('version')}, released {_when(str(kev.get('released')))}" if kev else ""


def _portfolio_tiles(rows: List[Dict[str, Any]], colours: Dict[str, str], kev: Optional[Dict[str, Any]] = None) -> str:
    if kev:
        third, notes = ("Total findings in CISA KEV", "kev_findings"), {}
    else:
        third = ("Total findings with a known exploit", "with_exploits")
        notes = {"with_exploits": "Stands in for findings in CISA KEV: no KEV catalogue is stored yet"}
    return _tiles_or_table(
        rows, colours, [("Total critical findings", "critical"), ("Total high findings", "high"), third], notes
    )


_KEV_ONLY = ("total_kev_findings", "kev_per_assessed_asset")
_STAND_IN_ONLY = ("total_known_exploit_findings",)


def _comparison_rows(rows: List[Dict[str, Any]], kev: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Organisation comparison with the KEV columns when a catalogue is stored, else the stand-in."""
    drop = _STAND_IN_ONLY if kev else _KEV_ONLY
    return [{k: v for k, v in row.items() if k not in drop} for row in rows]


def _heat_step(value: float, values: Sequence[float]) -> int:
    """Quintile of value among values, by its mid rank so ties share a step. The largest is darkest."""
    if value >= max(values):
        return len(HEAT_RAMP) - 1
    below = sum(1 for v in values if v < value)
    at_or_below = sum(1 for v in values if v <= value)
    mid_rank = (below + at_or_below) / 2
    return max(0, math.ceil(mid_rank * len(HEAT_RAMP) / len(values)) - 1)


_SEVERITY_RANGES = {"critical": "9.0-10.0", "high": "7.0-8.9", "medium": "4.0-6.9", "low": "0.1-3.9"}
"""CVSS v3 score range of each severity, as the UX prototype labels its columns."""


_AGE_BANDS = ("0-7", "8-30", "31-60", "61-90", "91+")


def _heat_table(rows: List[Dict[str, Any]], colours: Dict[str, str]) -> str:
    """Age band by severity, each non-zero cell shaded by its quintile among the non-zero cells."""
    if not rows:
        return '<p class="hint">No rows.</p>'
    severities = [c for c in rows[0] if c != "band"]
    # The query returns only bands that hold findings. The grid always shows all five, as
    # the prototype does, so an empty band reads as 0 rather than as missing.
    by_band = {r.get("band"): r for r in rows}
    if set(by_band) <= set(_AGE_BANDS):
        rows = [by_band.get(band) or {"band": band, **{c: 0 for c in severities}} for band in _AGE_BANDS]

    def count(value: Any) -> Optional[float]:
        if not _is_number(value):
            return None
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None

    values = [v for r in rows for c in severities if (v := count(r.get(c))) is not None]

    def shade(value: Any) -> str:
        number = count(value)
        if number is None:
            return f"<td>{_text(value)}</td>"
        background, text = HEAT_RAMP[_heat_step(number, values)]
        return f'<td style="background:{background};color:{text}">{_text(value)}</td>'

    oldest = next((r for r in rows if r.get("band") == "91+"), None)
    headline = ""
    if oldest is not None:
        total = sum(int(v) for c in severities if _is_number(v := oldest.get(c)))
        headline = f'<p class="headline"><span class="lab">91+ days</span> <strong>{_text(total)}</strong></p>'
    head = "".join(
        f'<th>{escape(_label(c))}<span class="range">CVSS {_SEVERITY_RANGES.get(c, "")}</span></th>'
        if c in _SEVERITY_RANGES
        else f"<th>{escape(_label(c))}</th>"
        for c in severities
    )
    body = "".join(
        f'<tr><td class="rl">{_text(r.get("band"))} days</td>' + "".join(shade(r.get(c)) for c in severities) + "</tr>"
        for r in rows
    )
    return (
        f'{headline}<table class="mx"><thead><tr><th class="rh"></th>{head}</tr></thead>'
        f"<tbody>{body}</tbody></table>"
        '<p class="inlinenote">Cell shade encodes magnitude, light amber (lowest) to dark red (highest).</p>'
    )


def _with_figures(chart: str, rows: List[Dict[str, Any]], colours: Dict[str, str]) -> str:
    """A chart with its exact figures in a collapsed table beneath it."""
    return f"{chart}<details><summary>Show the figures</summary>{_table(rows, colours)}</details>"


CHART_WEEKS = 26
"""Weeks each weekly chart plots, ending at the latest data date. Every week stays in the figures."""


def _recent_weeks(card: Dict[str, Any], count: int = CHART_WEEKS) -> List[str]:
    """The last count Monday weeks, ending at the week of the card's latest data date.

    Returns no weeks when a data date cannot be read, so the chart plots every week instead.
    """
    dates = list(card.get("as_of_by_org", {}).values()) or [card["as_of"]]
    try:
        latest = max(datetime.fromisoformat(str(d)).replace(tzinfo=None) for d in dates).date()
    except ValueError:
        return []
    monday = latest - timedelta(days=latest.weekday())
    return [(monday - timedelta(weeks=n)).isoformat() for n in range(count - 1, -1, -1)]


def _weekly(y_key: str, y_title: str) -> Callable[[List[Dict[str, Any]], Dict[str, str], List[str]], str]:
    def render(rows: List[Dict[str, Any]], colours: Dict[str, str], weeks: List[str]) -> str:
        if not rows:
            return '<p class="hint">No rows.</p>'
        chart = line_chart(
            rows,
            "week",
            y_key,
            "orgId",
            colours,
            _number_text,
            series=list(colours),
            y_title=y_title,
            x_values=weeks,
            leads=_leads(colours),
        )
        note = (
            f'<p class="inlinenote">Last {len(weeks)} weeks to the data date. Every week is in the figures.</p>'
            if weeks
            else ""
        )
        return _with_figures(f'<div class="chart">{chart}</div>{note}', rows, colours)

    return render


def _by_organisation(table: str, open_: bool) -> str:
    """A supporting per-organisation table, collapsed unless it is all the card can show."""
    return f"<details{' open' if open_ else ''}><summary>By organisation</summary>{table}</details>"


_LEAD = {("portfolio_summary", "portfolio")}
"""Results shown first on their card, as the UX prototype leads with its tiles."""

_SUPPORTING = {("portfolio_summary", "by_org")}
"""Per-organisation tables that back a card's main widget, shown collapsed under it."""

_FEATURED: Dict[Tuple[str, str], Callable[[List[Dict[str, Any]], Dict[str, str]], str]] = {
    ("portfolio_summary", "portfolio"): _portfolio_tiles,
    ("unresolved_age_severity", "by_band"): _heat_table,
}

_WEEKLY = {
    ("new_vulnerabilities", "by_week"): _weekly("new_critical_high", "New critical and high"),
    ("remediated_vulnerabilities", "by_week"): _weekly("reintroduced_critical_high", "Reintroduced critical and high"),
}


def _risk_distribution(
    rows: List[Dict[str, Any]], median: Any, colours: Dict[str, str], coverage_complete: bool
) -> List[str]:
    """Dots per organisation, with the median on the chart when it covers every organisation."""
    if not rows:
        return ['<p class="hint">No rows.</p>']
    whole = lambda v: _number_text(_whole(v))  # noqa: E731
    plot = dot_plot(
        rows, "mean_risk", "orgId", colours, whole, median=median, mark_highest=coverage_complete, leads=_leads(colours)
    )
    chart = f'<div class="chart">{plot}</div>'
    return [chart, _by_organisation(_table(rows, colours), open_=False)]


_DRAWN_ON = {("org_risk_distribution", "portfolio_median"): ("org_risk_distribution", "by_org")}
"""Combined results drawn onto another result's chart, rather than shown on their own."""


def _left_out(what: str) -> str:
    return f'<p class="inlinenote"><strong>{escape(what)}.</strong> {_LEFT_OUT_TEXT}</p>'


def _card_body(card: Dict[str, Any], coverage_complete: bool, colours: Dict[str, str]) -> List[str]:
    per_org = set(card.get("per_org", ()))
    results = card["results"]
    parts: List[str] = []
    ordered = sorted(results.items(), key=lambda item: (card["card"], item[0]) not in _LEAD)
    for name, rows in ordered:
        key = (card["card"], name)
        if not coverage_complete and name not in per_org:
            parts.append(_left_out("Median" if key in _DRAWN_ON else _label(name)))
            continue
        if key in _DRAWN_ON:
            continue
        if key == ("org_risk_distribution", "by_org"):
            median_rows = results.get("portfolio_median") or []
            usable = coverage_complete or "portfolio_median" in per_org
            median = median_rows[0].get("median_of_org_means") if median_rows and usable else None
            parts += _risk_distribution(rows, median, colours, coverage_complete)
        elif key in _SUPPORTING:
            parts.append(_by_organisation(_table(rows, colours), open_=not coverage_complete))
        else:
            if key in _WEEKLY:
                parts.append(_WEEKLY[key](rows, colours, _recent_weeks(card)))
            else:
                kev = card.get("kev_catalogue")
                if key == ("portfolio_summary", "portfolio"):
                    parts.append(_portfolio_tiles(rows, colours, kev))
                elif key == ("org_comparison", "by_org"):
                    parts.append(_table(_comparison_rows(rows, kev), colours))
                else:
                    parts.append(_FEATURED.get(key, _table)(rows, colours))
    return parts


def _data_dates(cards: List[Dict[str, Any]]) -> str:
    """Every organisation's data date, once for the whole report."""
    by_org: Dict[str, str] = {}
    for card in cards:
        for org, when in card.get("as_of_by_org", {}).items():
            by_org.setdefault(str(org), when)
    if not by_org:
        return ", ".join(sorted({_when(card["as_of"]) for card in cards}))
    dates = sorted({_when(when) for when in by_org.values()})
    if len(dates) == 1:
        return dates[0]
    return f"from {dates[0]} to {dates[-1]}, by organisation"


def _card_section(
    card: Dict[str, Any], coverage_complete: bool, colours: Dict[str, str], scope: Optional[str] = None
) -> str:
    parts = [
        f'<section class="card" id="{escape(card["card"])}">',
        f'<p class="sec">{escape(card["title"])}</p>',
        f'<p class="hint">{escape(card.get("description", ""))}</p>',
    ]
    if scope and set(card["results"]) - set(card.get("per_org", ())):
        parts.append(f'<p class="scope">{escape(scope)}</p>')
    parts += _card_body(card, coverage_complete, colours)
    parts.append(
        '<details class="about"><summary>About this card</summary>'
        f'<p class="inlinenote">{escape(card["definition"])}</p></details>'
    )
    parts.append("</section>")
    return "\n".join(parts)


def _lead_colours(cards: List[Dict[str, Any]], ids: List[str], names: Dict[str, str]) -> Dict[str, str]:
    """The three highest-risk organisations take the lead colours for the whole report.

    As in the UX prototype, the same three are coloured in every chart, legend, and table,
    and every other organisation is muted. With no risk score, the first three by ID lead.
    """
    risk: Dict[str, float] = {}
    for card in cards:
        for rows in card["results"].values():
            for row in rows:
                score = row.get("mean_risk")
                if row.get("orgId") is not None and _is_number(score) and math.isfinite(float(score)):
                    risk.setdefault(str(row["orgId"]), float(score))
    ranked = sorted(ids, key=lambda org: (-risk.get(org, float("-inf")), org))
    return {names[org]: LEAD_COLOURS[i] if i < len(LEAD_COLOURS) else MUTED_COLOUR for i, org in enumerate(ranked)}


def _leads(colours: Dict[str, str]) -> List[str]:
    return [name for name, colour in colours.items() if colour in LEAD_COLOURS]


def _org_ids(cards: List[Dict[str, Any]]) -> List[str]:
    """Every org ID the cards mention, in a stable order."""
    ids = {str(org) for card in cards for org in card["orgs"]}
    ids |= {
        str(row["orgId"])
        for card in cards
        for rows in card["results"].values()
        for row in rows
        if row.get("orgId") is not None
    }
    return sorted(ids)


def _display_names(ids: List[str], labels: Dict[str, str]) -> Dict[str, str]:
    """A unique display name for each org ID: its label, with its ID added while names clash."""
    names = {org: labels.get(org, org) for org in ids}
    for _ in range(3):
        counts = Counter(names.values())
        if all(count == 1 for count in counts.values()):
            return names
        names = {org: (f"{name} ({org})" if counts[name] > 1 else name) for org, name in names.items()}
    counts = Counter(names.values())
    return {org: (org if counts[name] > 1 else name) for org, name in names.items()}


def _with_labels(card: Dict[str, Any], names: Dict[str, str]) -> Dict[str, Any]:
    """The card with each org ID shown by its display name."""

    def name(org: Any) -> Any:
        return names.get(str(org), org) if org is not None else org

    shown = dict(card)
    shown["orgs"] = [name(org) for org in card["orgs"]]
    shown["as_of_by_org"] = {str(name(org)): when for org, when in card.get("as_of_by_org", {}).items()}
    shown["results"] = {
        query: [{k: (name(v) if k == "orgId" else v) for k, v in row.items()} for row in rows]
        for query, rows in card["results"].items()
    }
    return shown


def _org_count(orgs: Sequence[str], excluded: Sequence[str], configured_count: Optional[int]) -> str:
    """How many organisations the report covers, and how many of those have findings."""
    if not configured_count:
        return f"{len(orgs)} organisation(s)"
    loaded = configured_count - len(excluded)
    if loaded == len(orgs):
        return f"{loaded} organisation(s)"
    return f"{loaded} organisation(s) loaded, {len(orgs)} with findings"


def render_report(
    cards: List[Dict[str, Any]],
    generated_at: str,
    coverage_complete: bool,
    coverage_message: Optional[str] = None,
    excluded: Sequence[str] = (),
    configured_count: Optional[int] = None,
) -> str:
    """Render the dashboard cards as one HTML document that opens offline.

    Args:
        cards: run_card() results, in display order.
        generated_at: When the report was built, as a UTC ISO timestamp.
        coverage_complete: False when an organisation is missing or stale, which leaves
            out every result that combines organisations.
        coverage_message: Shown at the top when coverage is incomplete.
        excluded: With coverage_complete, one line per organisation left out because its
            region was unavailable. Combined figures are then shown, and each card that
            combines organisations says how many of configured_count it covers.
        configured_count: How many organisations are configured. Required with excluded.
    """
    labels = {org: label for card in cards for org, label in (card.get("org_labels") or {}).items()}
    ids = _org_ids(cards)
    names = _display_names(ids, labels)
    # Keyed by org ID, so a renamed label never moves another org's colour.
    colours = _lead_colours(cards, ids, names)
    cards = [_with_labels(card, names) for card in cards]
    orgs = sorted({str(org) for card in cards for org in card["orgs"]})
    kev = next((card["kev_catalogue"] for card in cards if card.get("kev_catalogue")), None)
    if kev:
        kev_line = (
            f'<div class="strip">{escape(_kev_source(kev))}, {_text(kev.get("entries"))} entries, '
            f"SHA-256 <code>{escape(str(kev.get('sha256')))}</code>.</div>"
        )
    elif any(card.get("kev_unavailable") == "no_cves_column" for card in cards):
        kev_line = (
            '<div class="strip">The loaded export has no list of CVE IDs per finding, so findings cannot be '
            "matched to CISA KEV, and known exploits stand in for KEV.</div>"
        )
    else:
        kev_line = '<div class="strip">No CISA KEV catalogue is stored yet, so known exploits stand in for KEV.</div>'
    scope = None
    if coverage_complete and excluded:
        if configured_count is None:
            raise ValueError("configured_count is required when organisations are excluded")
        scope = f"{configured_count - len(excluded)} of {configured_count} organisations"
        coverage = (
            f'<div class="strip warning"><strong>Coverage: {escape(scope)}.</strong> '
            f"{escape(' '.join(excluded))} Figures below exclude {'it' if len(excluded) == 1 else 'them'}.</div>"
        )
    elif coverage_complete:
        coverage = '<div class="strip">Organisation coverage check: complete.</div>'
    else:
        coverage = (
            '<div class="strip warning"><strong>Coverage is incomplete.</strong> '
            f"{escape(coverage_message or 'An organisation is missing or was not loaded today.')} "
            "Figures that combine organisations are left out.</div>"
        )
    counted = _org_count(orgs, excluded, configured_count)
    header = [
        "<!doctype html>",
        '<html lang="en" data-rds-brand="callisto" data-rds-mode="dark"><head><meta charset="utf-8">',
        "<title>Multi-Org Exposure Dashboard</title>",
        f"<style>/* Inter, copyright The Inter Project Authors, SIL Open Font License 1.1 */\n"
        f'{_font_faces()}\n{_STYLE}</style></head><body><div class="wrap">',
        f'<div class="topbar">{_logo()}</div>',
        "<h1>Multi-Org Exposure Dashboard</h1>",
        f'<p class="sub">Generated {escape(_when(generated_at))} &middot; {escape(counted)}</p>',
        f'<p class="sub">Organisations: {escape(", ".join("(no organisation)" if o == "None" else o for o in orgs))}. '
        f"Data date: {escape(_data_dates(cards))}.</p>",
        coverage,
        kev_line,
    ]
    body = [_card_section(card, coverage_complete, colours, scope) for card in cards]
    return "\n".join(header + body + [f"</div><script>{_SCRIPT}</script></body></html>"])
