"""Inline SVG charts for the dashboard report, drawn from a card's result rows.

The charts are plain SVG with no script, so the report still opens offline. They only
ever see a card's small result set, never the raw findings. Every label is HTML-escaped.
"""

import math
from html import escape
from typing import Any, Callable, Dict, List, Sequence

Formatter = Callable[[Any], str]

_FONT = "-apple-system, Helvetica, Arial, sans-serif"
_AXIS = "rgba(248,250,250,0.3)"
_GRID = "rgba(248,250,250,0.08)"
_LABEL = "rgba(248,250,250,0.5)"
_FALLBACK = "#78a3c6"
_SVG_OPEN = '<svg viewBox="0 0 {w} {h}" width="100%" role="img" xmlns="http://www.w3.org/2000/svg" font-family="{f}">'


def _value(raw: Any) -> float:
    try:
        number = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def nice_max(value: float) -> float:
    """The smallest of 1, 2, 2.5, 5 or 10 times a power of ten that is at least value."""
    if value <= 0:
        return 1.0
    power = 10 ** math.floor(math.log10(value))
    for step in (1, 2, 2.5, 5, 10):
        if value <= step * power:
            return step * power
    return 10 * power


def _legend(names: Sequence[str], colours: Dict[str, str]) -> str:
    items = "".join(
        f'<span><i style="background:{colours.get(name, _FALLBACK)}"></i>{escape(name)}</span>' for name in names
    )
    return f'<div class="legend">{items}</div>'


def _y_axis(top: float, fmt: Formatter, title: str, left: int, right: int, y0: int, y1: int) -> List[str]:
    parts = []
    for i in range(5):
        y = y1 - (y1 - y0) * i / 4
        if i:
            parts.append(
                f'<line x1="{left}" y1="{y:.2f}" x2="{right}" y2="{y:.2f}" stroke="{_GRID}" stroke-width="1"/>'
            )
        parts.append(
            f'<text x="{left - 6}" y="{y + 3:.2f}" font-size="10" fill="{_LABEL}" text-anchor="end">'
            f"{escape(fmt(top * i / 4))}</text>"
        )
    parts.append(f'<line x1="{left}" y1="{y0}" x2="{left}" y2="{y1}" stroke="{_AXIS}" stroke-width="1"/>')
    parts.append(f'<line x1="{left}" y1="{y1}" x2="{right}" y2="{y1}" stroke="{_AXIS}" stroke-width="1"/>')
    mid = (y0 + y1) / 2
    parts.append(
        f'<text x="12" y="{mid:.2f}" font-size="10" fill="{_LABEL}" text-anchor="middle" '
        f'transform="rotate(-90 12 {mid:.2f})">{escape(title)}</text>'
    )
    return parts


def line_chart(
    rows: List[Dict[str, Any]], x_key: str, y_key: str, series_key: str, colours: Dict[str, str], fmt: Formatter
) -> str:
    """One line per series over the sorted x values. A missing point counts as zero."""
    xs = sorted({str(r.get(x_key)) for r in rows})
    names = sorted({str(r.get(series_key)) for r in rows})
    if not xs:
        return ""
    points: Dict[str, Dict[str, float]] = {name: {} for name in names}
    for r in rows:
        series = points[str(r.get(series_key))]
        series[str(r.get(x_key))] = series.get(str(r.get(x_key)), 0.0) + _value(r.get(y_key))
    top = nice_max(max(v for series in points.values() for v in series.values()))
    left, right, y0, y1 = 64, 680, 20, 180
    step = (right - left) / max(1, len(xs) - 1)

    def x_at(i: int) -> float:
        return left + step * i if len(xs) > 1 else (left + right) / 2

    parts = [_SVG_OPEN.format(w=700, h=220, f=_FONT)]
    parts += _y_axis(top, fmt, _label_text(y_key), left, right, y0, y1)
    every = max(1, math.ceil(len(xs) / 12))
    for i, x in enumerate(xs):
        if i % every == 0:
            parts.append(
                f'<text x="{x_at(i):.2f}" y="{y1 + 16}" font-size="10" fill="{_LABEL}" '
                f'text-anchor="middle">{escape(x)}</text>'
            )
    for name in names:
        coords = [(x_at(i), y1 - (y1 - y0) * points[name].get(x, 0.0) / top) for i, x in enumerate(xs)]
        colour = colours.get(name, _FALLBACK)
        joined = " ".join(f"{cx:.2f},{cy:.2f}" for cx, cy in coords)
        parts.append(f'<polyline points="{joined}" fill="none" stroke="{colour}" stroke-width="2"/>')
        for (cx, cy), x in zip(coords, xs):
            tip = f"{name} {x}: {fmt(points[name].get(x, 0.0))}"
            parts.append(
                f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="2.5" fill="{colour}"><title>{escape(tip)}</title></circle>'
            )
    parts.append("</svg>")
    return "".join(parts) + _legend(names, colours)


def dot_plot(
    rows: List[Dict[str, Any]], value_key: str, label_key: str, colours: Dict[str, str], fmt: Formatter
) -> str:
    """One dot per row on a single axis from the lowest to the highest value."""
    values = [(str(r.get(label_key)), _value(r.get(value_key))) for r in rows]
    if not values:
        return ""
    low, high = min(v for _, v in values), max(v for _, v in values)
    span = high - low or 1.0
    left, right, axis_y, dot_y = 30, 270, 76, 62

    def x_at(v: float) -> float:
        return (left + right) / 2 if high == low else left + (right - left) * (v - low) / span

    parts = [_SVG_OPEN.format(w=300, h=96, f=_FONT).replace('width="100%"', 'width="100%" style="max-width:660px"')]
    parts.append(f'<line x1="{left}" y1="{axis_y}" x2="{right}" y2="{axis_y}" stroke="{_AXIS}" stroke-width="0.7"/>')
    for label, v in values:
        parts.append(
            f'<circle cx="{x_at(v):.2f}" cy="{dot_y}" r="4" fill="{colours.get(label, _FALLBACK)}" opacity="0.85">'
            f"<title>{escape(label)}: {escape(fmt(v))}</title></circle>"
        )
    ticks = [low] if high == low else [low, (low + high) / 2, high]
    for v in ticks:
        parts.append(
            f'<text x="{x_at(v):.2f}" y="88" font-size="6" fill="{_LABEL}" text-anchor="middle">{escape(fmt(v))}</text>'
        )
    parts.append("</svg>")
    return "".join(parts) + _legend([label for label, _ in values], colours)


def bar_chart(rows: List[Dict[str, Any]], label_key: str, value_key: str, fmt: Formatter) -> str:
    """One vertical bar per row, in row order."""
    bars = [(str(r.get(label_key)), _value(r.get(value_key))) for r in rows]
    if not bars:
        return ""
    top = nice_max(max(v for _, v in bars))
    left, right, y0, y1 = 64, 680, 20, 180
    slot = (right - left) / len(bars)
    width = min(60.0, slot * 0.6)
    parts = [_SVG_OPEN.format(w=700, h=220, f=_FONT)]
    parts += _y_axis(top, fmt, _label_text(value_key), left, right, y0, y1)
    for i, (label, v) in enumerate(bars):
        height = (y1 - y0) * v / top
        x = left + slot * i + (slot - width) / 2
        parts.append(
            f'<rect x="{x:.2f}" y="{y1 - height:.2f}" width="{width:.2f}" height="{height:.2f}" rx="2" '
            f'fill="{_FALLBACK}"><title>{escape(label)}: {escape(fmt(v))}</title></rect>'
        )
        parts.append(
            f'<text x="{x + width / 2:.2f}" y="{y1 + 16}" font-size="10" fill="{_LABEL}" '
            f'text-anchor="middle">{escape(label)}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def _label_text(key: str) -> str:
    text = key.replace("_", " ")
    return text[:1].upper() + text[1:]
