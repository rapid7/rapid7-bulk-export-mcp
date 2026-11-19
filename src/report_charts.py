"""Inline SVG charts for the dashboard report, drawn from a card's result rows.

The charts are plain SVG with no script, so the report still opens offline. They only
ever see a card's small result set, never the raw findings. Every label is HTML-escaped.
"""

import math
from datetime import date, timedelta
from html import escape
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

Formatter = Callable[[Any], str]

_FONT = "-apple-system, Helvetica, Arial, sans-serif"
_AXIS = "rgba(248,250,250,0.3)"
_GRID = "rgba(248,250,250,0.08)"
_LABEL = "rgba(248,250,250,0.5)"
_FALLBACK = "#9aa5ab"
_MEDIAN = "#82939c"
_MAX_PERIODS = 1000
_MAX_X_LABELS = 8
_LEADS = 3
_MARKED_PERIODS = 26
_MUTED_LINE = "#5b676c"
"""The prototype draws every series but the three largest as thin grey lines."""
_HIGHEST = "#ff903d"
_MAX_VALUE = 1e300
_SVG_OPEN = '<svg viewBox="0 0 {w} {h}" width="100%" role="img" xmlns="http://www.w3.org/2000/svg" font-family="{f}">'


def _value(raw: Any) -> float:
    """A count to draw: missing, non-finite or negative values draw as zero."""
    number = _maybe_value(raw)
    return 0.0 if number is None else max(0.0, number)


def _maybe_value(raw: Any) -> Optional[float]:
    try:
        number = float(raw)
    except (TypeError, ValueError):
        return None
    return min(number, _MAX_VALUE) if math.isfinite(number) else None


def _periods(xs: List[str]) -> List[str]:
    """Every period from the first to the last, so a gap keeps its width on the axis.

    Fills "YYYY-MM" quarters (MM the quarter's first month) and "YYYY-MM-DD" weeks that fall
    on the same weekday. A range longer than _MAX_PERIODS, such as one stray epoch date, is left
    as given, so no period with data is ever cut off. Anything else is left as given too.
    """
    return _fill_quarters(xs) or _fill_weeks(xs) or xs


def _fill_quarters(xs: List[str]) -> Optional[List[str]]:
    try:
        months = [(int(x[:4]), int(x[5:7])) for x in xs if len(x) == 7 and x[4] == "-"]
    except ValueError:
        return None
    if not xs or len(months) != len(xs) or any(m not in (1, 4, 7, 10) for _, m in months):
        return None
    (year, month), last = min(months), max(months)
    if (last[0] - year) * 4 + (last[1] - month) // 3 >= _MAX_PERIODS:
        return None
    filled = []
    while (year, month) <= last:
        filled.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 10 else (year, month + 3)
    return filled


def _fill_weeks(xs: List[str]) -> Optional[List[str]]:
    try:
        days = [date.fromisoformat(x) for x in xs]
    except ValueError:
        return None
    if not days or len({d.weekday() for d in days}) != 1:
        return None
    current, last, filled = min(days), max(days), []
    if (last - current).days // 7 >= _MAX_PERIODS:
        return None
    while current <= last:
        filled.append(current.isoformat())
        current += timedelta(days=7)
    return filled


def _tick(value: float) -> float:
    """An axis value rounded to two decimals, so float error such as 0.35000000003 never shows."""
    return round(value, 2)


def _largest(points: Dict[str, Dict[str, float]], count: int) -> Set[str]:
    """The series with the largest totals, which the UX prototype draws heavier and marks."""
    ranked = sorted(points, key=lambda name: (-sum(points[name].values()), name))
    return set(ranked[:count])


def nice_max(value: float) -> float:
    """The smallest of 1, 2, 2.5, 5 or 10 times a power of ten that is at least value."""
    if value <= 0:
        return 1.0
    power = 10.0 ** math.floor(math.log10(min(value, _MAX_VALUE)))
    for step in (1, 2, 2.5, 5, 10):
        if value <= step * power:
            return step * power
    return 10 * power


def _legend(names: Sequence[str], colours: Dict[str, str], muted: Sequence[str] = ()) -> str:
    items = "".join(
        f'<span data-series="{escape(name)}">'
        f'<i style="background:{_MUTED_LINE if name in muted else colours.get(name, _FALLBACK)}"></i>'
        f"{escape(name)}</span>"
        for name in names
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
            f'<text x="{left - 6}" y="{y + 3:.2f}" font-size="11" fill="{_LABEL}" text-anchor="end">'
            f"{escape(fmt(_tick(top * i / 4)))}</text>"
        )
    parts.append(f'<line x1="{left}" y1="{y0}" x2="{left}" y2="{y1}" stroke="{_AXIS}" stroke-width="1"/>')
    parts.append(f'<line x1="{left}" y1="{y1}" x2="{right}" y2="{y1}" stroke="{_AXIS}" stroke-width="1"/>')
    if title:
        mid = (y0 + y1) / 2
        parts.append(
            f'<text x="12" y="{mid:.2f}" font-size="11" fill="{_LABEL}" text-anchor="middle" '
            f'transform="rotate(-90 12 {mid:.2f})">{escape(title)}</text>'
        )
    return parts


def line_chart(
    rows: List[Dict[str, Any]],
    x_key: str,
    y_key: str,
    series_key: str,
    colours: Dict[str, str],
    fmt: Formatter,
    series: Sequence[str] = (),
    y_title: Optional[str] = None,
    x_values: Sequence[str] = (),
) -> str:
    """One line per series over the sorted x values. A missing point counts as zero.

    Args:
        series: Names to draw even with no rows, so a zero series shows as a flat line
            rather than vanishing from the chart and the legend.
        y_title: The y axis title. Defaults to y_key in sentence case.
        x_values: The x axis to draw, in order. Rows outside it are left off the chart.
            Defaults to every period from the first row to the last.
    """
    rows = [r for r in rows if r.get(x_key) is not None]
    if x_values:
        xs = [str(x) for x in x_values]
        rows = [r for r in rows if str(r.get(x_key)) in set(xs)]
    else:
        xs = _periods(sorted({str(r.get(x_key)) for r in rows}))
    names = sorted({str(r.get(series_key)) for r in rows} | {str(name) for name in series})
    if not xs:
        return ""
    points: Dict[str, Dict[str, float]] = {name: {} for name in names}
    for r in rows:
        series = points[str(r.get(series_key))]
        series[str(r.get(x_key))] = series.get(str(r.get(x_key)), 0.0) + _value(r.get(y_key))
    top = nice_max(max((v for series in points.values() for v in series.values()), default=0.0))
    left, right, y0, y1 = 90, 980, 20, 230
    step = (right - left) / max(1, len(xs) - 1)

    def x_at(i: int) -> float:
        return left + step * i if len(xs) > 1 else (left + right) / 2

    parts = [_SVG_OPEN.format(w=1000, h=270, f=_FONT)]
    parts += _y_axis(top, fmt, "", left, right, y0, y1)
    every = max(1, math.ceil(len(xs) / _MAX_X_LABELS))
    for i, x in enumerate(xs):
        if i % every == 0:
            parts.append(
                f'<text x="{x_at(i):.2f}" y="{y1 + 18}" font-size="11" fill="{_LABEL}" '
                f'text-anchor="middle">{escape(x)}</text>'
            )
    metric = y_title or _label_text(y_key)
    leads = _largest(points, _LEADS)
    marked = len(xs) <= _MARKED_PERIODS
    for name in sorted(names, key=lambda n: (sum(points[n].values()), n)):
        coords = [(x_at(i), y1 - (y1 - y0) * points[name].get(x, 0.0) / top) for i, x in enumerate(xs)]
        colour = colours.get(name, _FALLBACK)
        lead = name in leads
        joined = " ".join(f"{cx:.2f},{cy:.2f}" for cx, cy in coords)
        parts.append(
            f'<g class="series{" lead" if lead else ""}{" marked" if lead and marked else ""}" '
            f'data-series="{escape(name)}" '
            f'data-colour="{colour if lead else _MUTED_LINE}">'
        )
        parts.append(
            f'<polyline points="{joined}" fill="none" stroke="{colour if lead else _MUTED_LINE}" '
            f'stroke-width="{2.4 if lead else 1.6}" '
            'class="ln"/>'
        )
        for (cx, cy), x in zip(coords, xs):
            value = fmt(points[name].get(x, 0.0))
            heading = f"Week of {x}" if x_key == "week" else x
            parts.append(f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="4" fill="{colour}" class="mk"/>')
            parts.append(
                f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="10" fill="transparent" class="pt" '
                f'data-heading="{escape(heading)}" data-metric="{escape(metric)}" data-value="{escape(value)}">'
                f"<title>{escape(f'{name} {x}: {value}')}</title></circle>"
            )
        parts.append("</g>")
    parts.append("</svg>")
    return "".join(parts) + _legend(names, colours, muted=[n for n in names if n not in leads])


def dot_plot(
    rows: List[Dict[str, Any]],
    value_key: str,
    label_key: str,
    colours: Dict[str, str],
    fmt: Formatter,
    median: Any = None,
    mark_highest: bool = False,
) -> str:
    """One dot per row on a single axis from the lowest to the highest value.

    Args:
        median: Drawn as a dashed marker when given. It combines organisations, so pass it
            only when every organisation is present.
        mark_highest: Label the highest dot, as the prototype does. It compares organisations,
            so pass it only when every organisation is present.
    """
    measured = [(str(r.get(label_key)), _maybe_value(r.get(value_key))) for r in rows]
    values = [(label, v) for label, v in measured if v is not None]
    if not values:
        return ""
    low, high = min(v for _, v in values), max(v for _, v in values)
    span = high - low or 1.0
    left, right, axis_y, dot_y = 40, 960, 110, 90

    def x_at(v: float) -> float:
        return (left + right) / 2 if high == low else left + (right - left) * (v - low) / span

    parts = [_SVG_OPEN.format(w=1000, h=140, f=_FONT)]
    parts.append(f'<line x1="{left}" y1="{axis_y}" x2="{right}" y2="{axis_y}" stroke="{_AXIS}" stroke-width="0.7"/>')
    leads = _largest({label: {"v": v} for label, v in values}, _LEADS)
    for label, v in values:
        colour = colours.get(label, _FALLBACK) if label in leads else _MUTED_LINE
        parts.append(
            f'<g class="series{" lead" if label in leads else ""}" data-series="{escape(label)}" '
            f'data-colour="{colour}">'
            f'<circle cx="{x_at(v):.2f}" cy="{dot_y}" r="6" fill="{colour}" opacity="0.85" '
            f'class="pt" data-heading="Organisation risk distribution" data-metric="Risk score" '
            f'data-value="{escape(fmt(v))}">'
            f"<title>{escape(label)}: {escape(fmt(v))}</title></circle></g>"
        )
    median_value = _maybe_value(median)
    if median_value is not None:
        mx = x_at(min(max(median_value, low), high))
        parts.append(
            f'<line x1="{mx:.2f}" y1="50" x2="{mx:.2f}" y2="{axis_y}" stroke="{_MEDIAN}" stroke-width="1" '
            'stroke-dasharray="3 3"/>'
        )
        parts.append(
            f'<text x="{mx:.2f}" y="42" font-size="11" fill="{_MEDIAN}" text-anchor="middle">'
            f"median {escape(fmt(median_value))}</text>"
        )
    if mark_highest and len(values) > 1:
        top_label, top_value = max(values, key=lambda item: item[1])
        parts.append(
            f'<text x="{x_at(top_value):.2f}" y="72" font-size="11" fill="{_HIGHEST}" text-anchor="end">'
            f"highest {escape(fmt(top_value))}</text>"
        )
    ticks = [low] if high == low else [low, _tick((low + high) / 2), high]
    for v in ticks:
        parts.append(
            f'<text x="{x_at(v):.2f}" y="{axis_y + 20}" font-size="11" fill="{_LABEL}" text-anchor="middle">'
            f"{escape(fmt(v))}</text>"
        )
    parts.append("</svg>")
    names = [label for label, _ in values]
    return "".join(parts) + _legend(names, colours, muted=[name for name in names if name not in leads])


def _label_text(key: str) -> str:
    text = key.replace("_", " ")
    return text[:1].upper() + text[1:]
