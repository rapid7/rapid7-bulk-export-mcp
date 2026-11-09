"""Compare the dashboard card queries with figures read from the Exposure Management dashboard.

Opt-in and skipped by default. Set:
  DASHBOARD_REFERENCE_FILE  JSON of expected figures (see tests/fixtures/dashboard_reference.example.json)
  DASHBOARD_REFERENCE_DB    path to a database loaded with the same organisations' exports

Each check names a card, one of its queries, the row to pick (``match``, for example one
orgId), the expected field values, and an optional ``tolerance_pct``. A mismatch is a
finding, not necessarily a defect: the dashboard applies rules the export does not carry,
so the report says which figure differs and by how much.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import pytest

from src.dashboard_queries import CARDS, run_card
from src.duckdb_loader import VulnerabilityDatabase

REFERENCE_FILE = os.environ.get("DASHBOARD_REFERENCE_FILE")
REFERENCE_DB = os.environ.get("DASHBOARD_REFERENCE_DB")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not (REFERENCE_FILE and REFERENCE_DB),
        reason="DASHBOARD_REFERENCE_FILE and DASHBOARD_REFERENCE_DB not set; dashboard comparison skipped",
    ),
]


def _reference() -> dict:
    return json.loads(Path(REFERENCE_FILE).read_text()) if REFERENCE_FILE else {"checks": []}


def _check_id(check: dict) -> str:
    match = ",".join(f"{k}={v}" for k, v in sorted(check.get("match", {}).items()))
    return f"{check['card']}.{check['query']}[{match}]"


@pytest.fixture(scope="module")
def card_results():
    reference = _reference()
    as_of = datetime.fromisoformat(reference["as_of"]) if reference.get("as_of") else None
    db = VulnerabilityDatabase(REFERENCE_DB)
    try:
        cards = {check["card"] for check in reference["checks"]}
        yield {card: run_card(db, card, as_of=as_of) for card in cards}
    finally:
        db.close()


def find_row(rows: list, match: dict) -> dict:
    """Return the one row whose fields equal ``match``; an empty match needs a single-row result."""
    found = [row for row in rows if all(row.get(k) == v for k, v in match.items())]
    if len(found) != 1:
        raise AssertionError(f"expected exactly one row matching {match}, found {len(found)}")
    return found[0]


def fields_outside_tolerance(row: dict, expected: dict, tolerance_pct: float) -> list:
    problems = []
    for field, want in expected.items():
        got = row.get(field)
        if got is None:
            problems.append(f"{field}: missing, expected {want}")
            continue
        allowed = abs(want) * tolerance_pct / 100
        if abs(got - want) > allowed:
            delta = got - want
            pct = f" ({delta / want:+.2%})" if want else ""
            problems.append(f"{field}: got {got}, expected {want}, off by {delta:+}{pct}")
    return problems


@pytest.mark.parametrize("check", _reference()["checks"], ids=_check_id)
def test_card_matches_the_dashboard(card_results, check):
    assert check["card"] in CARDS, f"unknown card {check['card']}"
    rows = card_results[check["card"]]["results"][check["query"]]

    row = find_row(rows, check.get("match", {}))
    problems = fields_outside_tolerance(row, check["expected"], check.get("tolerance_pct", 0))

    assert not problems, f"{_check_id(check)} differs from the dashboard: " + "; ".join(problems)
