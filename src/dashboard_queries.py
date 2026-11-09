"""Prebaked queries behind the six multi-org dashboard cards.

Every card's numbers come from fixed, tested SQL here, never from SQL the AI writes,
so the same data always gives the same answer. The definitions follow the reference
dashboard the proof of concept produced, with two deliberate changes:

* "Active" means a finding with no matching vulnerability exception, and that rule
  applies to every card, so the cards reconcile with each other.
* Ages are counted from the time the vulnerability data was loaded, not from today,
  so a report regenerated later from the same data gives the same numbers.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .duckdb_loader import VulnerabilityDatabase


@dataclass(frozen=True)
class Card:
    """One dashboard card: what it shows and the named queries that produce it."""

    title: str
    definition: str
    queries: Tuple[Tuple[str, str], ...]


PORTFOLIO_SUMMARY = Card(
    title="Portfolio summary",
    definition=(
        "Active findings by CVSS v3 severity, and assets with at least one active finding. "
        "Active means no matching vulnerability exception on orgId, assetId, and vulnId. "
        "Unknown and None severities count in the total but in no severity column."
    ),
    queries=(
        (
            "by_org",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT "orgId",
              COUNT(*) AS active_total,
              COUNT(DISTINCT "assetId") AS assets_with_findings,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Critical') AS critical,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'High') AS high,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Medium') AS medium,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Low') AS low,
              COUNT(*) FILTER (WHERE "cvssV3Severity" IN ('Unknown', 'None')) AS unscored,
              COUNT(*) FILTER (WHERE "hasExploits") AS with_exploits
            FROM active GROUP BY "orgId" ORDER BY "orgId"
            """,
        ),
        (
            "portfolio",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT COUNT(*) AS active_total,
              COUNT(DISTINCT ("orgId", "assetId")) AS assets_with_findings,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Critical') AS critical,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'High') AS high,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Medium') AS medium,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Low') AS low
            FROM active
            """,
        ),
    ),
)

ORG_RISK_DISTRIBUTION = Card(
    title="Organisation risk distribution",
    definition=(
        "Mean, median, and maximum assets.riskScore per organisation. The export has no "
        "organisation-level risk score, and vulnerabilities.riskScore is content-level and "
        "identical across organisations, so the asset score stands in. The portfolio median "
        "is the median of the organisation means, unweighted, so each organisation counts once."
    ),
    queries=(
        (
            "by_org",
            """
            SELECT "orgId", COUNT(*) AS assets,
              ROUND(AVG("riskScore"), 1) AS mean_risk,
              ROUND(MEDIAN("riskScore"), 1) AS median_risk,
              ROUND(MAX("riskScore"), 1) AS max_risk
            FROM assets GROUP BY "orgId" ORDER BY mean_risk DESC, "orgId"
            """,
        ),
        (
            "portfolio_median",
            """
            SELECT ROUND(MEDIAN(mean_risk), 1) AS median_of_org_means
            FROM (SELECT AVG("riskScore") AS mean_risk FROM assets GROUP BY "orgId")
            """,
        ),
    ),
)

VULNERABILITY_ACTIVITY = Card(
    title="Vulnerability activity by organisation",
    definition=(
        "Active critical and high findings per calendar quarter, by the quarter each became "
        "active: the later of firstFoundTimestamp and reintroducedTimestamp. Remediated counts "
        "are not available per organisation, so reintroduced findings stand in for them. "
        "One daily snapshot has no history, so quarters replace weekly buckets."
    ),
    queries=(
        (
            "new_by_quarter",
            """
            WITH active AS (
              SELECT v."orgId", v."cvssV3Severity",
                GREATEST(v."firstFoundTimestamp",
                         COALESCE(v."reintroducedTimestamp", v."firstFoundTimestamp")) AS became_active
              FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT STRFTIME(DATE_TRUNC('quarter', became_active), '%Y-%m') AS quarter,
              "orgId", COUNT(*) AS new_critical_high
            FROM active WHERE "cvssV3Severity" IN ('Critical', 'High')
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
        ),
        (
            "reintroduced_by_quarter",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT STRFTIME(DATE_TRUNC('quarter', "reintroducedTimestamp"), '%Y-%m') AS quarter,
              "orgId", COUNT(*) AS reintroduced_critical_high
            FROM active
            WHERE "reintroducedTimestamp" IS NOT NULL AND "cvssV3Severity" IN ('Critical', 'High')
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
        ),
    ),
)

UNRESOLVED_AGE_SEVERITY = Card(
    title="Unresolved findings by days active and severity",
    definition=(
        "Active findings by severity in the PRD age bands 0 to 7, 8 to 30, 31 to 60, 61 to 90, "
        "and 91+ days. Days run from when each finding became active to when the data was loaded. "
        "Unknown and None severities are left out."
    ),
    queries=(
        (
            "by_band",
            """
            WITH active AS (
              SELECT v."cvssV3Severity",
                DATEDIFF('day',
                  GREATEST(v."firstFoundTimestamp",
                           COALESCE(v."reintroducedTimestamp", v."firstFoundTimestamp")),
                  CAST($as_of AS TIMESTAMP)) AS days_active
              FROM vulnerabilities v
              WHERE v."cvssV3Severity" IN ('Critical', 'High', 'Medium', 'Low')
                AND NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                  WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            ), banded AS (
              SELECT "cvssV3Severity",
                CASE WHEN days_active <= 7 THEN '0-7'
                     WHEN days_active <= 30 THEN '8-30'
                     WHEN days_active <= 60 THEN '31-60'
                     WHEN days_active <= 90 THEN '61-90'
                     ELSE '91+' END AS band,
                CASE WHEN days_active <= 7 THEN 1
                     WHEN days_active <= 30 THEN 2
                     WHEN days_active <= 60 THEN 3
                     WHEN days_active <= 90 THEN 4
                     ELSE 5 END AS band_order
              FROM active
            )
            SELECT band,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Critical') AS critical,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'High') AS high,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Medium') AS medium,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Low') AS low
            FROM banded GROUP BY band, band_order ORDER BY band_order
            """,
        ),
    ),
)

ORG_COMPARISON = Card(
    title="Organisation comparison",
    definition=(
        "Per organisation: mean asset risk score, asset count, and active, exploitable, and "
        "reintroduced findings. Exploitable means hasExploits; the export has no CISA KEV flag. "
        "Reintroduced stands in for remediated, which is not available per organisation."
    ),
    queries=(
        (
            "by_org",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            ), findings AS (
              SELECT "orgId", COUNT(*) AS active_findings,
                COUNT(*) FILTER (WHERE "hasExploits") AS exploitable,
                COUNT(*) FILTER (WHERE "reintroducedTimestamp" IS NOT NULL) AS reintroduced
              FROM active GROUP BY "orgId"
            ), risk AS (
              SELECT "orgId", COUNT(*) AS assets, ROUND(AVG("riskScore"), 1) AS mean_risk
              FROM assets GROUP BY "orgId"
            )
            SELECT COALESCE(r."orgId", f."orgId") AS "orgId",
              r.mean_risk, COALESCE(r.assets, 0) AS assets,
              COALESCE(f.active_findings, 0) AS active_findings,
              COALESCE(f.exploitable, 0) AS exploitable,
              COALESCE(f.reintroduced, 0) AS reintroduced
            FROM risk r FULL OUTER JOIN findings f ON f."orgId" = r."orgId"
            ORDER BY r.mean_risk DESC NULLS LAST, 1
            """,
        ),
    ),
)

WEAPONISED_CRITICAL = Card(
    title="Weaponised critical exposure",
    definition=(
        "Active findings that are critical, have a known exploit, are reachable over the "
        "network, and have an EPSS score of 0.5 or more. Days open run from firstFoundTimestamp "
        "to when the data was loaded."
    ),
    queries=(
        (
            "headline",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT COUNT(*) AS findings,
              COUNT(DISTINCT ("orgId", "assetId")) AS assets,
              COUNT(DISTINCT "vulnId") AS distinct_vulns,
              ROUND(AVG(DATEDIFF('day', "firstFoundTimestamp", CAST($as_of AS TIMESTAMP))), 0) AS mean_days_open,
              ROUND(MEDIAN(DATEDIFF('day', "firstFoundTimestamp", CAST($as_of AS TIMESTAMP))), 0) AS median_days_open
            FROM active
            WHERE "cvssV3Severity" = 'Critical' AND "hasExploits"
              AND "cvssV3AttackVector" = 'Network' AND "epssscore" >= 0.5
            """,
        ),
        (
            "by_org",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT "orgId",
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Critical' AND "hasExploits"
                AND "cvssV3AttackVector" = 'Network' AND "epssscore" >= 0.5) AS weaponised_critical,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Critical' AND "hasExploits") AS critical_exploitable,
              COUNT(*) FILTER (WHERE "hasExploits" AND "epssscore" >= 0.5) AS exploitable_epss_50,
              COUNT(*) FILTER (WHERE "epssscore" >= 0.9) AS epss_90
            FROM active GROUP BY "orgId" ORDER BY "orgId"
            """,
        ),
        (
            "by_quarter",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT STRFTIME(DATE_TRUNC('quarter',
                GREATEST("firstFoundTimestamp", COALESCE("reintroducedTimestamp", "firstFoundTimestamp"))),
                '%Y-%m') AS quarter,
              "orgId", COUNT(*) AS weaponised_critical
            FROM active
            WHERE "cvssV3Severity" = 'Critical' AND "hasExploits"
              AND "cvssV3AttackVector" = 'Network' AND "epssscore" >= 0.5
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
        ),
        (
            "top_solutions",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT "bestSolutionSummary", "bestSolutionType",
              COUNT(DISTINCT ("orgId", "assetId")) AS assets, COUNT(*) AS findings,
              COUNT(DISTINCT "vulnId") AS distinct_vulns, COUNT(DISTINCT "orgId") AS orgs,
              ROUND(MAX("epssscore"), 4) AS max_epss
            FROM active
            WHERE "cvssV3Severity" = 'Critical' AND "hasExploits"
              AND "cvssV3AttackVector" = 'Network' AND "epssscore" >= 0.5
              AND "bestSolutionSummary" IS NOT NULL
            GROUP BY 1, 2 ORDER BY assets DESC, findings DESC LIMIT 12
            """,
        ),
        (
            "solution_types",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT COALESCE("bestSolutionType", '(none)') AS solution_type,
              COUNT(*) AS findings, COUNT(DISTINCT ("orgId", "assetId")) AS assets
            FROM active
            WHERE "cvssV3Severity" = 'Critical' AND "hasExploits"
              AND "cvssV3AttackVector" = 'Network' AND "epssscore" >= 0.5
            GROUP BY 1 ORDER BY findings DESC, 1
            """,
        ),
        (
            "top_vulnerabilities",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT "title", "cvssV3Severity", ROUND(MAX("cvssV3Score"), 1) AS cvss,
              ROUND(MAX("epssscore"), 4) AS epss, COUNT(DISTINCT "orgId") AS orgs,
              COUNT(DISTINCT ("orgId", "assetId")) AS assets, COUNT(*) AS findings,
              ROUND(AVG(DATEDIFF('day', "firstFoundTimestamp", CAST($as_of AS TIMESTAMP))), 0) AS mean_days_open
            FROM active
            WHERE "hasExploits" AND "cvssV3Severity" IN ('Critical', 'High') AND "epssscore" >= 0.5
            GROUP BY 1, 2 ORDER BY assets DESC, findings DESC, 1 LIMIT 20
            """,
        ),
        (
            "top_assets",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            ), counted AS (
              SELECT "orgId", "assetId", COUNT(*) AS weaponised_critical
              FROM active
              WHERE "cvssV3Severity" = 'Critical' AND "hasExploits"
                AND "cvssV3AttackVector" = 'Network' AND "epssscore" >= 0.5
              GROUP BY 1, 2
            )
            SELECT c."orgId", a."hostName", a."osProduct", ROUND(a."riskScore", 0) AS risk_score,
              c.weaponised_critical
            FROM counted c JOIN assets a ON a."assetId" = c."assetId" AND a."orgId" = c."orgId"
            ORDER BY c.weaponised_critical DESC, c."orgId", a."hostName" LIMIT 12
            """,
        ),
    ),
)

CARDS: Dict[str, Card] = {
    "portfolio_summary": PORTFOLIO_SUMMARY,
    "org_risk_distribution": ORG_RISK_DISTRIBUTION,
    "vulnerability_activity": VULNERABILITY_ACTIVITY,
    "unresolved_age_severity": UNRESOLVED_AGE_SEVERITY,
    "org_comparison": ORG_COMPARISON,
    "weaponised_critical": WEAPONISED_CRITICAL,
}

# A portfolio where no organisation has an exception may export no exception rows, so
# the table is never created. An empty stand-in keeps every card's "active" rule valid.
_EMPTY_EXCEPTIONS = (
    'vulnerability_exceptions AS (SELECT CAST(NULL AS VARCHAR) AS "orgId", '
    'CAST(NULL AS VARCHAR) AS "assetId", CAST(NULL AS VARCHAR) AS "vulnId" WHERE FALSE)'
)


def _with_empty_exceptions(sql: str) -> str:
    """Prepend the empty exceptions stand-in to a query that starts with a WITH clause."""
    head, body = sql.strip().split(None, 1)
    if head.upper() != "WITH":
        return sql
    return f"WITH {_EMPTY_EXCEPTIONS}, {body}"


def run_card(db: VulnerabilityDatabase, card: str, as_of: Optional[datetime] = None) -> Dict[str, Any]:
    """Run every query behind one dashboard card.

    Args:
        db: The loaded multi-org database.
        card: A key of CARDS.
        as_of: Point in time ages are counted to. Defaults to when the vulnerability
            data was loaded.

    Raises:
        ValueError: If the card is unknown or no vulnerability data is loaded.
    """
    if card not in CARDS:
        raise ValueError(f"Unknown card '{card}'. Valid cards: {', '.join(CARDS)}")

    schema = db.get_schema()
    if "vulnerabilities" not in schema:
        raise ValueError("No vulnerability data is loaded, so no dashboard card can be produced.")

    if as_of is None:
        as_of = db.get_load_metadata().get("vulnerabilities")
        if as_of is None:
            raise ValueError("The vulnerability data has no recorded load time, so ages cannot be counted.")

    exceptions_loaded = "vulnerability_exceptions" in schema
    spec = CARDS[card]
    results: Dict[str, List[Dict[str, Any]]] = {}
    for name, sql in spec.queries:
        if not exceptions_loaded and "vulnerability_exceptions" in sql:
            sql = _with_empty_exceptions(sql)
        params = {"as_of": as_of} if "$as_of" in sql else None
        results[name] = db.query(sql, params)

    orgs = [row["orgId"] for row in db.query('SELECT DISTINCT "orgId" FROM vulnerabilities ORDER BY 1')]
    return {
        "card": card,
        "title": spec.title,
        "definition": spec.definition,
        "as_of": as_of.isoformat(),
        "orgs": orgs,
        "results": results,
    }
