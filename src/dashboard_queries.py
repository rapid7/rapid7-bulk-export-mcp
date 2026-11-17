"""Prebaked queries behind the multi-org dashboard cards.

Every card's numbers come from fixed, tested SQL here, never from SQL the AI writes,
so the same data always gives the same answer. The cards and their copy follow the UX
phase 1 prototype. Where the export lacks a figure, the card says what stands in. Two rules
apply throughout:

* "Active" means a finding with no matching vulnerability exception, and that rule
  applies to every card, so the cards reconcile with each other.
* Ages are counted to the time each organisation's vulnerability data was loaded, not to today,
  so a report regenerated later from the same data gives the same numbers.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .duckdb_loader import VulnerabilityDatabase


@dataclass(frozen=True)
class Card:
    """One dashboard card: what it shows and the named queries that produce it."""

    title: str
    description: str
    """The card's lead copy, worded as the UX reference has it wherever the data allows."""
    definition: str
    queries: Tuple[Tuple[str, str], ...]
    per_org: Tuple[str, ...] = ()
    """Queries whose every row describes one organisation alone. Any other query combines
    organisations, so it is wrong while an organisation is missing."""


PORTFOLIO_SUMMARY = Card(
    title="Portfolio summary",
    description="Totals across all selected organisations.",
    definition=(
        "Active findings by CVSS v3 severity, and assets with at least one active finding. "
        "Active means no matching vulnerability exception on orgId, assetId, and vulnId. "
        "Any other severity (Unknown, None, or missing) counts in the total and in unscored. "
        "Findings in CISA KEV are active findings with at least one CVE in CISA's Known Exploited "
        "Vulnerabilities catalogue, which the server fetches after each multi-org load. The report "
        "states the catalogue's version and date. When no catalogue is stored, active findings with "
        "a known exploit (hasExploits) stand in, and a known exploit is not the same as known "
        "exploitation."
    ),
    per_org=("by_org",),
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
              COUNT(*) FILTER (WHERE "cvssV3Severity" IS NULL
                OR "cvssV3Severity" NOT IN ('Critical', 'High', 'Medium', 'Low')) AS unscored,
              COUNT(*) FILTER (WHERE "hasExploits") AS with_exploits
            FROM active GROUP BY "orgId" ORDER BY "orgId"
            """,
        ),
        (
            "portfolio",
            """
            WITH kev AS (
              SELECT DISTINCT p."vulnId"
              FROM (SELECT DISTINCT "vulnId", cves FROM vulnerabilities) p, UNNEST(p.cves) AS u(cve)
              JOIN cisa_kev k ON k."cveID" = u.cve
            ), active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT COUNT(*) AS active_total,
              COUNT(DISTINCT ("orgId", "assetId")) AS assets_with_findings,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Critical') AS critical,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'High') AS high,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Medium') AS medium,
              COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Low') AS low,
              COUNT(*) FILTER (WHERE "hasExploits") AS with_exploits,
              COUNT(*) FILTER (WHERE kev."vulnId" IS NOT NULL) AS kev_findings
            FROM active
            LEFT JOIN kev ON kev."vulnId" = active."vulnId"
            """,
        ),
    ),
)

ORG_RISK_DISTRIBUTION = Card(
    title="Organisation risk distribution",
    description=(
        "One mark per organisation on a shared scale, with the median highlighted. "
        "Hover over a mark to see the organisation and its risk score."
    ),
    definition=(
        "Mean, median, and maximum assets.riskScore over every asset in each organisation, whether "
        "or not it has an active finding. The export has no 0 to 1000 organisation risk score, and "
        "vulnerabilities.riskScore is content-level and identical across organisations, so the mean "
        "asset risk score stands in. The portfolio median is the median of the organisation means, "
        "unweighted, so each organisation counts once."
    ),
    per_org=("by_org",),
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

NEW_VULNERABILITIES = Card(
    title="New critical and high vulnerabilities",
    description=(
        "Count of critical and high vulnerabilities first detected or reintroduced in each week. "
        "One series per organisation. Most group together and outliers stand out. "
        "Hover over a point to see the organisation and its value."
    ),
    definition=(
        "Active critical and high findings by the week each became active: the later of "
        "firstFoundTimestamp and reintroducedTimestamp. Weeks start on Monday. One daily snapshot "
        "holds only findings that are still open, so a finding remediated since does not appear."
    ),
    per_org=("by_week",),
    queries=(
        (
            "by_week",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT STRFTIME(DATE_TRUNC('week',
                GREATEST("firstFoundTimestamp", COALESCE("reintroducedTimestamp", "firstFoundTimestamp"))),
                '%Y-%m-%d') AS week,
              "orgId", COUNT(*) AS new_critical_high
            FROM active WHERE "cvssV3Severity" IN ('Critical', 'High')
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
        ),
    ),
)

REMEDIATED_VULNERABILITIES = Card(
    title="Remediated critical and high vulnerabilities",
    description=(
        "Remediated counts are not available per organisation, so this shows critical and high "
        "vulnerabilities reintroduced in each week: each one was remediated and later came back. "
        "One series per organisation. Hover over a point to see the organisation and its value."
    ),
    definition=(
        "Active critical and high findings by the week of reintroducedTimestamp. Weeks start on "
        "Monday. The remediation export cannot be scoped to one organisation, so reintroduced "
        "findings stand in for remediated ones."
    ),
    per_org=("by_week",),
    queries=(
        (
            "by_week",
            """
            WITH active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            )
            SELECT STRFTIME(DATE_TRUNC('week', "reintroducedTimestamp"), '%Y-%m-%d') AS week,
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
    description="All selected organisations. Days active measured from discovery date.",
    definition=(
        "Active findings by CVSS v3 severity in the bands 0 to 7, 8 to 30, 31 to 60, 61 to 90, "
        "and 91+ days. Days run from firstFoundTimestamp, the discovery date, to when its "
        "organisation's data was loaded. A finding with no discovery date is counted in 91+. "
        "Unknown and None severities are left out."
    ),
    queries=(
        (
            "by_band",
            """
            WITH org_as_of AS (
              SELECT UNNEST($org_ids) AS "orgId", UNNEST($org_as_of) AS as_of
            ), active AS (
              SELECT v."cvssV3Severity",
                DATEDIFF('day', v."firstFoundTimestamp", o.as_of) AS days_active
              FROM vulnerabilities v JOIN org_as_of o ON o."orgId" IS NOT DISTINCT FROM v."orgId"
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
    description="Every selected organisation. Default sorted by risk score.",
    definition=(
        "Risk score is the mean asset risk score, because the export has no organisation risk score. "
        "Total assessed assets counts the organisation's assets in the export. Total critical "
        "vulnerabilities counts active critical findings. The two averages divide active findings, "
        "and active findings with a known exploit, by total assessed assets. Total CISA KEV findings and "
        "CISA KEV per assessed asset match each finding's CVEs to CISA's KEV catalogue, as on the "
        "portfolio summary. When no catalogue is stored, total findings with a known exploit stand "
        "in for total CISA KEV findings. Remediated critical and "
        "high vulnerabilities shows reintroduced ones, because remediated counts are not available "
        "per organisation, so an organisation with no reintroduced findings shows 0, not a clean "
        "record. Exploited in the wild has no source or stand-in, so that column is left out."
    ),
    per_org=("by_org",),
    queries=(
        (
            "by_org",
            """
            WITH kev AS (
              SELECT DISTINCT p."vulnId"
              FROM (SELECT DISTINCT "vulnId", cves FROM vulnerabilities) p, UNNEST(p.cves) AS u(cve)
              JOIN cisa_kev k ON k."cveID" = u.cve
            ), active AS (
              SELECT v.* FROM vulnerabilities v
              WHERE NOT EXISTS (SELECT 1 FROM vulnerability_exceptions e
                WHERE e."orgId" = v."orgId" AND e."assetId" = v."assetId" AND e."vulnId" = v."vulnId")
            ), findings AS (
              SELECT active."orgId", COUNT(*) AS active_findings,
                COUNT(*) FILTER (WHERE kev."vulnId" IS NOT NULL) AS kev_findings,
                COUNT(*) FILTER (WHERE "cvssV3Severity" = 'Critical') AS critical_vulnerabilities,
                COUNT(*) FILTER (WHERE "hasExploits") AS with_known_exploits,
                COUNT(*) FILTER (WHERE "reintroducedTimestamp" IS NOT NULL
                  AND "cvssV3Severity" IN ('Critical', 'High')) AS reintroduced_critical_high
              FROM active LEFT JOIN kev ON kev."vulnId" = active."vulnId"
              GROUP BY active."orgId"
            ), risk AS (
              SELECT "orgId", COUNT(*) AS assets, ROUND(AVG("riskScore"), 1) AS risk_score
              FROM assets GROUP BY "orgId"
            )
            SELECT COALESCE(r."orgId", f."orgId") AS "orgId",
              r.risk_score,
              COALESCE(r.assets, 0) AS total_assessed_assets,
              COALESCE(f.critical_vulnerabilities, 0) AS total_critical_vulnerabilities,
              COALESCE(f.with_known_exploits, 0) AS total_known_exploit_findings,
              COALESCE(f.kev_findings, 0) AS total_kev_findings,
              ROUND(COALESCE(f.kev_findings, 0) / NULLIF(r.assets, 0), 3) AS kev_per_assessed_asset,
              ROUND(COALESCE(f.active_findings, 0) / NULLIF(r.assets, 0), 2) AS average_vulnerabilities_per_asset,
              ROUND(COALESCE(f.with_known_exploits, 0) / NULLIF(r.assets, 0), 2) AS average_known_exploits_per_asset,
              COALESCE(f.reintroduced_critical_high, 0) AS reintroduced_critical_high
            FROM risk r FULL OUTER JOIN findings f ON f."orgId" IS NOT DISTINCT FROM r."orgId"
            ORDER BY r.risk_score DESC NULLS LAST, 1
            """,
        ),
    ),
)

CARDS: Dict[str, Card] = {
    "portfolio_summary": PORTFOLIO_SUMMARY,
    "org_risk_distribution": ORG_RISK_DISTRIBUTION,
    "new_vulnerabilities": NEW_VULNERABILITIES,
    "remediated_vulnerabilities": REMEDIATED_VULNERABILITIES,
    "unresolved_age_severity": UNRESOLVED_AGE_SEVERITY,
    "org_comparison": ORG_COMPARISON,
}

# A portfolio where no organisation has an exception may export no exception rows, so
# the table is never created. An empty stand-in keeps every card's "active" rule valid.
_EMPTY_EXCEPTIONS = (
    'vulnerability_exceptions AS (SELECT CAST(NULL AS VARCHAR) AS "orgId", '
    'CAST(NULL AS VARCHAR) AS "assetId", CAST(NULL AS VARCHAR) AS "vulnId" WHERE FALSE)'
)


KEV_MATCH = """kev AS (
              SELECT DISTINCT p."vulnId"
              FROM (SELECT DISTINCT "vulnId", cves FROM vulnerabilities) p, UNNEST(p.cves) AS u(cve)
              JOIN cisa_kev k ON k."cveID" = u.cve
            ), """
"""The KEV match as it appears in each query. A finding's CVEs come from its vulnerability's
content, so matching on vulnId is exact, and each distinct vulnerability is checked once."""

_NO_KEV_MATCH = 'kev AS (SELECT CAST(NULL AS VARCHAR) AS "vulnId" WHERE FALSE), '
"""Takes the KEV match's place when no catalogue is stored or the export has no cves column."""

KEV_COLUMNS = ("kev_findings", "total_kev_findings", "kev_per_assessed_asset")
"""Card columns that come from the KEV catalogue. They are None while no catalogue is stored."""


def _with_stand_in(sql: str, stand_in: str) -> str:
    """Prepend an empty stand-in table to a query that starts with a WITH clause."""
    head, body = sql.strip().split(None, 1)
    if head.upper() != "WITH":
        return sql
    return f"WITH {stand_in}, {body}"


def _with_empty_exceptions(sql: str) -> str:
    return _with_stand_in(sql, _EMPTY_EXCEPTIONS)


def _kev_catalogue(db: VulnerabilityDatabase) -> Optional[Dict[str, Any]]:
    """The stored KEV catalogue's version, release date and hash, or None when none is stored."""
    present = db.query(
        "SELECT COUNT(*) AS n FROM information_schema.tables "
        "WHERE table_name IN ('cisa_kev', '_kev_catalogue_metadata')"
    )
    if not present or present[0]["n"] != 2:
        return None
    rows = db.query(
        "SELECT catalog_version AS version, date_released AS released, sha256, source, entries "
        "FROM _kev_catalogue_metadata"
    )
    return rows[0] if rows else None


def run_card(db: VulnerabilityDatabase, card: str, as_of: Optional[datetime] = None) -> Dict[str, Any]:
    """Run every query behind one dashboard card.

    Args:
        db: The loaded multi-org database.
        card: A key of CARDS.
        as_of: Point in time every org's ages are counted to. Defaults to each org's
            own vulnerability load time.

    Returns:
        The card's results, with as_of_by_org (each org's data date) and as_of (the
        oldest of them, so the card never claims fresher data than it has).

    Raises:
        ValueError: If the card is unknown, no vulnerability data is loaded, or an org
            has no recorded load time.
    """
    if card not in CARDS:
        raise ValueError(f"Unknown card '{card}'. Valid cards: {', '.join(CARDS)}")

    schema = db.get_schema()
    if "vulnerabilities" not in schema:
        raise ValueError("No vulnerability data is loaded, so no dashboard card can be produced.")

    orgs = [row["orgId"] for row in db.query('SELECT DISTINCT "orgId" FROM vulnerabilities ORDER BY 1')]
    if not orgs:
        raise ValueError("No vulnerability data is loaded, so no dashboard card can be produced.")
    as_of_by_org = _org_data_dates(db, orgs, as_of)

    exceptions_loaded = "vulnerability_exceptions" in schema
    kev = _kev_catalogue(db)
    cves_listed = any(c["column_name"] == "cves" and c["data_type"].endswith("[]") for c in schema["vulnerabilities"])
    kev_usable = kev is not None and cves_listed
    kev_unavailable = None if kev_usable else ("no_catalogue" if kev is None else "no_cves_column")
    if not kev_usable:
        kev = None
    spec = CARDS[card]
    age_params = {"org_ids": list(as_of_by_org), "org_as_of": list(as_of_by_org.values())}
    results: Dict[str, List[Dict[str, Any]]] = {}
    for name, sql in spec.queries:
        if not exceptions_loaded and "vulnerability_exceptions" in sql:
            sql = _with_empty_exceptions(sql)
        if not kev_usable and KEV_MATCH in sql:
            sql = sql.replace(KEV_MATCH, _NO_KEV_MATCH)
        rows = db.query(sql, age_params if "$org_ids" in sql else None)
        if kev is None:
            rows = [{k: (None if k in KEV_COLUMNS else v) for k, v in row.items()} for row in rows]
        results[name] = rows

    return {
        "card": card,
        "title": spec.title,
        "description": spec.description,
        "definition": spec.definition,
        "as_of": _utc_iso(min(as_of_by_org.values())),
        "as_of_by_org": {org: _utc_iso(when) for org, when in as_of_by_org.items()},
        "orgs": orgs,
        "per_org": list(spec.per_org),
        "org_labels": {str(org): label for org, label in db.get_org_labels().items() if org in set(map(str, orgs))},
        "kev_catalogue": kev,
        "kev_unavailable": kev_unavailable,
        "results": results,
    }


def _org_data_dates(db: VulnerabilityDatabase, orgs: List[str], as_of: Optional[datetime]) -> Dict[str, datetime]:
    """Each org's data date: the given as_of, else its own load time, else the table's.

    The table-level time covers databases loaded before per-org times were recorded, and
    rows with no orgId. The loader stores org ids as text, so the lookup uses str(org).
    """
    if as_of is not None:
        if as_of.tzinfo is not None:
            as_of = as_of.astimezone(timezone.utc).replace(tzinfo=None)
        return {org: as_of for org in orgs}
    per_org = db.get_org_load_metadata("vulnerabilities")
    table_level = db.get_load_metadata().get("vulnerabilities")
    dates = {org: (per_org.get(str(org)) if org is not None else None) or table_level for org in orgs}
    undated = [org for org, when in dates.items() if when is None]
    if undated:
        raise ValueError(f"No recorded load time for org(s) {', '.join(undated)}, so ages cannot be counted.")
    return dates


def _utc_iso(when: datetime) -> str:
    """Label a stored UTC clock time as UTC, so a reader never takes it for local time."""
    return when.replace(tzinfo=timezone.utc).isoformat()
