# Analysis Workflows

## Data Loading Workflow

Always check data availability before analysis:

```
1. Call list_rapid7_exports() to see recent exports
2. Call get_rapid7_stats() to check if data is loaded
3. If data exists from today → skip to analysis
4. If no data or stale → run export workflow below
```

### Full Export Workflow (All Data Types)

```
1. start_rapid7_export(export_type="vulnerability")  → save export_id_1
2. start_rapid7_export(export_type="policy")         → save export_id_2
3. start_rapid7_export(export_type="remediation")    → save export_id_3
4. Wait 30 seconds
5. check_rapid7_export_status(export_id=export_id_1)
6. check_rapid7_export_status(export_id=export_id_2)
7. check_rapid7_export_status(export_id=export_id_3)
8. Once COMPLETE:
   download_rapid7_export(export_id=export_id_1, export_type="vulnerability")
   download_rapid7_export(export_id=export_id_2, export_type="policy")
   download_rapid7_export(export_id=export_id_3, export_type="remediation")
```

Each export takes 3-5 minutes. Check every 30-60 seconds until COMPLETE.

## Common Query Patterns

### Risk Prioritization

Find the most urgent vulnerabilities to remediate:

```sql
SELECT
    assetId, hostName, vulnId, title,
    cvssV3Score, epssscore, hasExploits, severity
FROM vulnerabilities
WHERE severity = 'Critical'
  AND (epssscore > 0.5 OR hasExploits = true)
ORDER BY epssscore DESC, cvssV3Score DESC
LIMIT 20;
```

### Severity Distribution

```sql
SELECT severity, COUNT(*) as count, AVG(cvssV3Score) as avg_cvss
FROM vulnerabilities
GROUP BY severity
ORDER BY count DESC;
```

### Asset Risk Summary

```sql
SELECT
    assetId, hostName, ip, osDescription,
    COUNT(*) as total_vulns,
    SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) as critical_count,
    SUM(CASE WHEN hasExploits = true THEN 1 ELSE 0 END) as exploitable_count
FROM vulnerabilities
GROUP BY assetId, hostName, ip, osDescription
ORDER BY critical_count DESC, total_vulns DESC
LIMIT 20;
```

### CVE Search

```sql
SELECT assetId, hostName, vulnId, title, cvssV3Score, cves
FROM vulnerabilities
WHERE EXISTS (SELECT 1 FROM unnest(cves) AS cve WHERE cve LIKE '%CVE-2024%');
```

### Cloud Provider Breakdown

```sql
SELECT
    CASE
        WHEN awsInstanceId IS NOT NULL THEN 'AWS'
        WHEN azureResourceId IS NOT NULL THEN 'Azure'
        WHEN gcpObjectId IS NOT NULL THEN 'GCP'
        ELSE 'On-Premise'
    END as cloud_provider,
    COUNT(*) as vuln_count,
    COUNT(DISTINCT assetId) as asset_count
FROM vulnerabilities
GROUP BY cloud_provider;
```

### EPSS-Based Prioritization

```sql
SELECT
    vulnId, title, cvssV3Score, epssscore, epsspercentile,
    COUNT(DISTINCT assetId) as affected_assets
FROM vulnerabilities
WHERE epssscore > 0.1
GROUP BY vulnId, title, cvssV3Score, epssscore, epsspercentile
HAVING COUNT(DISTINCT assetId) > 5
ORDER BY epssscore DESC;
```

### Policy Compliance Summary

```sql
SELECT
    benchmarkTitle, profileTitle, source,
    COUNT(*) as total_rules,
    SUM(CASE WHEN finalStatus = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN finalStatus = 'fail' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN finalStatus = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate_pct
FROM policies
GROUP BY benchmarkTitle, profileTitle, source
ORDER BY pass_rate_pct ASC;
```

### Most Commonly Failed Rules

```sql
SELECT
    ruleTitle, benchmarkTitle,
    COUNT(DISTINCT assetId) as affected_assets,
    MAX(fixTexts) as fix_guidance
FROM policies
WHERE finalStatus = 'fail'
GROUP BY ruleTitle, benchmarkTitle
ORDER BY affected_assets DESC
LIMIT 25;
```

### Remediation Progress

```sql
SELECT
    CASE WHEN lastRemoved IS NOT NULL THEN 'Remediated' ELSE 'Still Present' END as status,
    COUNT(*) as vuln_count,
    COUNT(DISTINCT assetId) as affected_assets,
    AVG(cvssV3Score) as avg_cvss,
    AVG(CASE WHEN lastRemoved IS NOT NULL
        THEN DATEDIFF('day', firstFoundTimestamp, lastRemoved) END) as avg_days_to_remediate
FROM vulnerability_remediation
GROUP BY status;
```

### Mean Time to Remediate by Severity

```sql
SELECT
    cvssV3Severity,
    COUNT(*) as remediated_count,
    AVG(DATEDIFF('day', firstFoundTimestamp, lastRemoved)) as avg_mttr_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('day', firstFoundTimestamp, lastRemoved)) as median_mttr_days
FROM vulnerability_remediation
WHERE lastRemoved IS NOT NULL
GROUP BY cvssV3Severity
ORDER BY avg_mttr_days DESC;
```

### Reintroduced Vulnerabilities

```sql
SELECT
    assetId, hostName, cveId, title, cvssV3Score,
    firstFoundTimestamp, lastRemoved, reintroducedTimestamp,
    DATEDIFF('day', lastRemoved, reintroducedTimestamp) as days_until_reintroduction
FROM vulnerability_remediation
WHERE reintroducedTimestamp IS NOT NULL
ORDER BY reintroducedTimestamp DESC
LIMIT 50;
```

## Analysis Guidance

### Vulnerability Prioritization Framework

Combine multiple signals for effective prioritization:
1. **EPSS score** — Probability of exploitation in next 30 days (>0.5 = high risk)
2. **CVSS v3 score** — Technical severity (>=9.0 = critical)
3. **Known exploits** — `hasExploits = true` means active exploitation tools exist
4. **Asset criticality** — Join with `assets` table for `riskScoreV2_0` (Active Risk, scale: 1–1000; cap display at 1000). Legacy `riskScore` is deprecated as of Jan 2026.
5. **Exposure** — Count of affected assets (`COUNT(DISTINCT assetId)`)

### Table Relationships

- `vulnerabilities.assetId` → `assets.assetId` (asset details for vuln context)
- `policies.assetId` → `assets.assetId` (asset details for compliance context)
- `vulnerability_remediation.assetId` → `assets.assetId` (asset details for remediation)
- `vulnerability_remediation.vulnId` → `vulnerabilities.vulnId` (vuln details for remediation)

### DuckDB-Specific SQL Features

- `unnest(array_column)` — Expand array columns (e.g., `cves`, `sites`, `tags`)
- `DATEDIFF('day', start, end)` — Date difference calculation
- `DATE_TRUNC('week', timestamp)` — Truncate to time period
- `FILTER (WHERE condition)` — Conditional aggregation
- `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)` — Median calculation
- `union_by_name=true` — Used internally for Parquet loading with varying schemas
