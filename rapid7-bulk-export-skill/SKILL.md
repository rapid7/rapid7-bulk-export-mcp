---
name: Rapid7 Bulk Export Analysis Expert
description: Expert analysis of Rapid7 InsightVM data exported via Bulk Export API with strict MCP requirements
version: 0.2.7
author: Rapid7 Bulk Export MCP Tool
tags: [security, vulnerabilities, rapid7, insightvm, bulk-export, analysis, policy, remediation]
---

# Rapid7 Bulk Export Analysis Expert

**CRITICAL REQUIREMENTS:**
1. The Rapid7 MCP server MUST be installed and configured
2. You MUST ensure data is available before analysis (check with `get_stats()` or `list_exports()`)
3. Without these, you CANNOT help with analysis
4. For a complete dataset, all three export types (vulnerability, policy, remediation) MUST be loaded

## Prerequisites - MANDATORY

This skill ONLY works with the Rapid7 MCP server. You must:

1. **Verify MCP server is available** - Check if you have access to these tools:
   - `list_exports()` - Check available exports and their dates
   - `get_stats()` - Check if data is loaded (covers all tables)
   - `start_export(export_type, start_date, end_date)` - Kick off any export type (instant, non-blocking)
   - `check_export_status(export_id)` - Check export progress (instant)
   - `download_and_load_export(export_id, export_type)` - Download and load a completed export
   - `query(sql)` - Execute SQL queries against all tables
   - `get_schema()` - View table schemas for all tables

2. **Check data availability FIRST** - Before ANY analysis:
   ```
   1. Call list_exports() to see available exports
   2. Call get_stats() to check if data is loaded
   3. Only start a new export if:
      - No data exists
      - Last export is older than 1 day
      - User requests fresh data
   4. For a COMPLETE dataset, ensure all three export types have been run
   ```

3. **If MCP is not available**:
   - STOP immediately
   - Tell the user: "The Rapid7 MCP server is not configured. Please install and configure it first."
   - Provide setup instructions from the README
   - DO NOT attempt to provide analysis without the MCP server

## Data Source

The data comes from Rapid7 InsightVM's Bulk Export API, which exports three types of data:
- **Asset & Vulnerability data**: All assets with agent-based scanning (including cloud identifiers for AWS, Azure, GCP) and all vulnerabilities found on assets (including CVSS v2/v3 scores, EPSS scores, exploit information)
- **Policy compliance data**: Agent-based and scan-based policy assessment results, including benchmark compliance status, rule pass/fail results, and remediation guidance
- **Vulnerability remediation data**: Tracks vulnerability lifecycle — when vulnerabilities were first found, last detected, and last removed — for a specified date range
- **Export format**: Parquet files downloaded and loaded into DuckDB for SQL querying
- **Data freshness**: Refreshed once daily by Rapid7; exports are retained for 30 days
- **Schema**: Based on Rapid7's official Bulk Export API Parquet schema (see docs.rapid7.com/insightvm/bulk-export-api)
- **Database tables**: Data is loaded into four separate tables: `assets`, `vulnerabilities`, `policies`, and `vulnerability_remediation`
- **Export tracking**: The system tracks exports by type in `rapid7_bulk_export_tracking.db` to avoid redundant downloads

## Workflow - INTELLIGENT DATA LOADING

### Step 1: Check Data Availability
```
FIRST ACTION: Check if data is already available

You should:
- Call list_exports() to see available exports and their dates
- Call get_stats() to check if data is loaded and see row counts for all tables
- If data exists and is from today, skip to Step 4 (analysis)
- If no data or stale (>1 day old), proceed to Step 2
- Check which export types have been loaded — for a complete dataset,
  you need vulnerability, policy, AND remediation data
```

### Step 2: Start Exports (If Needed)
```
To load a complete dataset, kick off all three exports, then poll and load each:

1. Call start_export(export_type="vulnerability")
   → Save the vulnerability export_id

2. Call start_export(export_type="policy")
   → Save the policy export_id

3. Call start_export(export_type="remediation", start_date="YYYY-MM-DD", end_date="YYYY-MM-DD")
   → Save the remediation export_id
   → Default to last 30 days if user doesn't specify dates

All three calls return instantly with export IDs. Inform the user:
   "Starting Rapid7 exports. Each takes 3-5 minutes on Rapid7's
    servers. I'll check on them shortly."
```

### Step 3: Monitor and Load
```
4. Wait 30 seconds, then check each export:
   - check_export_status(export_id) for each
   - If still PENDING/PROCESSING, wait another 30 seconds and check again
   - Repeat until all three are COMPLETE

5. Once an export is COMPLETE, load it:
   - download_and_load_export(export_id, export_type="vulnerability")
   - download_and_load_export(export_id, export_type="policy")
   - download_and_load_export(export_id, export_type="remediation")

6. Inform the user:
   - "All exports loaded. X assets, Y vulnerabilities, Z policies,
      W remediation records."
```

### Step 4: Proceed with Analysis
Only after confirming data availability can you proceed with analysis. Remember that different analyses require different tables:
- Vulnerability analysis → `assets` + `vulnerabilities` tables
- Policy compliance analysis → `policies` table (+ `assets` for joins)
- Remediation tracking → `vulnerability_remediation` table (+ `assets` for joins)

## Database Schema

The Rapid7 Bulk Export data is loaded into four separate DuckDB tables:

### 1. Assets Table (`assets`)

The `assets` table contains core asset identification and metadata. Each row represents a unique asset in your environment.

**Identification Fields:**
- `orgId` (String) - Organization ID
- `assetId` (String) - Unique asset identifier
- `agentId` (String) - Rapid7 Agent ID

**Cloud Identifiers:**
- `awsInstanceId` (String) - AWS instance ID (if applicable)
- `azureResourceId` (String) - Azure resource ID (if applicable)
- `gcpObjectId` (String) - GCP object ID (if applicable)

**Network Information:**
- `mac` (String) - Primary MAC address
- `ip` (String) - Primary IP address
- `hostName` (String) - Primary hostname

**Operating System Details:**
- `osArchitecture` (String) - OS architecture (e.g., x86_64)
- `osFamily` (String) - OS family (e.g., Windows, Linux)
- `osProduct` (String) - OS product name
- `osVendor` (String) - OS vendor (e.g., Microsoft)
- `osVersion` (String) - OS version
- `osType` (String) - OS type (e.g., Server, Workstation)
- `osDescription` (String) - Full OS description

**Risk and Organization:**
- `riskScore` (Double) - Asset risk score
- `sites` (List) - Array of sites the asset belongs to
- `assetGroups` (List) - Asset groups
- `tags` (List) - Tags with name and tagType

### 2. Vulnerabilities Table (`vulnerabilities`)

The `vulnerabilities` table contains detailed vulnerability information. Each row represents a vulnerability instance on a specific asset.

**Identification:**
- `orgId` (String) - Organization ID
- `assetId` (String) - Asset identifier
- `vulnId` (String) - Vulnerability identifier
- `checkId` (String) - Unique check identifier

**Network Context:**
- `port` (Integer) - Scanned port (if applicable)
- `protocol` (String) - Protocol (e.g., TCP, UDP)
- `nic` (String) - Network interface (if applicable)

**Vulnerability Details:**
- `title` (String) - Vulnerability title
- `description` (String) - Detailed description (HTML/XML content)
- `proof` (String) - Proof of vulnerability

**CVSS v2 Metrics:**
- `cvssScore` (Double) - CVSS v2 score
- `cvssAccessVector` (String) - Access vector (N, A, L)
- `cvssAccessComplexity` (String) - Access complexity (H, M, L)
- `cvssAuthentication` (String) - Authentication (N, S, M)
- `cvssConfidentialityImpact` (String) - Confidentiality impact (N, P, C)
- `cvssIntegrityImpact` (String) - Integrity impact (N, P, C)
- `cvssAvailabilityImpact` (String) - Availability impact (N, P, C)

**CVSS v3 Metrics:**
- `cvssV3Score` (Double) - CVSS v3 score (0-10)
- `cvssV3Severity` (String) - Severity rating (Low, Medium, High, Critical)
- `cvssV3SeverityRank` (Integer) - Severity rank
- `cvssV3AttackVector` (String) - Attack vector (Network, Adjacent, Local, Physical)
- `cvssV3AttackComplexity` (String) - Attack complexity (Low, High)
- `cvssV3PrivilegesRequired` (String) - Privileges required (None, Low, High)
- `cvssV3UserInteraction` (String) - User interaction (None, Required)
- `cvssV3Scope` (String) - Scope (Unchanged, Changed)
- `cvssV3Confidentiality` (String) - Confidentiality impact (None, Low, High)
- `cvssV3Integrity` (String) - Integrity impact (None, Low, High)
- `cvssV3Availability` (String) - Availability impact (None, Low, High)

**Severity & Risk:**
- `severity` (String) - Severity level (Critical, Severe, Moderate)
- `severityRank` (Integer) - Severity rank
- `severityScore` (Integer) - Severity score
- `riskScore` (Double) - Legacy risk score (use when riskScoreV2_0 not present)
- `riskScoreV2_0` (Integer) - Active risk score (preferred)

**Exploit & Threat Intelligence:**
- `hasExploits` (Boolean) - Whether exploits exist
- `threatFeedExists` (Boolean) - Whether threat feed exists
- `skillLevel` (String) - Required skill level
- `skillLevelRank` (Integer) - Skill level rank
- `epssscore` (Double) - EPSS score (0-1, probability of exploitation in 30 days)
- `epsspercentile` (Double) - EPSS percentile (0-1)

**Compliance:**
- `pciCompliant` (Boolean) - PCI compliance status
- `pciSeverity` (Integer) - PCI severity level

**Temporal Information:**
- `firstFoundTimestamp` (Timestamp) - When first discovered on asset
- `reintroducedTimestamp` (Timestamp) - When reappeared after remediation
- `dateAdded` (Timestamp) - When added to vulnerability database
- `dateModified` (Timestamp) - Last modification date
- `datePublished` (Timestamp) - Publication date

**References:**
- `cves` (List) - Array of CVE IDs
- `tags` (List) - Associated tags

### 3. Policies Table (`policies`)

The `policies` table contains policy compliance assessment results. It combines both agent-based and scan-based policy data into a single table, distinguished by the `source` column.

- **Agent-based policies** (`source = 'agent'`): Collected by the Rapid7 Insight Agent running on the asset. These come from Parquet files with the `asset_policy` prefix.
- **Scan-based policies** (`source = 'scan'`): Collected via network-based policy scans. These come from Parquet files with the `asset_scan_policy` prefix.

This unified design means you can query all policy data in one table without needing UNION queries, and filter by `source` when you need to distinguish between agent and scan results.

**Identification:**
- `orgId` (String) - Organization ID
- `assetId` (String) - Asset identifier (join with `assets` table for asset details)

**Benchmark Information:**
- `benchmarkNaturalId` (String) - Unique benchmark identifier (e.g., CIS benchmark ID)
- `benchmarkTitle` (String) - Human-readable benchmark title
- `benchmarkVersion` (String) - Benchmark version number
- `profileNaturalId` (String) - Profile identifier within the benchmark
- `profileTitle` (String) - Human-readable profile title
- `publisher` (String) - Benchmark publisher (e.g., "CIS")

**Rule Assessment:**
- `ruleNaturalId` (String) - Unique rule identifier within the benchmark
- `ruleTitle` (String) - Human-readable rule title
- `finalStatus` (String) - Assessment result (e.g., "pass", "fail", "error", "notApplicable")
- `proof` (String) - Evidence or details supporting the assessment result
- `lastAssessmentTimestamp` (Timestamp) - When the rule was last assessed

**Remediation Guidance:**
- `fixTexts` (String) - Recommended fix actions for failed rules
- `rationales` (String) - Explanation of why the rule matters

**Data Source:**
- `source` (String) - Values: `'agent'` or `'scan'`. Indicates whether the policy result came from an agent-based assessment or a scan-based assessment.

### 4. Vulnerability Remediation Table (`vulnerability_remediation`)

The `vulnerability_remediation` table tracks the lifecycle of vulnerabilities — when they were first found, last detected, and last removed. This data is exported for a specific date range and is useful for tracking remediation progress over time.

**Identification:**
- `orgId` (String) - Organization ID
- `assetId` (String) - Asset identifier (join with `assets` table for asset details)
- `cveId` (String) - CVE identifier (e.g., "CVE-2024-1234")
- `vulnId` (String) - Rapid7 vulnerability identifier

**Evidence:**
- `proof` (String) - Proof or evidence of the vulnerability

**Lifecycle Timestamps:**
- `firstFoundTimestamp` (Timestamp) - When the vulnerability was first discovered on the asset
- `reintroducedTimestamp` (Timestamp) - When the vulnerability reappeared after being remediated
- `lastDetected` (Timestamp) - When the vulnerability was last seen on the asset
- `lastRemoved` (Timestamp) - When the vulnerability was last removed/remediated (NULL if still present)

**Vulnerability Details:**
- `title` (String) - Vulnerability title
- `description` (String) - Detailed vulnerability description

**CVSS Scores:**
- `cvssV2Score` (Double) - CVSS v2 score
- `cvssV3Score` (Double) - CVSS v3 score (0-10)
- `cvssV2Severity` (String) - CVSS v2 severity rating
- `cvssV3Severity` (String) - CVSS v3 severity rating

**Attack Vectors:**
- `cvssV2AttackVector` (String) - CVSS v2 attack vector
- `cvssV3AttackVector` (String) - CVSS v3 attack vector

**Risk:**
- `riskScoreV2_0` (Integer) - Active risk score

**Temporal Information:**
- `datePublished` (Timestamp) - When the vulnerability was published
- `dateAdded` (Timestamp) - When added to the vulnerability database
- `dateModified` (Timestamp) - Last modification date

**Exploit Prediction:**
- `epssscore` (Double) - EPSS score (0-1, probability of exploitation in 30 days)
- `epsspercentile` (Double) - EPSS percentile (0-1)

## Common Analysis Patterns

### Asset Analysis

#### 1. Asset Inventory
- List all assets with their key identifiers (assetId, hostName, ip, mac)
- Group assets by OS family, type, or vendor
- Identify cloud vs on-premise assets using cloud identifiers
- Track assets by site or asset group membership

#### 2. Cloud Asset Management
- Identify AWS assets using awsInstanceId
- Track Azure resources via azureResourceId
- Monitor GCP objects with gcpObjectId
- Compare security posture across cloud providers

#### 3. Operating System Analysis
- Analyze OS distribution (osFamily, osProduct, osVendor)
- Identify outdated OS versions
- Track server vs workstation deployment (osType)
- Monitor OS architecture distribution (osArchitecture)

#### 4. Asset Risk Assessment
- Identify high-risk assets using riskScore
- Correlate asset risk with vulnerability counts
- Prioritize remediation by asset criticality
- Track risk trends over time

### Vulnerability Analysis

#### 1. Risk Prioritization
Focus on vulnerabilities with:
- High CVSS v3 scores (cvssV3Score >= 7.0)
- Critical or Severe severity
- Known exploits (hasExploits = true)
- High EPSS scores (epssscore > 0.5 indicates >50% exploitation probability)
- Recent discovery dates (firstFoundTimestamp)

#### 2. Exploit Prediction
Use EPSS (Exploit Prediction Scoring System) metrics:
- `epssscore` - Probability (0-1) of exploitation in next 30 days
- `epsspercentile` - Percentile ranking compared to all CVEs
- Combine with hasExploits for comprehensive threat assessment

#### 3. Trend Analysis
- Track vulnerability discovery over time using firstFoundTimestamp
- Identify reintroduced vulnerabilities (reintroducedTimestamp)
- Monitor remediation progress
- Analyze patterns in affected assets

#### 4. Asset-Centric View
- Group vulnerabilities by assetId
- Identify most vulnerable systems using asset riskScore
- Prioritize by OS type, cloud provider, or asset groups
- Consider asset tags for business context

#### 5. Compliance Reporting
- Filter by specific CVEs from cves array
- Generate severity distributions using severity and severityRank
- Track PCI compliance with pciCompliant and pciSeverity
- Calculate vulnerability age from firstFoundTimestamp

### Policy Compliance Analysis

#### 1. Benchmark Compliance
- Summarize pass/fail rates by benchmark (benchmarkTitle)
- Compare compliance across profiles (profileTitle)
- Track compliance trends using lastAssessmentTimestamp
- Identify benchmarks with the lowest compliance rates

#### 2. Rule Failure Analysis
- Identify the most commonly failed rules across assets
- Analyze failure patterns by OS type or asset group (join with assets)
- Review fixTexts for remediation guidance on failed rules
- Prioritize rule failures by the number of affected assets

#### 3. Agent vs Scan Comparison
- Compare policy results between agent-based and scan-based assessments using the `source` column
- Identify discrepancies where agent and scan results differ for the same rule
- Assess coverage gaps between the two assessment methods

### Remediation Analysis

#### 1. Remediation Progress
- Track which vulnerabilities have been remediated (lastRemoved IS NOT NULL)
- Calculate mean time to remediate (MTTR) from firstFoundTimestamp to lastRemoved
- Identify vulnerabilities that were remediated and then reintroduced (reintroducedTimestamp)
- Monitor remediation velocity over time

#### 2. CVE-Specific Tracking
- Track remediation status for specific CVEs across all assets
- Identify assets where a specific CVE is still present (lastRemoved IS NULL)
- Calculate remediation coverage percentage per CVE

#### 3. Risk-Based Remediation
- Prioritize unremediated vulnerabilities by CVSS score and EPSS score
- Track remediation of high-risk vulnerabilities separately
- Correlate remediation timelines with risk scores

## Query Examples

### Asset Queries

#### List All Assets with Key Information
```sql
SELECT
    assetId,
    hostName,
    ip,
    mac,
    osFamily,
    osProduct,
    osVersion,
    osType,
    riskScore
FROM assets
ORDER BY riskScore DESC
LIMIT 100;
```

#### Assets by Operating System
```sql
SELECT
    osFamily,
    osProduct,
    osVendor,
    COUNT(*) as asset_count,
    AVG(riskScore) as avg_risk_score
FROM assets
GROUP BY osFamily, osProduct, osVendor
ORDER BY asset_count DESC;
```

#### Cloud Assets by Provider
```sql
SELECT
    CASE
        WHEN awsInstanceId IS NOT NULL THEN 'AWS'
        WHEN azureResourceId IS NOT NULL THEN 'Azure'
        WHEN gcpObjectId IS NOT NULL THEN 'GCP'
        ELSE 'On-Premise'
    END as cloud_provider,
    COUNT(*) as asset_count,
    AVG(riskScore) as avg_risk_score
FROM assets
GROUP BY cloud_provider
ORDER BY asset_count DESC;
```

#### High-Risk Assets
```sql
SELECT
    assetId,
    hostName,
    ip,
    osDescription,
    riskScore,
    sites,
    assetGroups
FROM assets
WHERE riskScore > 800  -- Adjust threshold as needed
ORDER BY riskScore DESC
LIMIT 50;
```

#### Assets by Site or Group
```sql
-- Assets in specific sites
SELECT
    assetId,
    hostName,
    ip,
    osFamily,
    riskScore
FROM assets
WHERE EXISTS (
    SELECT 1 FROM unnest(sites) AS site
    WHERE site LIKE '%Production%'
)
ORDER BY riskScore DESC;
```

#### Windows vs Linux Distribution
```sql
SELECT
    CASE
        WHEN osFamily LIKE '%Windows%' THEN 'Windows'
        WHEN osFamily LIKE '%Linux%' THEN 'Linux'
        WHEN osFamily LIKE '%Unix%' THEN 'Unix'
        ELSE 'Other'
    END as os_category,
    osType,
    COUNT(*) as count,
    AVG(riskScore) as avg_risk
FROM assets
GROUP BY os_category, osType
ORDER BY count DESC;
```

### Vulnerability Queries

#### Find Critical Vulnerabilities with High Exploitation Probability
```sql
SELECT
    assetId,
    hostName,
    vulnId,
    title,
    cvssV3Score,
    epssscore,
    epsspercentile,
    hasExploits,
    firstFoundTimestamp
FROM vulnerabilities
WHERE severity = 'Critical'
  AND epssscore > 0.5  -- >50% chance of exploitation
ORDER BY epssscore DESC, cvssV3Score DESC
LIMIT 20;
```

### Asset Vulnerability Summary with Risk Scores
```sql
SELECT
    assetId,
    hostName,
    ip,
    osDescription,
    COUNT(*) as total_vulns,
    SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) as critical_count,
    SUM(CASE WHEN hasExploits = true THEN 1 ELSE 0 END) as exploitable_count,
    MAX(riskScoreV2_0) as max_risk_score,
    AVG(cvssV3Score) as avg_cvss_score
FROM vulnerabilities
GROUP BY assetId, hostName, ip, osDescription
ORDER BY critical_count DESC, max_risk_score DESC
LIMIT 50;
```

### Vulnerability Age Analysis
```sql
SELECT
  CASE
    WHEN DATEDIFF('day', firstFoundTimestamp, CURRENT_TIMESTAMP) < 30 THEN '0-30 days'
    WHEN DATEDIFF('day', firstFoundTimestamp, CURRENT_TIMESTAMP) < 90 THEN '30-90 days'
    WHEN DATEDIFF('day', firstFoundTimestamp, CURRENT_TIMESTAMP) < 180 THEN '90-180 days'
    ELSE '180+ days'
  END as age_bucket,
  severity,
  COUNT(*) as count,
  AVG(cvssV3Score) as avg_cvss
FROM vulnerabilities
GROUP BY age_bucket, severity
ORDER BY age_bucket, severity;
```

### CVE Search with Asset Details
```sql
SELECT
    v.assetId,
    v.hostName,
    v.ip,
    v.osDescription,
    v.vulnId,
    v.title,
    v.cvssV3Score,
    v.severity,
    v.cves,
    v.firstFoundTimestamp
FROM vulnerabilities v
WHERE EXISTS (
    SELECT 1 FROM unnest(v.cves) AS cve
    WHERE cve LIKE '%CVE-2024-1234%'
)
ORDER BY v.cvssV3Score DESC;
```

### EPSS-Based Prioritization
```sql
SELECT
    vulnId,
    title,
    cvssV3Score,
    epssscore,
    epsspercentile,
    hasExploits,
    COUNT(DISTINCT assetId) as affected_assets,
    AVG(riskScoreV2_0) as avg_risk_score
FROM vulnerabilities
WHERE epssscore > 0.1  -- >10% exploitation probability
GROUP BY vulnId, title, cvssV3Score, epssscore, epsspercentile, hasExploits
HAVING COUNT(DISTINCT assetId) > 5  -- Affects multiple assets
ORDER BY epssscore DESC, affected_assets DESC
LIMIT 25;
```

### Policy Queries

#### Benchmark Compliance Summary
```sql
SELECT
    benchmarkTitle,
    profileTitle,
    source,
    COUNT(*) as total_rules,
    SUM(CASE WHEN finalStatus = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN finalStatus = 'fail' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN finalStatus = 'error' THEN 1 ELSE 0 END) as errors,
    SUM(CASE WHEN finalStatus = 'notApplicable' THEN 1 ELSE 0 END) as not_applicable,
    ROUND(100.0 * SUM(CASE WHEN finalStatus = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate_pct
FROM policies
GROUP BY benchmarkTitle, profileTitle, source
ORDER BY pass_rate_pct ASC;
```

#### Most Commonly Failed Rules
```sql
SELECT
    ruleNaturalId,
    ruleTitle,
    benchmarkTitle,
    COUNT(DISTINCT assetId) as affected_assets,
    COUNT(*) as total_failures,
    MAX(fixTexts) as fix_guidance
FROM policies
WHERE finalStatus = 'fail'
GROUP BY ruleNaturalId, ruleTitle, benchmarkTitle
ORDER BY affected_assets DESC
LIMIT 25;
```

#### Cross-Asset Policy Comparison
```sql
SELECT
    p.assetId,
    a.hostName,
    a.ip,
    a.osFamily,
    p.benchmarkTitle,
    COUNT(*) as total_rules,
    SUM(CASE WHEN p.finalStatus = 'pass' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN p.finalStatus = 'fail' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN p.finalStatus = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate_pct
FROM policies p
LEFT JOIN assets a ON p.assetId = a.assetId
GROUP BY p.assetId, a.hostName, a.ip, a.osFamily, p.benchmarkTitle
ORDER BY pass_rate_pct ASC
LIMIT 50;
```

#### Agent vs Scan Policy Results Comparison
```sql
SELECT
    benchmarkTitle,
    ruleNaturalId,
    ruleTitle,
    COUNT(DISTINCT CASE WHEN source = 'agent' THEN assetId END) as agent_assessed,
    COUNT(DISTINCT CASE WHEN source = 'scan' THEN assetId END) as scan_assessed,
    SUM(CASE WHEN source = 'agent' AND finalStatus = 'pass' THEN 1 ELSE 0 END) as agent_pass,
    SUM(CASE WHEN source = 'agent' AND finalStatus = 'fail' THEN 1 ELSE 0 END) as agent_fail,
    SUM(CASE WHEN source = 'scan' AND finalStatus = 'pass' THEN 1 ELSE 0 END) as scan_pass,
    SUM(CASE WHEN source = 'scan' AND finalStatus = 'fail' THEN 1 ELSE 0 END) as scan_fail
FROM policies
GROUP BY benchmarkTitle, ruleNaturalId, ruleTitle
HAVING agent_assessed > 0 AND scan_assessed > 0
ORDER BY benchmarkTitle, ruleNaturalId;
```

#### Failed Rules with Remediation Guidance
```sql
SELECT
    ruleNaturalId,
    ruleTitle,
    benchmarkTitle,
    finalStatus,
    fixTexts,
    rationales,
    COUNT(DISTINCT assetId) as affected_assets
FROM policies
WHERE finalStatus = 'fail'
  AND fixTexts IS NOT NULL
GROUP BY ruleNaturalId, ruleTitle, benchmarkTitle, finalStatus, fixTexts, rationales
ORDER BY affected_assets DESC
LIMIT 20;
```

### Remediation Queries

#### Remediation Timeline Overview
```sql
SELECT
    CASE
        WHEN lastRemoved IS NOT NULL THEN 'Remediated'
        ELSE 'Still Present'
    END as status,
    COUNT(*) as vuln_count,
    COUNT(DISTINCT assetId) as affected_assets,
    AVG(cvssV3Score) as avg_cvss,
    AVG(CASE
        WHEN lastRemoved IS NOT NULL
        THEN DATEDIFF('day', firstFoundTimestamp, lastRemoved)
    END) as avg_days_to_remediate
FROM vulnerability_remediation
GROUP BY status;
```

#### CVE Remediation Status
```sql
SELECT
    cveId,
    title,
    cvssV3Score,
    cvssV3Severity,
    COUNT(DISTINCT assetId) as total_assets,
    SUM(CASE WHEN lastRemoved IS NOT NULL THEN 1 ELSE 0 END) as remediated_count,
    SUM(CASE WHEN lastRemoved IS NULL THEN 1 ELSE 0 END) as still_present_count,
    ROUND(100.0 * SUM(CASE WHEN lastRemoved IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as remediation_pct
FROM vulnerability_remediation
GROUP BY cveId, title, cvssV3Score, cvssV3Severity
ORDER BY still_present_count DESC
LIMIT 25;
```

#### Remediation Velocity Metrics
```sql
SELECT
    DATE_TRUNC('week', lastRemoved) as remediation_week,
    COUNT(*) as vulns_remediated,
    COUNT(DISTINCT assetId) as assets_affected,
    AVG(DATEDIFF('day', firstFoundTimestamp, lastRemoved)) as avg_days_open,
    AVG(cvssV3Score) as avg_cvss_remediated
FROM vulnerability_remediation
WHERE lastRemoved IS NOT NULL
GROUP BY remediation_week
ORDER BY remediation_week DESC
LIMIT 12;
```

#### Reintroduced Vulnerabilities (Remediation Regression)
```sql
SELECT
    vr.assetId,
    a.hostName,
    a.ip,
    vr.cveId,
    vr.title,
    vr.cvssV3Score,
    vr.firstFoundTimestamp,
    vr.lastRemoved,
    vr.reintroducedTimestamp,
    DATEDIFF('day', vr.lastRemoved, vr.reintroducedTimestamp) as days_until_reintroduction
FROM vulnerability_remediation vr
LEFT JOIN assets a ON vr.assetId = a.assetId
WHERE vr.reintroducedTimestamp IS NOT NULL
ORDER BY vr.reintroducedTimestamp DESC
LIMIT 50;
```

#### High-Risk Unremediated Vulnerabilities
```sql
SELECT
    vr.assetId,
    a.hostName,
    a.ip,
    vr.cveId,
    vr.title,
    vr.cvssV3Score,
    vr.cvssV3Severity,
    vr.epssscore,
    vr.firstFoundTimestamp,
    DATEDIFF('day', vr.firstFoundTimestamp, CURRENT_TIMESTAMP) as days_open
FROM vulnerability_remediation vr
LEFT JOIN assets a ON vr.assetId = a.assetId
WHERE vr.lastRemoved IS NULL
  AND vr.cvssV3Score >= 7.0
ORDER BY vr.cvssV3Score DESC, days_open DESC
LIMIT 50;
```

#### Mean Time to Remediate by Severity
```sql
SELECT
    cvssV3Severity,
    COUNT(*) as remediated_count,
    AVG(DATEDIFF('day', firstFoundTimestamp, lastRemoved)) as avg_mttr_days,
    MIN(DATEDIFF('day', firstFoundTimestamp, lastRemoved)) as min_mttr_days,
    MAX(DATEDIFF('day', firstFoundTimestamp, lastRemoved)) as max_mttr_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('day', firstFoundTimestamp, lastRemoved)) as median_mttr_days
FROM vulnerability_remediation
WHERE lastRemoved IS NOT NULL
GROUP BY cvssV3Severity
ORDER BY avg_mttr_days DESC;
```

### Joined Asset and Vulnerability Queries

#### Assets with Vulnerability Counts
```sql
SELECT
    a.assetId,
    a.hostName,
    a.ip,
    a.osDescription,
    a.riskScore,
    COUNT(v.vulnId) as total_vulns,
    SUM(CASE WHEN v.severity = 'Critical' THEN 1 ELSE 0 END) as critical_count,
    SUM(CASE WHEN v.severity = 'Severe' THEN 1 ELSE 0 END) as severe_count,
    SUM(CASE WHEN v.hasExploits = true THEN 1 ELSE 0 END) as exploitable_count,
    AVG(v.cvssV3Score) as avg_cvss_score
FROM assets a
LEFT JOIN vulnerabilities v ON a.assetId = v.assetId
GROUP BY a.assetId, a.hostName, a.ip, a.osDescription, a.riskScore
ORDER BY critical_count DESC, a.riskScore DESC
LIMIT 50;
```

#### Cloud Assets with Critical Vulnerabilities
```sql
SELECT
    a.assetId,
    a.hostName,
    a.ip,
    CASE
        WHEN a.awsInstanceId IS NOT NULL THEN 'AWS'
        WHEN a.azureResourceId IS NOT NULL THEN 'Azure'
        WHEN a.gcpObjectId IS NOT NULL THEN 'GCP'
        ELSE 'On-Premise'
    END as cloud_provider,
    a.awsInstanceId,
    a.azureResourceId,
    a.gcpObjectId,
    COUNT(v.vulnId) as critical_vuln_count,
    AVG(v.cvssV3Score) as avg_cvss
FROM assets a
INNER JOIN vulnerabilities v ON a.assetId = v.assetId
WHERE v.severity = 'Critical'
GROUP BY a.assetId, a.hostName, a.ip, cloud_provider, a.awsInstanceId, a.azureResourceId, a.gcpObjectId
ORDER BY critical_vuln_count DESC
LIMIT 50;
```

#### Windows Servers with High-Risk Vulnerabilities
```sql
SELECT
    a.assetId,
    a.hostName,
    a.ip,
    a.osProduct,
    a.osVersion,
    v.vulnId,
    v.title,
    v.cvssV3Score,
    v.epssscore,
    v.hasExploits
FROM assets a
INNER JOIN vulnerabilities v ON a.assetId = v.assetId
WHERE a.osFamily LIKE '%Windows%'
  AND a.osType = 'Server'
  AND v.severity = 'Critical'
  AND v.epssscore > 0.3
ORDER BY v.epssscore DESC, v.cvssV3Score DESC
LIMIT 100;
```

#### Assets Without Vulnerabilities (Clean Assets)
```sql
SELECT
    a.assetId,
    a.hostName,
    a.ip,
    a.osDescription,
    a.riskScore
FROM assets a
LEFT JOIN vulnerabilities v ON a.assetId = v.assetId
WHERE v.vulnId IS NULL
ORDER BY a.riskScore DESC;
```

### Cloud Asset Vulnerabilities
```sql
SELECT
    CASE
        WHEN awsInstanceId IS NOT NULL THEN 'AWS'
        WHEN azureResourceId IS NOT NULL THEN 'Azure'
        WHEN gcpObjectId IS NOT NULL THEN 'GCP'
        ELSE 'On-Premise'
    END as cloud_provider,
    COUNT(DISTINCT assetId) as asset_count,
    COUNT(*) as vuln_count,
    SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) as critical_vulns,
    AVG(cvssV3Score) as avg_cvss
FROM vulnerabilities
GROUP BY cloud_provider
ORDER BY critical_vulns DESC;
```

### Reintroduced Vulnerabilities
```sql
SELECT
    assetId,
    hostName,
    vulnId,
    title,
    severity,
    firstFoundTimestamp,
    reintroducedTimestamp,
    DATEDIFF('day', firstFoundTimestamp, reintroducedTimestamp) as days_between
FROM vulnerabilities
WHERE reintroducedTimestamp IS NOT NULL
ORDER BY reintroducedTimestamp DESC
LIMIT 50;
```

### PCI Compliance Report
```sql
SELECT
    pciSeverity,
    pciCompliant,
    COUNT(*) as vuln_count,
    COUNT(DISTINCT assetId) as affected_assets,
    AVG(cvssV3Score) as avg_cvss
FROM vulnerabilities
WHERE pciSeverity IS NOT NULL
GROUP BY pciSeverity, pciCompliant
ORDER BY pciSeverity DESC;
```

## Best Practices

1. **Always check the schema first** - Use `get_schema()` to see available columns in all tables
2. **Start with statistics** - Use `get_stats()` to understand the data distribution across all tables
3. **Know your tables** - There are four tables: `assets`, `vulnerabilities`, `policies`, and `vulnerability_remediation`
4. **Load all data types** - For comprehensive analysis, use the unified non-blocking tools: `start_export(export_type="vulnerability")`, `start_export(export_type="policy")`, and `start_export(export_type="remediation")` to kick off all three, then `check_export_status()` → `download_and_load_export()` for each.
5. **Join when needed** - Use JOIN queries to correlate asset information with vulnerabilities, policies, or remediation data (all join on `assetId`)
6. **Limit large queries** - Add `LIMIT` clauses when exploring data
7. **Use appropriate filters** - Filter by severity, cvssV3Score, osFamily, cloud provider, finalStatus, source, etc.
8. **Handle NULL values** - Many columns may contain NULL values, especially cloud IDs and remediation timestamps
9. **Consider performance** - Common filter columns: vulnId, assetId, severity, cvssV3Score, hasExploits, hostName, ip, finalStatus, source, cveId
10. **Leverage EPSS scores** - Use epssscore and epsspercentile for data-driven prioritization (available in both vulnerabilities and vulnerability_remediation tables)
11. **Check for reintroductions** - Monitor reintroducedTimestamp in both vulnerabilities and vulnerability_remediation tables to catch recurring issues
12. **Use risk scores wisely** - Prefer riskScoreV2_0 over riskScore when available
13. **Combine metrics** - Use CVSS, EPSS, severity, and hasExploits together for best prioritization
14. **Asset-first analysis** - Start with asset queries to understand your environment before diving into vulnerabilities or policies
15. **Cloud awareness** - Use cloud identifiers (awsInstanceId, azureResourceId, gcpObjectId) to segment analysis by provider
16. **Policy source awareness** - Use the `source` column in the `policies` table to distinguish between agent-based (`'agent'`) and scan-based (`'scan'`) assessments. Compare results from both sources to identify coverage gaps.
17. **Remediation date ranges** - The `vulnerability_remediation` table contains data for a specific date range. Check which range was exported using `list_exports()` to understand the scope of your remediation data.
18. **Track remediation velocity** - Use `firstFoundTimestamp` and `lastRemoved` in the `vulnerability_remediation` table to calculate mean time to remediate (MTTR) and track improvement over time.

## Integration with MCP - REQUIRED

The MCP server provides these tools:

**Export Tools (non-blocking):**
- `start_export(export_type, start_date, end_date)` — Kick off any export type (instant). `export_type` is one of `"vulnerability"`, `"policy"`, or `"remediation"`. `start_date` and `end_date` are only used for remediation exports (YYYY-MM-DD format, defaults to last 30 days).
- `check_export_status(export_id)` — Check if an export is done (instant)
- `download_and_load_export(export_id, export_type)` — Download completed export and load into DB (~1 min). Pass the same `export_type` used in `start_export`.

**Data Management Tools:**
- `list_exports(limit=10)` - List recent exports with dates, types, and metadata (check this FIRST)
- `get_stats()` - Get summary statistics for all tables and verify data is loaded
- `load_from_parquet(parquet_path)` - Load from existing Parquet files (advanced use)

**Query Tools:**
- `query(sql="...")` - Execute SQL queries against all tables (`assets`, `vulnerabilities`, `policies`, `vulnerability_remediation`)
- `get_schema()` - Get table schema for all existing tables

**Recommended Workflow:**
1. Call `list_exports()` — do we have data from today for all export types?
2. Call `get_stats()` — is data loaded in the database for all tables?
3. If no data or stale:
   a. `start_export(export_type="vulnerability")` → save export_id
   b. `start_export(export_type="policy")` → save export_id
   c. `start_export(export_type="remediation", start_date="...", end_date="...")` → save export_id
   d. Wait 30s, then `check_export_status(export_id)` for each
   e. Once COMPLETE: `download_and_load_export(export_id, export_type="...")` for each
4. Proceed with `query()`, `get_schema()`, `get_stats()`

## Error Handling

If you cannot access MCP tools:
1. STOP immediately
2. Inform user: "Rapid7 MCP server is not configured"
3. Direct them to README for setup instructions
4. DO NOT proceed with analysis

If get_stats() shows no data:
1. Call list_exports() to check for available exports
2. If a COMPLETE export exists, call download_and_load_export(export_id, export_type="...")
3. If no exports exist or last export is >1 day old:
   a. Call start_export(export_type="vulnerability") for vulnerability data
   b. Call start_export(export_type="policy") for policy data
   c. Call start_export(export_type="remediation", start_date="...", end_date="...") for remediation data
4. Inform user: "No data available. Starting exports — each takes 3-5 minutes."

If start_export() fails:
1. Show the error to the user
2. Common issues: RAPID7_API_KEY not set, invalid key, invalid region, network issues
3. DO NOT proceed without data

If check_export_status() shows FAILED:
1. Inform user the export failed
2. Offer to retry with start_export(export_type="...")

If download_and_load_export() fails:
1. Show the error — the export ID is included in the response
2. The user can retry with the same export ID
3. DO NOT proceed without data

If start_export(export_type="remediation") fails with date validation error:
1. Check that dates are in YYYY-MM-DD format
2. Ensure start_date != end_date
3. Ensure the date range does not exceed 31 days
4. Retry with corrected dates

If data seems stale:
1. Check list_exports() to see export date
2. Inform user: "Current data is from [date]. Would you like to refresh?"
3. Only call start_export() if user confirms

## Tips for Analysis

- **Check data freshness first**: Always call list_exports() and get_stats() before analysis
- **Inform about data age**: Tell users which export date is being used
- **Start with assets**: Understand your asset inventory before diving into vulnerabilities or policies
- **Use all four tables**: Join `assets`, `vulnerabilities`, `policies`, and `vulnerability_remediation` tables for comprehensive analysis
- **Prioritize by risk**: Combine cvssV3Score, severity, hasExploits, and epssscore for comprehensive risk assessment
- **Consider context**: Asset criticality (from tags, assetGroups, riskScore) matters more than raw vulnerability count
- **Track trends**: Compare current state to historical data using temporal fields
- **Validate remediation**: Check reintroducedTimestamp to ensure vulnerabilities stay fixed
- **Correlate data**: Link vulnerabilities to assets using assetId, then to cloud resources via awsInstanceId, azureResourceId, or gcpObjectId
- **Use EPSS wisely**: High EPSS scores indicate active exploitation attempts - prioritize these even if CVSS is moderate
- **Cloud-aware analysis**: Filter by cloud provider IDs to focus on specific environments (AWS, Azure, GCP)
- **OS-based segmentation**: Group analysis by osFamily, osType, or osVendor for targeted remediation
- **Compliance focus**: Use pciCompliant and pciSeverity for PCI DSS compliance tracking; use the `policies` table for CIS and other benchmark compliance
- **Asset risk correlation**: Compare asset riskScore with vulnerability counts to identify discrepancies
- **Clean asset identification**: Find assets without vulnerabilities to understand your security baseline
- **Policy compliance trends**: Track pass/fail rates over time using lastAssessmentTimestamp in the policies table
- **Remediation MTTR**: Calculate mean time to remediate using firstFoundTimestamp and lastRemoved in the vulnerability_remediation table
- **Agent vs scan coverage**: Compare policy results from agent and scan sources to identify assessment gaps
