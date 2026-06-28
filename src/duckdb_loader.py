"""
DuckDB Loader Module

This module handles loading Parquet files into DuckDB for efficient querying
of vulnerability data.
"""

import os
import sys
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import duckdb

# Maps Rapid7 API result prefixes to target DuckDB tables.
# Tuple values indicate (table_name, source_column_value) for policy prefixes.
PREFIX_TABLE_MAP: Dict[str, Union[str, Tuple[str, str]]] = {
    "asset": "assets",
    "asset_vulnerability": "vulnerabilities",
    "asset_policy": ("policies", "agent"),
    "asset_scan_policy": ("policies", "scan"),
    "vulnerability_remediation": "vulnerability_remediation",
    "asset_software": "asset_software",
}


def _normalize_prefix(prefix: str) -> str:
    """Normalize API prefix to match PREFIX_TABLE_MAP keys.

    The Rapid7 API sometimes returns prefixes with sub-path suffixes
    (e.g., 'vulnerability_remediation/ivm' instead of 'vulnerability_remediation').
    This strips those suffixes to match our routing map.
    """
    # Try exact match first
    if prefix in PREFIX_TABLE_MAP:
        return prefix
    # Strip sub-path (e.g., 'vulnerability_remediation/ivm' -> 'vulnerability_remediation')
    base_prefix = prefix.split("/")[0]
    if base_prefix in PREFIX_TABLE_MAP:
        return base_prefix
    return prefix


class VulnerabilityDatabase:
    """
    Manages a DuckDB database for vulnerability data.

    Provides methods to load Parquet files, execute queries, and retrieve
    schema information.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the vulnerability database.

        Args:
            db_path: Path to persistent database file. Defaults to 'rapid7_bulk_export.db'.
        """
        self.db_path = db_path or "rapid7_bulk_export.db"
        self.conn = duckdb.connect(self.db_path)
        self._locked_down = False
        # Restrict file permissions to owner-only
        os.chmod(self.db_path, 0o600)
        self._setup_database()

    def _setup_database(self):
        """Set up the database schema and indexes."""
        # Table will be created when Parquet files are loaded
        # DuckDB doesn't allow creating tables without columns
        pass

    def _lockdown(self):
        """Lock down the connection by disabling external filesystem access.

        This is a one-way operation per connection — once disabled, external
        access cannot be re-enabled. If more data needs to be loaded later,
        the connection is reopened via _ensure_unlocked().

        Blocks: read_csv, read_parquet, read_json, glob, and any other
        filesystem or network access from user SQL queries.
        """
        if not self._locked_down:
            self.conn.execute("SET enable_external_access = false")
            self._locked_down = True

    def _ensure_unlocked(self):
        """Ensure the connection allows external access for data loading.

        If the connection was previously locked down, close and reopen it
        since DuckDB doesn't allow re-enabling external access at runtime.
        """
        if self._locked_down:
            self.conn.close()
            self.conn = duckdb.connect(self.db_path)
            self._locked_down = False

    def load_parquet_files_by_prefix(
        self,
        prefix_file_map: Dict[str, List[str]],
        skip_prefixes: Set[str] = None,
    ) -> Dict[str, int]:
        """
        Load Parquet files into tables based on prefix routing.

        Routing rules (from PREFIX_TABLE_MAP):
          'asset'                    → assets table
          'asset_vulnerability'      → vulnerabilities table
          'asset_policy'             → policies table (source='agent')
          'asset_scan_policy'        → policies table (source='scan')
          'vulnerability_remediation'→ vulnerability_remediation table

        Args:
            prefix_file_map: Mapping of prefixes to lists of local Parquet file paths.
            skip_prefixes: Optional set of prefixes to skip (e.g., {'asset'} during
                policy-only loads to avoid duplicating asset data).

        Returns:
            Dict mapping table names to total row counts loaded.
        """
        if skip_prefixes is None:
            skip_prefixes = set()

        # Reopen connection if previously locked down
        self._ensure_unlocked()

        # Track which tables have been created fresh in this call
        tables_created: Set[str] = set()
        # Accumulate row counts per table
        row_counts: Dict[str, int] = {}

        for prefix, file_paths in prefix_file_map.items():
            if prefix in skip_prefixes:
                continue

            # Normalize prefix to handle sub-path suffixes (e.g., 'vulnerability_remediation/ivm')
            normalized_prefix = _normalize_prefix(prefix)
            if normalized_prefix in skip_prefixes:
                continue

            mapping = PREFIX_TABLE_MAP.get(normalized_prefix)
            if mapping is None:
                print(f"Warning: Unknown prefix '{prefix}', skipping", file=sys.stderr)
                continue

            # Determine target table and optional source value
            if isinstance(mapping, tuple):
                table_name, source_value = mapping
            else:
                table_name = mapping
                source_value = None

            for file_path in file_paths:
                try:
                    # Try reading the parquet file
                    if source_value is not None:
                        select_expr = (
                            f"SELECT *, '{source_value}' AS source"
                            f" FROM read_parquet('{file_path}')"  # nosec B608
                        )
                    else:
                        select_expr = f"SELECT * FROM read_parquet('{file_path}')"  # nosec B608

                    if table_name not in tables_created:
                        # First load for this table in this call — drop and create
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")  # nosec B608
                        self.conn.execute(f"CREATE TABLE {table_name} AS {select_expr}")  # nosec B608
                        tables_created.add(table_name)
                    else:
                        # Subsequent load — insert into existing table
                        self.conn.execute(f"INSERT INTO {table_name} {select_expr}")  # nosec B608
                except Exception as e:
                    print(
                        f"Warning: Failed to read Parquet file '{file_path}': {e}",
                        file=sys.stderr,
                    )
                    continue

        # Collect row counts for all tables that were loaded
        for table_name in tables_created:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # nosec B608
            row_counts[table_name] = result[0] if result else 0

        # Lock down external access now that loading is complete
        self._lockdown()

        return row_counts

    def _create_indexes(self):
        """Create indexes on commonly queried columns.

        Based on the Rapid7 Bulk Export API Parquet schema, we optimize for:
        - Vulnerability identification: vulnId, assetId, checkId
        - Severity filtering: severity, severityRank, cvssV3Score, cvssV3Severity
        - Temporal queries: firstFoundTimestamp, reintroducedTimestamp
        - Exploit analysis: hasExploits, epssscore
        - Asset identification: hostName, ip, mac
        """
        # Common columns from the actual Rapid7 Parquet schema
        common_index_columns = [
            # Vulnerability identification
            "vulnId",
            "assetId",
            "checkId",
            # Severity and risk
            "severity",
            "severityRank",
            "cvssV3Score",
            "cvssV3Severity",
            "riskScore",
            "riskScoreV2_0",
            # Temporal
            "firstFoundTimestamp",
            "reintroducedTimestamp",
            # Exploit intelligence
            "hasExploits",
            "epssscore",
            "epsspercentile",
            # Asset identification
            "hostName",
            "ip",
            "mac",
            # Cloud identifiers
            "awsInstanceId",
            "azureResourceId",
            "gcpObjectId",
        ]

        for col in common_index_columns:
            try:
                # Check if column exists by attempting to select it
                self.conn.execute(f"SELECT {col} FROM vulnerabilities LIMIT 1")  # nosec B608
                # Note: DuckDB doesn't require explicit indexes for performance
                # It automatically optimizes queries based on column statistics
            except Exception:
                pass  # nosec B110 - column doesn't exist in this export, skip

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dictionaries.

        The connection has external access disabled after data loading,
        enforced at the DuckDB engine level. Filesystem reads (read_csv,
        read_parquet, etc.) and network access are blocked regardless of
        what SQL is submitted.

        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries

        Returns:
            List of dictionaries, one per row

        Raises:
            ValueError: If the query fails (e.g., blocked by external access restriction)
        """
        try:
            if params:
                result = self.conn.execute(sql, params).fetchall()
            else:
                result = self.conn.execute(sql).fetchall()

            # Get column names
            description = self.conn.description
            if not description:
                return []

            columns = [desc[0] for desc in description]

            # Convert to list of dicts
            return [dict(zip(columns, row)) for row in result]

        except Exception as e:
            raise ValueError(f"Query execution failed: {str(e)}") from e

    def get_schema(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get the schema of all existing tables.

        Queries information_schema.columns for each known table
        (assets, vulnerabilities, policies, vulnerability_remediation)
        and returns only those that exist.

        Returns:
            Dictionary keyed by table name, each value is a list of
            dictionaries with column_name and data_type.
        """
        known_tables = ["assets", "vulnerabilities", "policies", "vulnerability_remediation", "asset_software"]
        schemas: Dict[str, List[Dict[str, str]]] = {}

        for table_name in known_tables:
            try:
                result = self.conn.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = ?
                    ORDER BY ordinal_position
                """,
                    [table_name],
                ).fetchall()

                if result:
                    schemas[table_name] = [{"column_name": row[0], "data_type": row[1]} for row in result]
            except Exception:
                # Table doesn't exist or query failed — skip it
                continue  # nosec B112

        return schemas

    def get_stats(self) -> Dict[str, Any]:
        """
        Get summary statistics for all existing tables.

        Returns a dictionary keyed by table name, where each value contains
        statistics relevant to that table. Tables that don't exist are omitted.

        Returns:
            Dictionary keyed by table name with per-table statistics.
        """
        all_stats: Dict[str, Any] = {}

        # --- vulnerabilities ---
        vuln_stats = self._get_vulnerabilities_stats()
        if vuln_stats is not None:
            all_stats["vulnerabilities"] = vuln_stats

        # --- assets ---
        assets_stats = self._get_assets_stats()
        if assets_stats is not None:
            all_stats["assets"] = assets_stats

        # --- policies ---
        policies_stats = self._get_policies_stats()
        if policies_stats is not None:
            all_stats["policies"] = policies_stats

        # --- vulnerability_remediation ---
        remediation_stats = self._get_remediation_stats()
        if remediation_stats is not None:
            all_stats["vulnerability_remediation"] = remediation_stats

        # --- asset_software ---
        software_stats = self._get_asset_software_stats()
        if software_stats is not None:
            all_stats["asset_software"] = software_stats

        return all_stats

    def _get_vulnerabilities_stats(self) -> Optional[Dict[str, Any]]:
        """Gather statistics for the vulnerabilities table. Returns None if table doesn't exist."""
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM vulnerabilities").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        # Count distinct assets and vulnerabilities
        try:
            counts = self.conn.execute("""
                SELECT
                    COUNT(DISTINCT assetId) as asset_count,
                    COUNT(DISTINCT vulnId) as vuln_count
                FROM vulnerabilities
            """).fetchone()
            if counts:
                stats["unique_assets"] = counts[0]
                stats["unique_vulnerabilities"] = counts[1]
        except Exception:
            pass

        # Severity distribution
        try:
            severity_dist = self.conn.execute("""
                SELECT severity, COUNT(*) as count
                FROM vulnerabilities
                WHERE severity IS NOT NULL
                GROUP BY severity
                ORDER BY count DESC
            """).fetchall()
            stats["severity_distribution"] = {row[0]: row[1] for row in severity_dist}
        except Exception:
            pass

        # CVSS v3 score statistics
        try:
            cvss_stats = self.conn.execute("""
                SELECT
                    MIN(cvssV3Score) as min_score,
                    MAX(cvssV3Score) as max_score,
                    AVG(cvssV3Score) as avg_score,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cvssV3Score) as median_score
                FROM vulnerabilities
                WHERE cvssV3Score IS NOT NULL
            """).fetchone()
            if cvss_stats:
                stats["cvss_v3_stats"] = {
                    "min": cvss_stats[0],
                    "max": cvss_stats[1],
                    "avg": round(cvss_stats[2], 2) if cvss_stats[2] else None,
                    "median": round(cvss_stats[3], 2) if cvss_stats[3] else None,
                }
        except Exception:
            pass

        # Exploit statistics
        try:
            exploit_stats = self.conn.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE hasExploits = true) as with_exploits,
                    COUNT(*) FILTER (WHERE epssscore > 0.5) as high_epss,
                    AVG(epssscore) as avg_epss
                FROM vulnerabilities
            """).fetchone()
            if exploit_stats:
                stats["exploit_stats"] = {
                    "vulnerabilities_with_exploits": exploit_stats[0],
                    "high_epss_score_count": exploit_stats[1],
                    "avg_epss_score": round(exploit_stats[2], 4) if exploit_stats[2] else None,
                }
        except Exception:
            pass

        # Cloud provider distribution
        try:
            cloud_dist = self.conn.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE awsInstanceId IS NOT NULL) as aws_assets,
                    COUNT(*) FILTER (WHERE azureResourceId IS NOT NULL) as azure_assets,
                    COUNT(*) FILTER (WHERE gcpObjectId IS NOT NULL) as gcp_assets
                FROM vulnerabilities
            """).fetchone()
            if cloud_dist and any(cloud_dist):
                stats["cloud_distribution"] = {"aws": cloud_dist[0], "azure": cloud_dist[1], "gcp": cloud_dist[2]}
        except Exception:
            pass

        return stats

    def _get_assets_stats(self) -> Optional[Dict[str, Any]]:
        """Gather statistics for the assets table. Returns None if table doesn't exist."""
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM assets").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            counts = self.conn.execute("""
                SELECT COUNT(DISTINCT assetId) FROM assets
            """).fetchone()
            if counts:
                stats["unique_assets"] = counts[0]
        except Exception:
            pass

        try:
            os_dist = self.conn.execute("""
                SELECT osFamily, COUNT(*) as count
                FROM assets
                WHERE osFamily IS NOT NULL
                GROUP BY osFamily
                ORDER BY count DESC
            """).fetchall()
            if os_dist:
                stats["os_family_distribution"] = {row[0]: row[1] for row in os_dist}
        except Exception:
            pass

        return stats

    def _get_policies_stats(self) -> Optional[Dict[str, Any]]:
        """Gather statistics for the policies table. Returns None if table doesn't exist."""
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM policies").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            status_dist = self.conn.execute("""
                SELECT finalStatus, COUNT(*) as count
                FROM policies
                WHERE finalStatus IS NOT NULL
                GROUP BY finalStatus
                ORDER BY count DESC
            """).fetchall()
            if status_dist:
                stats["finalStatus_distribution"] = {row[0]: row[1] for row in status_dist}
        except Exception:
            pass

        try:
            source_dist = self.conn.execute("""
                SELECT source, COUNT(*) as count
                FROM policies
                WHERE source IS NOT NULL
                GROUP BY source
                ORDER BY count DESC
            """).fetchall()
            if source_dist:
                stats["source_distribution"] = {row[0]: row[1] for row in source_dist}
        except Exception:
            pass

        return stats

    def _get_remediation_stats(self) -> Optional[Dict[str, Any]]:
        """Gather statistics for the vulnerability_remediation table. Returns None if table doesn't exist."""
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM vulnerability_remediation").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            severity_dist = self.conn.execute("""
                SELECT cvssV3Severity, COUNT(*) as count
                FROM vulnerability_remediation
                WHERE cvssV3Severity IS NOT NULL
                GROUP BY cvssV3Severity
                ORDER BY count DESC
            """).fetchall()
            if severity_dist:
                stats["severity_distribution"] = {row[0]: row[1] for row in severity_dist}
        except Exception:
            pass

        return stats

    def _get_asset_software_stats(self) -> Optional[Dict[str, Any]]:
        """Gather statistics for the asset_software table. Returns None if table doesn't exist."""
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM asset_software").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            counts = self.conn.execute("""
                SELECT COUNT(DISTINCT assetId) FROM asset_software
            """).fetchone()
            if counts:
                stats["unique_assets"] = counts[0]
        except Exception:
            pass

        return stats

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def purge(self):
        """Purge all data by dropping all tables and deleting the database file.

        Closes the connection, removes the database file and any associated
        WAL file from disk, then reinitializes with a fresh connection.
        """
        # Close existing connection
        if self.conn:
            self.conn.close()
            self.conn = None

        # Delete database file and WAL
        for suffix in ("", ".wal"):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.remove(path)

        # Reinitialize fresh
        self.conn = duckdb.connect(self.db_path)
        self._locked_down = False
        os.chmod(self.db_path, 0o600)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
