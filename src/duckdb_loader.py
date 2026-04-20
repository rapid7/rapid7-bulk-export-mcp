"""
DuckDB Loader Module

This module handles loading Parquet files into DuckDB for efficient querying
of vulnerability data.
"""

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
}


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
            db_path: Path to persistent database file. If None, uses in-memory database.
        """
        self.db_path = db_path or ":memory:"
        self.conn = duckdb.connect(self.db_path)
        self._setup_database()

    def _setup_database(self):
        """Set up the database schema and indexes."""
        # Table will be created when Parquet files are loaded
        # DuckDB doesn't allow creating tables without columns
        pass

    def load_parquet_files(self, parquet_paths: List[str]) -> int:
        """
        Load Parquet files into separate tables based on their schema.

        The Rapid7 Bulk Export API returns two types of files:
        - Asset files: Contains asset information (prefix="asset")
        - Asset-Vulnerability files: Contains vulnerability instances (prefix="asset_vulnerability")

        This method detects the file type and loads them into appropriate tables.

        Args:
            parquet_paths: List of file paths to Parquet files

        Returns:
            int: Total number of rows loaded across all tables
        """
        if not parquet_paths:
            raise ValueError("No Parquet files provided")

        # Separate files by type based on their schema
        asset_files = []
        vuln_files = []

        for path in parquet_paths:
            # Peek at the schema to determine file type
            try:
                schema_query = f"SELECT * FROM read_parquet('{path}', filename=true) LIMIT 0"
                result = self.conn.execute(schema_query)
                columns = [desc[0] for desc in result.description]

                # Asset files have columns like hostName, ip, mac, osFamily
                # Vulnerability files have columns like vulnId, checkId, severity, cvssV3Score
                if "vulnId" in columns or "checkId" in columns:
                    vuln_files.append(path)
                else:
                    asset_files.append(path)
            except Exception as e:
                print(f"Warning: Could not determine type for {path}: {e}", file=sys.stderr)
                continue

        total_rows = 0

        # Load asset files into 'assets' table
        if asset_files:
            self.conn.execute("DROP TABLE IF EXISTS assets")
            if len(asset_files) == 1:
                self.conn.execute(f"""
                    CREATE TABLE assets AS
                    SELECT * FROM read_parquet('{asset_files[0]}')
                """)
            else:
                asset_list = ", ".join([f"'{p}'" for p in asset_files])
                self.conn.execute(f"""
                    CREATE TABLE assets AS
                    SELECT * FROM read_parquet([{asset_list}], union_by_name=true)
                """)

            result = self.conn.execute("SELECT COUNT(*) FROM assets").fetchone()
            asset_count = result[0] if result else 0
            total_rows += asset_count
            print(f"Loaded {asset_count} assets into 'assets' table", file=sys.stderr)

        # Load vulnerability files into 'vulnerabilities' table
        if vuln_files:
            self.conn.execute("DROP TABLE IF EXISTS vulnerabilities")
            if len(vuln_files) == 1:
                self.conn.execute(f"""
                    CREATE TABLE vulnerabilities AS
                    SELECT * FROM read_parquet('{vuln_files[0]}')
                """)
            else:
                vuln_list = ", ".join([f"'{p}'" for p in vuln_files])
                self.conn.execute(f"""
                    CREATE TABLE vulnerabilities AS
                    SELECT * FROM read_parquet([{vuln_list}], union_by_name=true)
                """)

            result = self.conn.execute("SELECT COUNT(*) FROM vulnerabilities").fetchone()
            vuln_count = result[0] if result else 0
            total_rows += vuln_count
            print(f"Loaded {vuln_count} vulnerability instances into 'vulnerabilities' table", file=sys.stderr)

        # Create indexes for common query patterns
        self._create_indexes()

        return total_rows

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

        # Track which tables have been created fresh in this call
        tables_created: Set[str] = set()
        # Accumulate row counts per table
        row_counts: Dict[str, int] = {}

        for prefix, file_paths in prefix_file_map.items():
            if prefix in skip_prefixes:
                continue

            mapping = PREFIX_TABLE_MAP.get(prefix)
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
                        select_expr = f"SELECT *, '{source_value}' AS source FROM read_parquet('{file_path}')"
                    else:
                        select_expr = f"SELECT * FROM read_parquet('{file_path}')"

                    if table_name not in tables_created:
                        # First load for this table in this call — drop and create
                        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        self.conn.execute(f"CREATE TABLE {table_name} AS {select_expr}")
                        tables_created.add(table_name)
                    else:
                        # Subsequent load — insert into existing table
                        self.conn.execute(f"INSERT INTO {table_name} {select_expr}")
                except Exception as e:
                    print(
                        f"Warning: Failed to read Parquet file '{file_path}': {e}",
                        file=sys.stderr,
                    )
                    continue

        # Collect row counts for all tables that were loaded
        for table_name in tables_created:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_counts[table_name] = result[0] if result else 0

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
                self.conn.execute(f"SELECT {col} FROM vulnerabilities LIMIT 1")
                # Note: DuckDB doesn't require explicit indexes for performance
                # It automatically optimizes queries based on column statistics
            except Exception:
                pass  # Column doesn't exist in this export, skip

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dictionaries.

        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries

        Returns:
            List of dictionaries, one per row
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
        known_tables = ["assets", "vulnerabilities", "policies", "vulnerability_remediation"]
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
                continue

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

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
