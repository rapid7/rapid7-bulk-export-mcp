"""
DuckDB Loader Module

This module handles loading Parquet files into DuckDB for efficient querying
of vulnerability data.
"""

import os
import sys
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from .db_utils import connect_with_retry, duckdb_connection

KNOWN_TABLES = [
    "assets",
    "vulnerabilities",
    "vulnerability_exceptions",
    "policies",
    "vulnerability_remediation",
    "asset_software",
]

# Maps Rapid7 API result prefixes to target DuckDB tables.
# Tuple values indicate (table_name, source_column_value) for policy prefixes.
PREFIX_TABLE_MAP: Dict[str, Union[str, Tuple[str, str]]] = {
    "asset": "assets",
    "asset_vulnerability": "vulnerabilities",
    "vulnerability_exception": "vulnerability_exceptions",
    "asset_policy": ("policies", "agent"),
    "asset_scan_policy": ("policies", "scan"),
    "vulnerability_remediation": "vulnerability_remediation",
    "asset_software": "asset_software",
}


def _delete_db_files(db_path: str) -> None:
    """Delete the database file and its WAL so the next write starts from zero."""
    for suffix in ("", ".wal"):
        path = db_path + suffix
        if os.path.exists(path):
            os.remove(path)


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


# Org identifier column, in the spellings the export datasets use. Every current
# dataset emits 'orgId'; 'org_id' is accepted so the unified findings datasets do
# not silently fall through if they are added later.
ORG_COLUMN_CANDIDATES = ("orgId", "org_id")


def _resolve_org_column(conn, file_path: str) -> str:
    """Return the org identifier column of a Parquet file.

    Raises:
        ValueError: If the file carries no recognised org column. An org-scoped
            load must fail here rather than proceed, because a load that cannot
            identify its tenant would delete the wrong rows or none at all.
    """
    rows = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").fetchall()  # nosec B608
    by_lowercase = {row[0].lower(): row[0] for row in rows}
    for candidate in ORG_COLUMN_CANDIDATES:
        if candidate.lower() in by_lowercase:
            return by_lowercase[candidate.lower()]
    raise ValueError(
        f"Parquet file '{file_path}' has no org identifier column "
        f"(looked for {', '.join(ORG_COLUMN_CANDIDATES)}). "
        f"Refusing an org-scoped load."
    )


def _distinct_org_ids(conn, file_path: str, org_column: str) -> List[Any]:
    """Return the distinct org identifiers present in a Parquet file."""
    rows = conn.execute(
        f"SELECT DISTINCT \"{org_column}\" FROM read_parquet('{file_path}')"  # nosec B608
        f' WHERE "{org_column}" IS NOT NULL'
    ).fetchall()
    return [row[0] for row in rows]


def _null_org_row_count(conn, file_path: str, org_column: str) -> int:
    """Count rows in a Parquet file that carry no org identifier."""
    result = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{file_path}')"  # nosec B608
        f' WHERE "{org_column}" IS NULL'
    ).fetchone()
    return result[0] if result else 0


class VulnerabilityDatabase:
    """
    Manages a DuckDB database for vulnerability data.

    Each operation opens a short-lived connection and releases it on return.
    Read operations use read-only connections (DuckDB allows unlimited concurrent
    readers), so multiple Claude processes can query simultaneously without lock
    conflicts. Write operations (load) use a read-write connection held only for
    the duration of the load.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the vulnerability database.

        Args:
            db_path: Path to persistent database file. Defaults to 'rapid7_bulk_export.db'.
        """
        self.db_path = db_path or "rapid7_bulk_export.db"
        if not os.path.exists(self.db_path):
            # Create the file and set permissions; connection is immediately released.
            conn = connect_with_retry(self.db_path)
            conn.close()
        os.chmod(self.db_path, 0o600)

    def has_data(self) -> bool:
        """Return True if at least one known table has been loaded."""
        placeholders = ", ".join("?" * len(KNOWN_TABLES))
        with duckdb_connection(self.db_path, read_only=True) as conn:
            result = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ({placeholders})",  # nosec B608
                KNOWN_TABLES,
            ).fetchone()
        return result is not None and result[0] > 0

    def load_parquet_files_by_prefix(
        self,
        prefix_file_map: Dict[str, List[str]],
        skip_prefixes: Set[str] = None,
        append: bool = False,
        org_scoped: bool = False,
    ) -> Dict[str, int]:
        """
        Load Parquet files into tables based on prefix routing.

        Routing rules (from PREFIX_TABLE_MAP):
          'asset'                    → assets table
          'asset_vulnerability'      → vulnerabilities table
          'asset_policy'             → policies table (source='agent')
          'asset_scan_policy'        → policies table (source='scan')
          'vulnerability_remediation'→ vulnerability_remediation table
          'asset_software'           → asset_software table

        Opens a short-lived read-write connection for the duration of the load,
        then releases it so concurrent readers can proceed unblocked.

        In snapshot mode (append=False), only the tables targeted by the incoming
        prefixes are dropped and recreated. All other tables are preserved. This
        allows loading vulnerability and policy exports independently without
        one overwriting the other.

        Args:
            prefix_file_map: Mapping of prefixes to lists of local Parquet file paths.
            skip_prefixes: Optional set of prefixes to skip (e.g., {'asset'} during
                policy-only loads to avoid duplicating asset data).
            append: When True, insert rows into existing tables rather than dropping
                and recreating them. Use for additive loads (e.g. remediation chunks).
                Default False preserves snapshot-replace behavior.
            org_scoped: When True, replace only the rows whose org identifier appears
                in the incoming files, leaving every other org in the table intact.
                Use for multi-org loads, where a per-org export must refresh that org
                without discarding the tenants loaded before it. Mutually exclusive
                with append. Does not compact the database file, because a fan-out
                across many orgs would otherwise compact once per org; call compact()
                after the batch instead.

        Raises:
            ValueError: If both append and org_scoped are True, or if an org-scoped
                load meets a file with no org identifier column.

        Returns:
            Dict mapping table names to the number of rows inserted by THIS
            call (a delta, not the table's cumulative total) — so appended
            windows report only their own rows.
        """
        if append and org_scoped:
            raise ValueError(
                "append and org_scoped are mutually exclusive: append never removes rows, "
                "org_scoped removes the incoming org's rows before inserting"
            )

        if skip_prefixes is None:
            skip_prefixes = set()

        # Accumulate row counts per table
        row_counts: Dict[str, int] = {}

        with duckdb_connection(self.db_path) as conn:
            # Determine which tables this load will write to, so we can drop
            # only those in snapshot mode (preserving unrelated tables).
            tables_to_replace: Set[str] = set()
            if not append and not org_scoped:
                for prefix in prefix_file_map:
                    if prefix in skip_prefixes:
                        continue
                    normalized = _normalize_prefix(prefix)
                    if normalized in skip_prefixes:
                        continue
                    mapping = PREFIX_TABLE_MAP.get(normalized)
                    if mapping is None:
                        continue
                    table_name = mapping[0] if isinstance(mapping, tuple) else mapping
                    tables_to_replace.add(table_name)

                # Drop only the targeted tables before recreating them
                for table_name in tables_to_replace:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")  # nosec B608

            # Tables we write to in this call (tracks CREATE vs INSERT decisions).
            tables_touched: Set[str] = set()

            # Files this call could not read. Tolerated in snapshot and append mode
            # (pre-existing behaviour), fatal in org-scoped mode.
            failed_files: List[str] = []

            # Pre-existing tables (after any drops above) for append-mode decisions.
            preexisting: Set[str] = {
                row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
            }

            # Org-scoped mode: remove only the incoming orgs' rows, so this load
            # refreshes that tenant and leaves every other tenant in place. Runs
            # before the baseline counts below so the returned figures stay
            # rows-inserted-by-this-call. Wrapped in a transaction with the inserts,
            # because a delete that commits without its insert would leave the org
            # silently absent from every query.
            if org_scoped:
                # Resolve every file's tenant BEFORE opening the transaction. A file
                # that cannot be read must abort the load having changed nothing,
                # rather than fail midway and rely on a rollback.
                deletions: List[Tuple[str, str, Optional[str], List[Any]]] = []
                for prefix, file_paths in prefix_file_map.items():
                    if prefix in skip_prefixes:
                        continue
                    normalized = _normalize_prefix(prefix)
                    if normalized in skip_prefixes:
                        continue
                    mapping = PREFIX_TABLE_MAP.get(normalized)
                    if mapping is None:
                        # Snapshot and append mode warn and skip. An org-scoped load must
                        # not: skipping a prefix drops that whole dataset for the org, and
                        # a quietly smaller report is the failure this mode exists to stop.
                        raise ValueError(
                            f"Org-scoped load aborted before any change: unknown prefix '{prefix}'. "
                            f"Known prefixes: {', '.join(sorted(PREFIX_TABLE_MAP))}"
                        )
                    if isinstance(mapping, tuple):
                        table_name, source_value = mapping
                    else:
                        table_name, source_value = mapping, None
                    for file_path in file_paths:
                        try:
                            org_column = _resolve_org_column(conn, file_path)
                            org_ids = _distinct_org_ids(conn, file_path, org_column)
                            null_org_rows = _null_org_row_count(conn, file_path, org_column)
                        except ValueError:
                            raise
                        except Exception as e:
                            raise ValueError(
                                f"Org-scoped load aborted before any change: could not read '{file_path}': {e}"
                            ) from e
                        if null_org_rows:
                            # A row with no org cannot be attributed to a tenant, so the
                            # delete never removes it and every refresh adds another copy.
                            # The policy datasets emit shared content rows with a NULL
                            # orgId on purpose, so they need their own decision before
                            # they can be loaded per org.
                            raise ValueError(
                                f"Org-scoped load aborted before any change: '{file_path}' has "
                                f"{null_org_rows} row(s) with a NULL '{org_column}'. Rows without an "
                                f"org cannot be refreshed per org and would duplicate on every load."
                            )
                        if org_ids and table_name in preexisting:
                            deletions.append((table_name, org_column, source_value, org_ids))

                conn.begin()
                try:
                    for table_name, org_column, source_value, org_ids in deletions:
                        placeholders = ", ".join("?" * len(org_ids))
                        sql = f'DELETE FROM {table_name} WHERE "{org_column}" IN ({placeholders})'  # nosec B608
                        params: List[Any] = list(org_ids)
                        if source_value is not None:
                            # Two prefixes share the policies table. Without narrowing by
                            # source, refreshing an org's agent policies would also delete
                            # its scan policies, which this call is not replacing.
                            sql += " AND source = ?"
                            params.append(source_value)
                        conn.execute(sql, params)
                except Exception:
                    conn.rollback()
                    raise

            # Row count of each pre-existing table BEFORE this call's inserts, so
            # the returned counts are rows-inserted-this-call (a delta), not the
            # table's cumulative total. Critical for append mode: without this,
            # N appended windows of R rows each would report R, 2R, 3R, ...
            counts_before: Dict[str, int] = {}
            for table_name in preexisting:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # nosec B608
                counts_before[table_name] = result[0] if result else 0

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
                        if source_value is not None:
                            select_expr = (
                                f"SELECT *, '{source_value}' AS source"
                                f" FROM read_parquet('{file_path}')"  # nosec B608
                            )
                        else:
                            select_expr = f"SELECT * FROM read_parquet('{file_path}')"  # nosec B608

                        if table_name not in tables_touched and table_name not in preexisting:
                            # Table doesn't exist — create it
                            conn.execute(f"CREATE TABLE {table_name} AS {select_expr}")  # nosec B608
                        else:
                            # Table exists (from earlier file in this call, or pre-existing in append mode)
                            conn.execute(f"INSERT INTO {table_name} {select_expr}")  # nosec B608
                        tables_touched.add(table_name)
                    except Exception as e:
                        print(
                            f"Warning: Failed to read Parquet file '{file_path}': {e}",
                            file=sys.stderr,
                        )
                        failed_files.append(file_path)
                        continue

            # An org-scoped load that lost a file would leave that org partially
            # represented, which reads as a real drop in every chart. Roll back the
            # whole refresh so the org keeps its previous data and the caller retries.
            if org_scoped:
                if failed_files:
                    conn.rollback()
                    raise ValueError(
                        "Org-scoped load rolled back: could not read "
                        f"{len(failed_files)} file(s): {', '.join(failed_files)}"
                    )
                conn.commit()

            # Report rows inserted BY THIS CALL: current total minus the
            # pre-call baseline (0 for tables created in this call).
            for table_name in tables_touched:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # nosec B608
                after = result[0] if result else 0
                row_counts[table_name] = after - counts_before.get(table_name, 0)

        # Reclaim disk space from dropped tables. DuckDB does not shrink the file
        # on DROP TABLE or VACUUM — the only way is to copy to a fresh database.
        if not append and tables_to_replace:
            self._compact()

        return row_counts

    def compact(self) -> None:
        """Reclaim disk space after org-scoped loads.

        Org-scoped loads skip compaction so a fan-out across many orgs does not
        compact the whole database once per org. Call this once after the batch.
        """
        self._compact()

    def _compact(self) -> None:
        """Compact the database file by copying all data to a fresh file.

        DuckDB's storage engine never releases pages from dropped tables, so
        repeated snapshot reloads cause unbounded file growth. This method
        creates a clean copy via COPY FROM DATABASE and atomically replaces
        the original, keeping the file size proportional to actual data.
        """
        compact_path = self.db_path + ".compact"
        try:
            with duckdb_connection(self.db_path) as conn:
                # DuckDB names the default catalog after the file stem, not "main"
                db_name = conn.execute("SELECT current_database()").fetchone()[0]
                conn.execute("CHECKPOINT")
                conn.execute(f"ATTACH '{compact_path}' AS compact_db")  # nosec B608
                conn.execute(f'COPY FROM DATABASE "{db_name}" TO compact_db')  # nosec B608
            # Atomic swap — replaces the bloated original with the compact copy
            os.replace(compact_path, self.db_path)
            os.chmod(self.db_path, 0o600)
        except Exception as e:
            # If compaction fails, the original DB is still intact — just log and continue
            print(f"Warning: Database compaction failed (non-fatal): {e}", file=sys.stderr)
            if os.path.exists(compact_path):
                os.remove(compact_path)

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dictionaries.

        Opens a short-lived read-only connection with external filesystem and
        network access disabled at the DuckDB engine level, so user SQL cannot
        reach read_parquet, read_csv, glob, or network resources.

        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries

        Returns:
            List of dictionaries, one per row

        Raises:
            ValueError: If the query fails
        """
        try:
            with duckdb_connection(self.db_path, read_only=True, disable_external_access=True) as conn:
                if params:
                    result = conn.execute(sql, params).fetchall()
                else:
                    result = conn.execute(sql).fetchall()

                description = conn.description
                if not description:
                    return []

                columns = [desc[0] for desc in description]
                return [dict(zip(columns, row)) for row in result]

        except Exception as e:
            raise ValueError(f"Query execution failed: {str(e)}") from e

    def get_schema(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get the schema of all existing tables.

        Queries information_schema.columns for each known table and returns
        only those that exist.

        Returns:
            Dictionary keyed by table name, each value is a list of
            dictionaries with column_name and data_type.
        """
        schemas: Dict[str, List[Dict[str, str]]] = {}

        with duckdb_connection(self.db_path, read_only=True) as conn:
            for table_name in KNOWN_TABLES:
                try:
                    result = conn.execute(
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
                    continue  # nosec B112

        return schemas

    def get_stats(self) -> Dict[str, Any]:
        """
        Get summary statistics for all existing tables.

        Opens a single short-lived read-only connection and runs all stat
        queries within it. Tables that don't exist are omitted.

        Returns:
            Dictionary keyed by table name with per-table statistics.
        """
        all_stats: Dict[str, Any] = {}

        with duckdb_connection(self.db_path, read_only=True) as conn:
            vuln_stats = self._get_vulnerabilities_stats(conn)
            if vuln_stats is not None:
                all_stats["vulnerabilities"] = vuln_stats

            assets_stats = self._get_assets_stats(conn)
            if assets_stats is not None:
                all_stats["assets"] = assets_stats

            policies_stats = self._get_policies_stats(conn)
            if policies_stats is not None:
                all_stats["policies"] = policies_stats

            remediation_stats = self._get_remediation_stats(conn)
            if remediation_stats is not None:
                all_stats["vulnerability_remediation"] = remediation_stats

            software_stats = self._get_asset_software_stats(conn)
            if software_stats is not None:
                all_stats["asset_software"] = software_stats

        return all_stats

    def _get_vulnerabilities_stats(self, conn) -> Optional[Dict[str, Any]]:
        """Gather statistics for the vulnerabilities table. Returns None if table doesn't exist."""
        try:
            result = conn.execute("SELECT COUNT(*) FROM vulnerabilities").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            counts = conn.execute("""
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

        try:
            severity_dist = conn.execute("""
                SELECT severity, COUNT(*) as count
                FROM vulnerabilities
                WHERE severity IS NOT NULL
                GROUP BY severity
                ORDER BY count DESC
            """).fetchall()
            stats["severity_distribution"] = {row[0]: row[1] for row in severity_dist}
        except Exception:
            pass

        try:
            cvss_stats = conn.execute("""
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

        try:
            exploit_stats = conn.execute("""
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

        try:
            cloud_dist = conn.execute("""
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

    def _get_assets_stats(self, conn) -> Optional[Dict[str, Any]]:
        """Gather statistics for the assets table. Returns None if table doesn't exist."""
        try:
            result = conn.execute("SELECT COUNT(*) FROM assets").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            counts = conn.execute("SELECT COUNT(DISTINCT assetId) FROM assets").fetchone()
            if counts:
                stats["unique_assets"] = counts[0]
        except Exception:
            pass

        try:
            os_dist = conn.execute("""
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

    def _get_policies_stats(self, conn) -> Optional[Dict[str, Any]]:
        """Gather statistics for the policies table. Returns None if table doesn't exist."""
        try:
            result = conn.execute("SELECT COUNT(*) FROM policies").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            status_dist = conn.execute("""
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
            source_dist = conn.execute("""
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

    def _get_remediation_stats(self, conn) -> Optional[Dict[str, Any]]:
        """Gather statistics for the vulnerability_remediation table. Returns None if table doesn't exist."""
        try:
            result = conn.execute("SELECT COUNT(*) FROM vulnerability_remediation").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            severity_dist = conn.execute("""
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

    def _get_asset_software_stats(self, conn) -> Optional[Dict[str, Any]]:
        """Gather statistics for the asset_software table. Returns None if table doesn't exist."""
        try:
            result = conn.execute("SELECT COUNT(*) FROM asset_software").fetchone()
        except Exception:
            return None

        stats: Dict[str, Any] = {}
        stats["total_rows"] = result[0] if result else 0

        try:
            counts = conn.execute("SELECT COUNT(DISTINCT assetId) FROM asset_software").fetchone()
            if counts:
                stats["unique_assets"] = counts[0]
        except Exception:
            pass

        return stats

    def close(self):
        """No-op — connections are short-lived and released per-operation."""

    def purge(self):
        """Purge all data by deleting the database file from disk.

        Removes the database file and any associated WAL file, then
        recreates the file so subsequent operations don't hit a missing path.
        """
        _delete_db_files(self.db_path)
        conn = connect_with_retry(self.db_path)
        conn.close()
        os.chmod(self.db_path, 0o600)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
