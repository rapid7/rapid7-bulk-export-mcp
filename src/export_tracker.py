"""
Export Tracker Module

This module manages a separate DuckDB database to track Rapid7 export metadata,
allowing reuse of exports from the same day instead of creating new ones.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import duckdb


class ExportTracker:
    """Tracks export metadata in a separate DuckDB database."""

    def __init__(self, db_path: str = "rapid7_bulk_export_tracking.db"):
        """
        Initialize the export tracker.

        Args:
            db_path: Path to the DuckDB database file for tracking exports
        """
        self.db_path = db_path
        self.conn = None
        self._initialize_db()

    def _initialize_db(self):
        """Initialize the export tracking database and create schema."""
        self.conn = duckdb.connect(self.db_path)

        # Create exports table to track export metadata
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS exports (
                export_id VARCHAR PRIMARY KEY,
                export_date DATE NOT NULL,
                created_at TIMESTAMP NOT NULL,
                status VARCHAR NOT NULL,
                file_count INTEGER,
                row_count INTEGER,
                parquet_urls VARCHAR[],
                local_files VARCHAR[]
            )
        """)

        # Migrate schema: add export_type column for existing databases
        try:
            self.conn.execute("""
                ALTER TABLE exports ADD COLUMN export_type VARCHAR DEFAULT 'vulnerability'
            """)
        except Exception:
            # Column already exists, ignore
            pass  # nosec B110

        # Create index on export_date and export_type for fast lookups
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_export_date_type
            ON exports(export_date, export_type)
        """)

    def get_today_export(self, export_type: str = "vulnerability") -> Optional[Dict[str, Any]]:
        """
        Get the most recent completed export from today.

        Args:
            export_type: Type of export to filter by (default: 'vulnerability')

        Returns:
            Dictionary with export metadata if found, None otherwise
        """
        today = date.today()

        result = self.conn.execute(
            """
            SELECT
                export_id,
                export_date,
                created_at,
                status,
                file_count,
                row_count,
                parquet_urls,
                local_files,
                export_type
            FROM exports
            WHERE export_date = ?
              AND status = 'COMPLETE'
              AND export_type = ?
            ORDER BY created_at DESC
            LIMIT 1
        """,
            [today, export_type],
        ).fetchone()

        if result:
            return {
                "export_id": result[0],
                "export_date": result[1],
                "created_at": result[2],
                "status": result[3],
                "file_count": result[4],
                "row_count": result[5],
                "parquet_urls": result[6],
                "local_files": result[7],
                "export_type": result[8],
            }

        return None

    def save_export(
        self,
        export_id: str,
        status: str,
        parquet_urls: List[str],
        local_files: Optional[List[str]] = None,
        row_count: Optional[int] = None,
        export_type: str = "vulnerability",
    ):
        """
        Save or update export metadata.

        Args:
            export_id: The Rapid7 export ID
            status: Export status (COMPLETE, FAILED, etc.)
            parquet_urls: List of Parquet file URLs
            local_files: List of local file paths (optional)
            row_count: Number of rows loaded (optional)
            export_type: Type of export (default: 'vulnerability')
        """
        today = date.today()
        now = datetime.now()

        self.conn.execute(
            """
            INSERT INTO exports (
                export_id,
                export_date,
                created_at,
                status,
                file_count,
                row_count,
                parquet_urls,
                local_files,
                export_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (export_id) DO UPDATE SET
                status = EXCLUDED.status,
                file_count = EXCLUDED.file_count,
                row_count = EXCLUDED.row_count,
                parquet_urls = EXCLUDED.parquet_urls,
                local_files = EXCLUDED.local_files,
                export_type = EXCLUDED.export_type
        """,
            [
                export_id,
                today,
                now,
                status,
                len(parquet_urls) if parquet_urls else 0,
                row_count,
                parquet_urls,
                local_files,
                export_type,
            ],
        )

    def get_export_by_id(self, export_id: str) -> Optional[Dict[str, Any]]:
        """
        Get export metadata by export ID.

        Args:
            export_id: The Rapid7 export ID

        Returns:
            Dictionary with export metadata if found, None otherwise
        """
        result = self.conn.execute(
            """
            SELECT
                export_id,
                export_date,
                created_at,
                status,
                file_count,
                row_count,
                parquet_urls,
                local_files
            FROM exports
            WHERE export_id = ?
        """,
            [export_id],
        ).fetchone()

        if result:
            return {
                "export_id": result[0],
                "export_date": result[1],
                "created_at": result[2],
                "status": result[3],
                "file_count": result[4],
                "row_count": result[5],
                "parquet_urls": result[6],
                "local_files": result[7],
            }

        return None

    def list_exports(self, limit: int = 10, export_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List recent exports.

        Args:
            limit: Maximum number of exports to return
            export_type: Optional export type to filter by

        Returns:
            List of export metadata dictionaries
        """
        if export_type is not None:
            results = self.conn.execute(
                """
                SELECT
                    export_id,
                    export_date,
                    created_at,
                    status,
                    file_count,
                    row_count,
                    export_type
                FROM exports
                WHERE export_type = ?
                ORDER BY created_at DESC
                LIMIT ?
            """,
                [export_type, limit],
            ).fetchall()
        else:
            results = self.conn.execute(
                """
                SELECT
                    export_id,
                    export_date,
                    created_at,
                    status,
                    file_count,
                    row_count,
                    export_type
                FROM exports
                ORDER BY created_at DESC
                LIMIT ?
            """,
                [limit],
            ).fetchall()

        return [
            {
                "export_id": row[0],
                "export_date": row[1],
                "created_at": row[2],
                "status": row[3],
                "file_count": row[4],
                "row_count": row[5],
                "export_type": row[6],
            }
            for row in results
        ]

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
