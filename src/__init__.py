"""
Rapid7 Vulnerability Export Package

This package provides a command-line tool for exporting vulnerability data from
Rapid7 InsightVM using the Bulk Export API. It handles the complete workflow of
creating exports, polling for completion, downloading Parquet files, filtering
data, and writing results to CSV format.

Modules:
    config: Configuration loading and validation from environment variables
    graphql_client: GraphQL communication with the Rapid7 API
    export_manager: Export lifecycle management (create, poll, retrieve URLs)
    download: Parquet file downloading from the API
    data_processing: Data filtering, transformation, and CSV writing
    cli: Command-line interface and workflow orchestration

Usage:
    This package is typically used via the command-line interface:

    $ python rapid7_export.py --output vulnerabilities.csv

    See the CLI module documentation for more details on available options.
"""

__version__ = "1.0.0"
__author__ = "Rapid7 Vulnerability Export Team"
