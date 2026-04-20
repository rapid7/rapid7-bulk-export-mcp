#!/usr/bin/env python3
"""MCPB entry point for the Rapid7 Bulk Export MCP server.

This thin wrapper avoids relative import issues when the host app
runs `uv run run_server.py` directly.
"""

from src.mcp_server import main

if __name__ == "__main__":
    main()
