#!/usr/bin/env python3
"""Foreground refresh CLI for the Rapid7 bulk export data.

This is the headless counterpart to the MCP tools: a scheduled job runs
``rapid7-refresh`` to create, poll, download and load the requested exports
synchronously in a single process, with no background threads that could die
mid-write. It exits non-zero if any window fails so the job can alert and retry.

The entrypoint is a thin wiring shell — argument parsing, config load, and
logging setup — over :mod:`src.orchestrator`, where the orchestration lives.
"""

import logging
import os
import sys
from pathlib import Path

import click

from .artifact_store import publish_artifact
from .config import load_config, redact_secret
from .orchestrator import VALID_REFRESH_TYPES, refresh_all

# Structured, level-tagged logs to stderr. stdout is reserved so the finished
# database path can be emitted cleanly for a downstream publish step.
logger = logging.getLogger("rapid7.refresh")


def _configure_logging(verbose: bool) -> None:
    """Send level-tagged, context-carrying log lines to stderr."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.propagate = False


def _default_data_dir() -> Path:
    """Resolve the data directory the same way the server does.

    Precedence: an explicit DATA_DIR, then the plugin host's PLUGIN_DATA, then
    ~/.rapid7_mcp — so a job configured like the server writes to the same place.
    """
    return Path(os.environ.get("DATA_DIR") or os.environ.get("PLUGIN_DATA") or "~/.rapid7_mcp").expanduser().resolve()


@click.command()
@click.option(
    "--type",
    "types",
    multiple=True,
    type=click.Choice(VALID_REFRESH_TYPES),
    help="Export type to refresh; repeat for several. Defaults to all snapshot types plus remediation.",
)
@click.option(
    "--db-path",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Database file to build into. Defaults to rapid7_bulk_export.db under the data directory.",
)
@click.option(
    "--start-date",
    default="",
    help="Remediation range start (YYYY-MM-DD). Defaults to 30 days ago.",
)
@click.option(
    "--end-date",
    default="",
    help="Remediation range end (YYYY-MM-DD). Defaults to today.",
)
@click.option("-v", "--verbose", is_flag=True, help="Emit DEBUG-level logs.")
def main(types, db_path, start_date, end_date, verbose):
    """Refresh Rapid7 export data into a local database, synchronously.

    Exits 0 when every requested window loaded, and non-zero (naming the failed
    windows) when any did not — so a scheduled job can retry and alert.
    """
    _configure_logging(verbose)

    selected = list(types) or list(VALID_REFRESH_TYPES)
    data_dir = _default_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    target = str(db_path) if db_path else str(data_dir / "rapid7_bulk_export.db")

    try:
        config = load_config()
    except ValueError as e:
        # A missing/invalid API key is an operator error, not a window failure.
        logger.error("configuration error: %s", e)
        raise SystemExit(2) from e

    logger.info("refresh starting: types=%s db_path=%s", selected, target)

    result = refresh_all(
        selected,
        config=config,
        db_path=target,
        data_dir=data_dir,
        start_date=start_date,
        end_date=end_date,
    )

    for window in result.windows:
        if window.ok:
            logger.info("window ok: %s (%s)", window.kind, window.detail)
        else:
            # A window error is str(e) from deep in the export/download path;
            # scrub the credential before it reaches the log, so a leaking
            # exception can never write the key to a refresh job's output.
            logger.error("window failed: %s: %s", window.kind, redact_secret(window.error))

    if not result.ok:
        failed = ", ".join(w.kind for w in result.failed)
        logger.error("refresh incomplete: %d window(s) failed: %s", len(result.failed), failed)
        raise SystemExit(1)

    logger.info("refresh complete: %d row(s) across %d window(s)", result.total_rows, len(result.windows))

    # Publish the finished database to Blob as a versioned artifact when hosted
    # storage is configured. No-op in local mode, so the file simply stays on
    # disk. Only a fully-loaded database is published, because a failed window
    # exits non-zero above before reaching here.
    # Logged BEFORE the call: publishing uploads the whole database over HTTPS and
    # is the slowest single step, so without this a stall or a network/permission
    # failure during upload is indistinguishable from the job having finished.
    logger.info("publishing artifact from %s", target)
    version = publish_artifact(target)
    if version is not None:
        logger.info("published artifact version: %s", version)
    else:
        logger.info("no artifact store configured; database left on local disk")

    # The finished artifact path on stdout, for a downstream publish step.
    click.echo(target)


if __name__ == "__main__":
    main()
