#!/usr/bin/env python3
"""
Check for updates to the official InsightIDR Investigations v2 OpenAPI spec
and refresh the local copy in rapid7-insightidr-skill/references/ if it has
changed.

This is a maintenance script (run via `make check-idr-spec`), not part of
the MCP server — it keeps the skill's bundled API reference in sync with
Rapid7's published spec so the reference doesn't silently go stale.

Author: rozumeyroman@gmail.com
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

import requests

SPEC_URL = "https://help.rapid7.com/insightidr/en-us/api/v2/insightidr-api-v2.json"
DEFAULT_CHECK_INTERVAL_DAYS = 7

SKILL_DIR = Path(__file__).resolve().parent.parent / "rapid7-insightidr-skill" / "references"
DEFAULT_SPEC_PATH = SKILL_DIR / "investigations-api-v2.json"
DEFAULT_META_PATH = SKILL_DIR / ".spec-meta.json"


def load_meta(meta_path: Path) -> Dict[str, Any]:
    """Load check metadata (last check time, last known hash). Empty dict if absent/corrupt."""
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_meta(meta_path: Path, meta: Dict[str, Any]) -> None:
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")


def needs_check(meta: Dict[str, Any], now: datetime, interval_days: int = DEFAULT_CHECK_INTERVAL_DAYS) -> bool:
    """True if it's been at least interval_days since the last check, or never checked."""
    last_checked = meta.get("last_checked")
    if not last_checked:
        return True
    try:
        last_checked_dt = datetime.fromisoformat(last_checked)
    except ValueError:
        return True
    return now - last_checked_dt >= timedelta(days=interval_days)


def canonical_hash(spec: Dict[str, Any]) -> str:
    """Stable content hash, independent of key order or incidental whitespace."""
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fetch_spec(url: str, timeout: int = 30) -> Dict[str, Any]:
    """Download and parse the remote OpenAPI spec.

    Raises:
        requests.RequestException: on network failure.
        requests.HTTPError: on non-2xx response.
        json.JSONDecodeError: if the response isn't valid JSON.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def diff_paths(old_spec: Optional[Dict[str, Any]], new_spec: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    """Return (added_paths, removed_paths) between two OpenAPI specs' `paths` sections."""
    old_paths = set((old_spec or {}).get("paths", {}).keys())
    new_paths = set(new_spec.get("paths", {}).keys())
    return new_paths - old_paths, old_paths - new_paths


def check_and_update(
    force: bool = False,
    url: str = SPEC_URL,
    spec_path: Path = DEFAULT_SPEC_PATH,
    meta_path: Path = DEFAULT_META_PATH,
    interval_days: int = DEFAULT_CHECK_INTERVAL_DAYS,
) -> int:
    """Run the check/update flow. Returns a process exit code.

    0 = no action needed or successfully up to date, 1 = fetch/parse error,
    2 = spec changed and the local copy was updated (informational, not a
    failure — signals to the caller that references/investigations-api-v2.md
    should be reviewed by hand, since it isn't auto-generated).
    """
    now = datetime.now(timezone.utc)
    meta = load_meta(meta_path)

    if not force and not needs_check(meta, now, interval_days):
        print(
            f"Checked {meta.get('last_checked')} — within the {interval_days}-day window, skipping. "
            "Use --force to override."
        )
        return 0

    print(f"Fetching {url} ...")
    try:
        new_spec = fetch_spec(url)
    except (requests.RequestException, ValueError) as e:
        print(f"✗ Failed to fetch or parse spec: {e}", file=sys.stderr)
        return 1

    new_hash = canonical_hash(new_spec)
    old_hash = meta.get("sha256")

    if old_hash == new_hash:
        meta["last_checked"] = now.isoformat()
        save_meta(meta_path, meta)
        print("No changes — spec is up to date.")
        return 0

    old_spec = None
    if spec_path.exists():
        try:
            old_spec = json.loads(spec_path.read_text())
        except (json.JSONDecodeError, OSError):
            old_spec = None

    added, removed = diff_paths(old_spec, new_spec)

    spec_path.write_text(json.dumps(new_spec, indent=2) + "\n")
    meta["last_checked"] = now.isoformat()
    meta["last_changed"] = now.isoformat()
    meta["sha256"] = new_hash
    meta["source_url"] = url
    save_meta(meta_path, meta)

    print(f"✓ Spec changed — downloaded and saved to {spec_path}")
    if added:
        print(f"  Added paths: {sorted(added)}")
    if removed:
        print(f"  Removed paths: {sorted(removed)}")
    if not added and not removed and old_hash is not None:
        print("  Path list unchanged — the change is inside existing operations/schemas (parameters, enums, etc.)")
    print("  Review references/investigations-api-v2.md by hand — it is a curated summary, not auto-generated.")
    return 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Check now, ignoring the weekly interval.")
    parser.add_argument("--url", default=SPEC_URL, help="Override the spec URL (for testing).")
    parser.add_argument("--spec-path", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--meta-path", type=Path, default=DEFAULT_META_PATH)
    parser.add_argument("--interval-days", type=int, default=DEFAULT_CHECK_INTERVAL_DAYS)
    args = parser.parse_args()

    return check_and_update(
        force=args.force,
        url=args.url,
        spec_path=args.spec_path,
        meta_path=args.meta_path,
        interval_days=args.interval_days,
    )


if __name__ == "__main__":
    sys.exit(main())
