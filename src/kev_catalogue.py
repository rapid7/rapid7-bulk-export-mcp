"""Fetch, check, and store the CISA Known Exploited Vulnerabilities (KEV) catalogue.

No bulk export carries a KEV flag, so the multi-org report matches each finding's own CVE IDs
against CISA's public catalogue. This is the fast, interim route: when an export starts to
carry KEV, the report should use that and this module can go.

The catalogue changes most days, so it cannot be pinned to one hash. It is fetched only from
CISA's own feed over verified HTTPS, checked strictly before anything is stored, and stored with
its version, release date, and SHA-256 so the report can say exactly which copy it used.
"""

import hashlib
import json
import os
import re
import time
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, ContextManager, Dict, Optional, Tuple

import requests

from .config import USER_AGENT
from .db_utils import duckdb_connection

KEV_FEED_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
KEV_TABLE = "cisa_kev"
KEV_METADATA_TABLE = "_kev_catalogue_metadata"
MAX_BYTES = 10 * 1024 * 1024
MAX_ENTRIES = 100_000
TIMEOUT_SECONDS = (10, 20)
"""Connect and per-read timeouts. DEADLINE_SECONDS bounds the whole download."""
DEADLINE_SECONDS = 60
_RANSOMWARE_VALUES = {"Known", "Unknown"}

_CVE_ID = re.compile(r"CVE-\d{4}-\d{4,7}")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_VERSION = re.compile(r"[0-9A-Za-z.\-]{1,32}")


class KevCatalogueError(ValueError):
    """The catalogue could not be fetched, or failed a check, so nothing was stored."""


@dataclass(frozen=True)
class KevEntry:
    cve_id: str
    date_added: str
    known_ransomware_use: Optional[str]


@dataclass(frozen=True)
class KevCatalogue:
    version: str
    released: str
    sha256: str
    source: str
    entries: Tuple[KevEntry, ...]


def parse_catalogue(raw: bytes, source: str = KEV_FEED_URL) -> KevCatalogue:
    """Check a raw catalogue document and return its entries.

    Raises:
        KevCatalogueError: If the document is too large, is not the catalogue's JSON shape,
            or holds any entry that is not a well-formed CVE ID with an ISO date.
    """
    if len(raw) > MAX_BYTES:
        raise KevCatalogueError(f"The catalogue is larger than {MAX_BYTES} bytes.")
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise KevCatalogueError("The catalogue is not valid JSON.") from e
    if not isinstance(document, dict):
        raise KevCatalogueError("The catalogue is not a JSON object.")

    version = document.get("catalogVersion")
    if not isinstance(version, str) or not _VERSION.fullmatch(version):
        raise KevCatalogueError("The catalogue has no valid catalogVersion.")
    released = document.get("dateReleased")
    if not isinstance(released, str) or not _parses_as_time(released):
        raise KevCatalogueError("The catalogue has no valid dateReleased.")
    items = document.get("vulnerabilities")
    if not isinstance(items, list) or not 0 < len(items) <= MAX_ENTRIES:
        raise KevCatalogueError("The catalogue has no vulnerabilities list of a plausible size.")

    entries: Dict[str, KevEntry] = {}
    for index, item in enumerate(items):
        entry = _entry(item)
        if entry is None:
            raise KevCatalogueError(f"Catalogue entry {index} is not a valid KEV entry.")
        if entries.setdefault(entry.cve_id, entry) != entry:
            raise KevCatalogueError(f"Catalogue lists {entry.cve_id} twice with different details.")
    return KevCatalogue(
        version=version,
        released=released,
        sha256=hashlib.sha256(raw).hexdigest(),
        source=source,
        entries=tuple(sorted(entries.values(), key=lambda e: e.cve_id)),
    )


def _parses_as_time(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _is_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _entry(item: Any) -> Optional[KevEntry]:
    if not isinstance(item, dict):
        return None
    cve_id, date_added = item.get("cveID"), item.get("dateAdded")
    ransomware = item.get("knownRansomwareCampaignUse")
    if not isinstance(cve_id, str) or not _CVE_ID.fullmatch(cve_id):
        return None
    if not isinstance(date_added, str) or not _DATE.fullmatch(date_added) or not _is_date(date_added):
        return None
    if ransomware is not None and ransomware not in _RANSOMWARE_VALUES:
        return None
    return KevEntry(cve_id=cve_id, date_added=date_added, known_ransomware_use=ransomware)


@dataclass(frozen=True)
class Download:
    catalogue: KevCatalogue
    etag: Optional[str]
    last_modified: Optional[str]


def fetch_catalogue(
    session: Optional[requests.Session] = None,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
    clock: Callable[[], float] = time.monotonic,
) -> Optional[Download]:
    """Download and check the catalogue from CISA's feed, unless CISA says it has not changed.

    Args:
        etag: The stored copy's ETag, sent as If-None-Match.
        last_modified: The stored copy's Last-Modified, sent as If-Modified-Since.

    Returns:
        The checked catalogue and its validators, or None when CISA answers 304 Not Modified.

    Raises:
        KevCatalogueError: If the download fails, redirects, is too large, runs past
            DEADLINE_SECONDS, or the document fails a check.
    """
    http = session or requests
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    try:
        deadline = clock() + DEADLINE_SECONDS
        response = http.get(KEV_FEED_URL, headers=headers, timeout=TIMEOUT_SECONDS, stream=True, allow_redirects=False)
        if response.status_code == 304:
            return None
        if 300 <= response.status_code < 400:
            raise KevCatalogueError("CISA's feed answered with a redirect, which is not followed.")
        response.raise_for_status()
        raw = bytearray()
        for chunk in response.iter_content(chunk_size=65536):
            raw.extend(chunk)
            if len(raw) > MAX_BYTES:
                raise KevCatalogueError(f"The catalogue is larger than {MAX_BYTES} bytes.")
            if clock() > deadline:
                raise KevCatalogueError(f"The catalogue took longer than {DEADLINE_SECONDS} seconds to download.")
    except requests.RequestException as e:
        raise KevCatalogueError(f"The catalogue could not be downloaded: {type(e).__name__}.") from e
    return Download(
        catalogue=parse_catalogue(bytes(raw)),
        etag=_header(response, "ETag"),
        last_modified=_header(response, "Last-Modified"),
    )


def _header(response: Any, name: str) -> Optional[str]:
    """A short, printable response header, or None, so nothing odd is stored or sent back."""
    value = response.headers.get(name)
    if not isinstance(value, str) or len(value) > 200 or not value.isprintable():
        return None
    return value


def refresh_catalogue(
    db_path: str, session: Optional[requests.Session] = None, write_lock: Optional[ContextManager[Any]] = None
) -> str:
    """Bring the stored catalogue up to date, and leave it alone when it already is.

    Every read and write of the database takes write_lock, because DuckDB refuses a second
    connection to one file with a different configuration. The download runs outside it, so
    a slow fetch never blocks queries or another organisation's load.

    Returns:
        "unchanged" when CISA reports no change or sends the stored version again, else "updated".

    Raises:
        KevCatalogueError: If the catalogue could not be fetched or failed a check. The stored
            copy, if any, is left as it was.
    """
    lock = write_lock if write_lock is not None else nullcontext()
    with lock:
        stored = stored_catalogue(db_path)
    download = fetch_catalogue(
        session,
        etag=stored.get("etag") if stored else None,
        last_modified=stored.get("last_modified") if stored else None,
    )
    if download is None:
        return "unchanged"
    with lock:
        if stored and stored.get("version") == download.catalogue.version:
            _update_validators(db_path, download.etag, download.last_modified)
            return "unchanged"
        store_catalogue(db_path, download.catalogue, etag=download.etag, last_modified=download.last_modified)
    return "updated"


def _update_validators(db_path: str, etag: Optional[str], last_modified: Optional[str]) -> None:
    """Keep the validators CISA sent, so the next refresh can be answered with 304. Missing ones are kept."""
    with duckdb_connection(db_path) as conn:
        conn.execute(
            f"UPDATE {KEV_METADATA_TABLE} SET etag = COALESCE(?, etag), "  # nosec B608
            "last_modified = COALESCE(?, last_modified)",
            [etag, last_modified],
        )


def store_catalogue(
    db_path: str,
    catalogue: KevCatalogue,
    fetched_at: Optional[datetime] = None,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
) -> None:
    """Replace the stored catalogue and its metadata in one transaction."""
    fetched = (fetched_at or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(tzinfo=None)
    rows = [(e.cve_id, e.date_added, e.known_ransomware_use) for e in catalogue.entries]
    with duckdb_connection(db_path) as conn:
        conn.begin()
        try:
            conn.execute(
                f"CREATE OR REPLACE TABLE {KEV_TABLE} ("  # nosec B608
                '"cveID" VARCHAR PRIMARY KEY, "dateAdded" DATE, "knownRansomwareCampaignUse" VARCHAR)'
            )
            conn.executemany(f"INSERT INTO {KEV_TABLE} VALUES (?, ?, ?)", rows)  # nosec B608
            conn.execute(
                f"CREATE OR REPLACE TABLE {KEV_METADATA_TABLE} ("  # nosec B608
                "catalog_version VARCHAR, date_released VARCHAR, sha256 VARCHAR, source VARCHAR, "
                "entries INTEGER, fetched_at TIMESTAMP, etag VARCHAR, last_modified VARCHAR)"
            )
            conn.execute(
                f"INSERT INTO {KEV_METADATA_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",  # nosec B608
                [
                    catalogue.version,
                    catalogue.released,
                    catalogue.sha256,
                    catalogue.source,
                    len(rows),
                    fetched,
                    etag,
                    last_modified,
                ],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def stored_catalogue(db_path: str) -> Optional[Dict[str, Any]]:
    """The stored catalogue's metadata, or None when no catalogue has been stored."""
    if not os.path.exists(db_path):
        return None
    with duckdb_connection(db_path, read_only=True) as conn:
        present = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [KEV_METADATA_TABLE]
        ).fetchone()
        if not present or not present[0]:
            return None
        row = conn.execute(
            "SELECT catalog_version, date_released, sha256, source, entries, fetched_at, etag, last_modified "
            f"FROM {KEV_METADATA_TABLE}"  # nosec B608
        ).fetchone()
    if row is None:
        return None
    keys = ("version", "released", "sha256", "source", "entries", "fetched_at", "etag", "last_modified")
    return dict(zip(keys, row))
