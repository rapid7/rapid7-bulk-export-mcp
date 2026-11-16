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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

from .config import USER_AGENT
from .db_utils import duckdb_connection

KEV_FEED_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
KEV_TABLE = "cisa_kev"
KEV_METADATA_TABLE = "_kev_catalogue_metadata"
MAX_BYTES = 10 * 1024 * 1024
MAX_ENTRIES = 100_000
TIMEOUT_SECONDS = 30

_HOST = urlparse(KEV_FEED_URL).hostname
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
        entries.setdefault(entry.cve_id, entry)
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


def _entry(item: Any) -> Optional[KevEntry]:
    if not isinstance(item, dict):
        return None
    cve_id, date_added = item.get("cveID"), item.get("dateAdded")
    ransomware = item.get("knownRansomwareCampaignUse")
    if not isinstance(cve_id, str) or not _CVE_ID.fullmatch(cve_id):
        return None
    if not isinstance(date_added, str) or not _DATE.fullmatch(date_added):
        return None
    if ransomware is not None and (not isinstance(ransomware, str) or len(ransomware) > 32):
        return None
    return KevEntry(cve_id=cve_id, date_added=date_added, known_ransomware_use=ransomware)


@dataclass(frozen=True)
class Download:
    catalogue: KevCatalogue
    etag: Optional[str]
    last_modified: Optional[str]


def fetch_catalogue(
    session: Optional[requests.Session] = None, etag: Optional[str] = None, last_modified: Optional[str] = None
) -> Optional[Download]:
    """Download and check the catalogue from CISA's feed, unless CISA says it has not changed.

    Args:
        etag: The stored copy's ETag, sent as If-None-Match.
        last_modified: The stored copy's Last-Modified, sent as If-Modified-Since.

    Returns:
        The checked catalogue and its validators, or None when CISA answers 304 Not Modified.

    Raises:
        KevCatalogueError: If the download fails, leaves CISA's host, is too large, or the
            document fails a check.
    """
    http = session or requests
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    try:
        response = http.get(KEV_FEED_URL, headers=headers, timeout=TIMEOUT_SECONDS, stream=True)
        if urlparse(response.url).scheme != "https" or urlparse(response.url).hostname != _HOST:
            raise KevCatalogueError("The catalogue download left CISA's site.")
        if response.status_code == 304:
            return None
        response.raise_for_status()
        raw = bytearray()
        for chunk in response.iter_content(chunk_size=65536):
            raw.extend(chunk)
            if len(raw) > MAX_BYTES:
                raise KevCatalogueError(f"The catalogue is larger than {MAX_BYTES} bytes.")
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


def refresh_catalogue(db_path: str, session: Optional[requests.Session] = None) -> str:
    """Bring the stored catalogue up to date, and leave it alone when it already is.

    Returns:
        "unchanged" when CISA reports no change or sends the stored version again, else "updated".

    Raises:
        KevCatalogueError: If the catalogue could not be fetched or failed a check. The stored
            copy, if any, is left as it was.
    """
    stored = stored_catalogue(db_path)
    download = fetch_catalogue(
        session,
        etag=stored.get("etag") if stored else None,
        last_modified=stored.get("last_modified") if stored else None,
    )
    if download is None or (stored and stored.get("version") == download.catalogue.version):
        return "unchanged"
    store_catalogue(db_path, download.catalogue, etag=download.etag, last_modified=download.last_modified)
    return "updated"


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
