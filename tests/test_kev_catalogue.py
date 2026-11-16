"""Tests for fetching, checking, and storing the CISA KEV catalogue. No test reaches the network."""

import json
from datetime import datetime

import pytest
import requests

from src import kev_catalogue as kev

FEED = kev.KEV_FEED_URL


def _document(version="2026.11.16", entries=None, **overrides):
    document = {
        "title": "CISA Catalog of Known Exploited Vulnerabilities",
        "catalogVersion": version,
        "dateReleased": "2026-11-16T15:00:00.0000Z",
        "count": 2,
        "vulnerabilities": entries
        if entries is not None
        else [
            {"cveID": "CVE-2021-44228", "dateAdded": "2021-12-10", "knownRansomwareCampaignUse": "Known"},
            {"cveID": "CVE-2023-4966", "dateAdded": "2023-10-18", "knownRansomwareCampaignUse": "Unknown"},
        ],
    }
    document.update(overrides)
    return json.dumps(document).encode("utf-8")


class _Response:
    def __init__(self, body=b"", status=200, url=FEED, headers=None):
        self.status_code, self.url, self._body = status, url, body
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def iter_content(self, chunk_size):
        for start in range(0, len(self._body), chunk_size):
            yield self._body[start : start + chunk_size]


class _Session:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, headers, timeout, stream):
        self.calls.append({"url": url, "headers": headers, "timeout": timeout, "stream": stream})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_a_valid_catalogue_is_parsed_with_its_version_and_hash():
    raw = _document()

    catalogue = kev.parse_catalogue(raw)

    assert catalogue.version == "2026.11.16"
    assert [e.cve_id for e in catalogue.entries] == ["CVE-2021-44228", "CVE-2023-4966"]
    assert catalogue.entries[0].known_ransomware_use == "Known"
    assert len(catalogue.sha256) == 64


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"not json", id="not JSON"),
        pytest.param(b"[]", id="not an object"),
        pytest.param(_document(version="1; DROP TABLE x"), id="bad version"),
        pytest.param(_document(dateReleased="yesterday"), id="bad release date"),
        pytest.param(_document(entries=[]), id="empty"),
        pytest.param(_document(entries=[{"cveID": "CVE-2021-44228<b>", "dateAdded": "2021-12-10"}]), id="bad CVE"),
        pytest.param(_document(entries=[{"cveID": "CVE-2021-44228", "dateAdded": "10/12/2021"}]), id="bad date"),
        pytest.param(
            _document(
                entries=[{"cveID": "CVE-2021-44228", "dateAdded": "2021-12-10", "knownRansomwareCampaignUse": 1}]
            ),
            id="bad ransomware flag",
        ),
        pytest.param(b" " * (kev.MAX_BYTES + 1), id="too large"),
    ],
)
def test_anything_but_a_well_formed_catalogue_is_refused(raw):
    with pytest.raises(kev.KevCatalogueError):
        kev.parse_catalogue(raw)


def test_a_fetch_sends_the_stored_validators_and_streams_from_cisa():
    session = _Session(_Response(_document(), headers={"ETag": '"abc"', "Last-Modified": "Mon, 16 Nov 2026"}))

    download = kev.fetch_catalogue(session, etag='"old"', last_modified="Sun, 15 Nov 2026")

    [call] = session.calls
    assert call["url"] == FEED and call["stream"] is True and call["timeout"] == kev.TIMEOUT_SECONDS
    assert call["headers"]["If-None-Match"] == '"old"'
    assert call["headers"]["If-Modified-Since"] == "Sun, 15 Nov 2026"
    assert (download.etag, download.last_modified) == ('"abc"', "Mon, 16 Nov 2026")


def test_not_modified_downloads_nothing():
    assert kev.fetch_catalogue(_Session(_Response(status=304)), etag='"abc"') is None


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (_Response(_document(), url="https://evil.example/kev.json"), "left CISA's site"),
        (_Response(_document(), url="http://www.cisa.gov/sites/default/files/feeds/x.json"), "left CISA's site"),
        (_Response(b"x" * (kev.MAX_BYTES + 1)), "larger than"),
        (_Response(status=500), "could not be downloaded"),
        (requests.ConnectionError("down"), "could not be downloaded"),
    ],
)
def test_a_bad_download_is_refused(response, message):
    with pytest.raises(kev.KevCatalogueError, match=message):
        kev.fetch_catalogue(_Session(response))


def test_an_odd_validator_header_is_not_kept():
    download = kev.fetch_catalogue(
        _Session(_Response(_document(), headers={"ETag": "a\nb", "Last-Modified": "x" * 300}))
    )

    assert (download.etag, download.last_modified) == (None, None)


def _db(tmp_path):
    return str(tmp_path / "kev.db")


def test_refresh_stores_the_catalogue_then_leaves_an_unchanged_copy_alone(tmp_path):
    db = _db(tmp_path)

    assert kev.refresh_catalogue(db, _Session(_Response(_document(), headers={"ETag": '"v1"'}))) == "updated"
    first = kev.stored_catalogue(db)
    same_version = _Session(_Response(_document()))
    not_modified = _Session(_Response(status=304))

    assert kev.refresh_catalogue(db, same_version) == "unchanged"
    assert kev.refresh_catalogue(db, not_modified) == "unchanged"
    assert kev.stored_catalogue(db) == first, "nothing is rewritten when the copy is current"
    assert not_modified.calls[0]["headers"]["If-None-Match"] == '"v1"'
    assert first["version"] == "2026.11.16" and first["entries"] == 2


def test_refresh_replaces_an_older_copy(tmp_path):
    db = _db(tmp_path)
    kev.store_catalogue(db, kev.parse_catalogue(_document(version="2026.11.15")), fetched_at=datetime(2026, 11, 15))

    newer = [{"cveID": "CVE-2024-3400", "dateAdded": "2024-04-12", "knownRansomwareCampaignUse": None}]

    assert kev.refresh_catalogue(db, _Session(_Response(_document(entries=newer)))) == "updated"
    assert kev.stored_catalogue(db)["version"] == "2026.11.16"
    assert kev.stored_catalogue(db)["entries"] == 1


def test_a_failed_refresh_keeps_the_stored_copy(tmp_path):
    db = _db(tmp_path)
    kev.refresh_catalogue(db, _Session(_Response(_document())))
    before = kev.stored_catalogue(db)

    with pytest.raises(kev.KevCatalogueError):
        kev.refresh_catalogue(db, _Session(_Response(_document(version="2026.11.17", entries=[{"cveID": "bad"}]))))

    assert kev.stored_catalogue(db) == before


def test_no_stored_catalogue_reads_as_none(tmp_path):
    assert kev.stored_catalogue(_db(tmp_path)) is None
