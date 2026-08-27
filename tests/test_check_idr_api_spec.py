"""
Unit tests for scripts/check_idr_api_spec.py.

Covers the weekly gate logic, hash comparison, path diffing, and the
end-to-end check_and_update flow with a mocked network response.

Author: rozumeyroman@gmail.com
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import responses

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from check_idr_api_spec import (  # noqa: E402
    canonical_hash,
    check_and_update,
    diff_paths,
    load_meta,
    needs_check,
    save_meta,
)

MINIMAL_SPEC_V1 = {"paths": {"/idr/v2/investigations": {}}}
MINIMAL_SPEC_V2 = {"paths": {"/idr/v2/investigations": {}, "/idr/v2/investigations/{id}": {}}}
TEST_URL = "https://example.com/spec.json"


class TestNeedsCheck:
    def test_no_meta_needs_check(self):
        assert needs_check({}, datetime.now(timezone.utc)) is True

    def test_recent_check_does_not_need_check(self):
        now = datetime.now(timezone.utc)
        meta = {"last_checked": (now - timedelta(days=1)).isoformat()}
        assert needs_check(meta, now) is False

    def test_check_exactly_at_interval_needs_check(self):
        now = datetime.now(timezone.utc)
        meta = {"last_checked": (now - timedelta(days=7)).isoformat()}
        assert needs_check(meta, now, interval_days=7) is True

    def test_stale_check_needs_check(self):
        now = datetime.now(timezone.utc)
        meta = {"last_checked": (now - timedelta(days=10)).isoformat()}
        assert needs_check(meta, now, interval_days=7) is True

    def test_corrupt_timestamp_needs_check(self):
        assert needs_check({"last_checked": "not-a-date"}, datetime.now(timezone.utc)) is True


class TestCanonicalHash:
    def test_same_content_different_key_order_same_hash(self):
        a = {"b": 2, "a": 1}
        b = {"a": 1, "b": 2}
        assert canonical_hash(a) == canonical_hash(b)

    def test_different_content_different_hash(self):
        assert canonical_hash(MINIMAL_SPEC_V1) != canonical_hash(MINIMAL_SPEC_V2)


class TestDiffPaths:
    def test_added_and_removed_paths(self):
        old = {"paths": {"/a": {}, "/b": {}}}
        new = {"paths": {"/b": {}, "/c": {}}}
        added, removed = diff_paths(old, new)
        assert added == {"/c"}
        assert removed == {"/a"}

    def test_none_old_spec_treats_everything_as_added(self):
        added, removed = diff_paths(None, MINIMAL_SPEC_V1)
        assert added == {"/idr/v2/investigations"}
        assert removed == set()


class TestMetaRoundTrip:
    def test_save_and_load(self, tmp_path):
        meta_path = tmp_path / "meta.json"
        save_meta(meta_path, {"sha256": "abc", "last_checked": "2026-01-01T00:00:00+00:00"})
        loaded = load_meta(meta_path)
        assert loaded["sha256"] == "abc"

    def test_missing_file_returns_empty_dict(self, tmp_path):
        assert load_meta(tmp_path / "nope.json") == {}

    def test_corrupt_file_returns_empty_dict(self, tmp_path):
        meta_path = tmp_path / "meta.json"
        meta_path.write_text("{not valid json")
        assert load_meta(meta_path) == {}


class TestCheckAndUpdate:
    @responses.activate
    def test_skips_when_within_interval_and_not_forced(self, tmp_path):
        spec_path = tmp_path / "spec.json"
        meta_path = tmp_path / "meta.json"
        now = datetime.now(timezone.utc)
        save_meta(meta_path, {"last_checked": now.isoformat(), "sha256": "whatever"})

        exit_code = check_and_update(force=False, url=TEST_URL, spec_path=spec_path, meta_path=meta_path)

        assert exit_code == 0
        assert len(responses.calls) == 0  # no network call made

    @responses.activate
    def test_no_change_updates_last_checked_only(self, tmp_path):
        spec_path = tmp_path / "spec.json"
        meta_path = tmp_path / "meta.json"
        spec_path.write_text(json.dumps(MINIMAL_SPEC_V1))
        save_meta(meta_path, {"sha256": canonical_hash(MINIMAL_SPEC_V1), "last_checked": "2020-01-01T00:00:00+00:00"})
        responses.add(responses.GET, TEST_URL, json=MINIMAL_SPEC_V1, status=200)

        exit_code = check_and_update(force=True, url=TEST_URL, spec_path=spec_path, meta_path=meta_path)

        assert exit_code == 0
        updated_meta = load_meta(meta_path)
        assert updated_meta["last_checked"] != "2020-01-01T00:00:00+00:00"
        assert spec_path.read_text() == json.dumps(MINIMAL_SPEC_V1)  # untouched, not rewritten

    @responses.activate
    def test_change_downloads_and_updates_meta(self, tmp_path):
        spec_path = tmp_path / "spec.json"
        meta_path = tmp_path / "meta.json"
        spec_path.write_text(json.dumps(MINIMAL_SPEC_V1))
        save_meta(meta_path, {"sha256": canonical_hash(MINIMAL_SPEC_V1), "last_checked": "2020-01-01T00:00:00+00:00"})
        responses.add(responses.GET, TEST_URL, json=MINIMAL_SPEC_V2, status=200)

        exit_code = check_and_update(force=True, url=TEST_URL, spec_path=spec_path, meta_path=meta_path)

        assert exit_code == 2
        assert json.loads(spec_path.read_text()) == MINIMAL_SPEC_V2
        updated_meta = load_meta(meta_path)
        assert updated_meta["sha256"] == canonical_hash(MINIMAL_SPEC_V2)
        assert "last_changed" in updated_meta

    @responses.activate
    def test_first_run_with_no_prior_meta_downloads(self, tmp_path):
        spec_path = tmp_path / "spec.json"
        meta_path = tmp_path / "meta.json"
        responses.add(responses.GET, TEST_URL, json=MINIMAL_SPEC_V1, status=200)

        exit_code = check_and_update(force=False, url=TEST_URL, spec_path=spec_path, meta_path=meta_path)

        assert exit_code == 2
        assert json.loads(spec_path.read_text()) == MINIMAL_SPEC_V1

    @responses.activate
    def test_fetch_error_returns_1_and_does_not_touch_files(self, tmp_path):
        spec_path = tmp_path / "spec.json"
        meta_path = tmp_path / "meta.json"
        spec_path.write_text(json.dumps(MINIMAL_SPEC_V1))
        responses.add(responses.GET, TEST_URL, status=500)

        exit_code = check_and_update(force=True, url=TEST_URL, spec_path=spec_path, meta_path=meta_path)

        assert exit_code == 1
        assert json.loads(spec_path.read_text()) == MINIMAL_SPEC_V1
        assert not meta_path.exists()
