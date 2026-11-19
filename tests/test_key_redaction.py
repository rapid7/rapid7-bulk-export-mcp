"""No org API key leaves the process in error text, or travels to a host that is not Rapid7's API."""

import json

import pytest
import responses

from src import config
from src.config import redact_secret
from src.download import download_parquet_file

ORG_KEYS = {
    "R7_KEY_ALPHA": "11111111-aaaa-4aaa-8aaa-111111111111",
    "R7_KEY_BRAVO": "22222222-bbbb-4bbb-8bbb-222222222222",
}


@pytest.fixture
def orgs(tmp_path, monkeypatch):
    path = tmp_path / "orgs.json"
    path.write_text(
        json.dumps(
            {
                "orgs": [
                    {"label": "alpha", "key_ref": "R7_KEY_ALPHA", "region": "us"},
                    {"label": "bravo", "key_ref": "R7_KEY_BRAVO", "region": "eu"},
                    {"label": "missing", "key_ref": "R7_KEY_MISSING", "region": "us"},
                ]
            }
        )
    )
    monkeypatch.setenv("RAPID7_ORGS_FILE", str(path))
    monkeypatch.setenv("RAPID7_API_KEY", "single-org-key-value")
    for name, value in ORG_KEYS.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("R7_KEY_MISSING", raising=False)
    monkeypatch.setattr(config, "_get_key_from_keychain", lambda name: None)


def test_every_org_key_and_the_single_key_are_redacted(orgs):
    text = f"failed: {ORG_KEYS['R7_KEY_ALPHA']} then {ORG_KEYS['R7_KEY_BRAVO']} then single-org-key-value"

    redacted = redact_secret(text)

    assert redacted == "failed: ***REDACTED*** then ***REDACTED*** then ***REDACTED***"


def test_an_org_whose_key_cannot_be_found_does_not_stop_the_others_being_redacted(orgs):
    assert redact_secret(f"x {ORG_KEYS['R7_KEY_BRAVO']}") == "x ***REDACTED***"


def test_a_broken_orgs_file_still_redacts_the_single_key(orgs, tmp_path, monkeypatch):
    broken = tmp_path / "broken.json"
    broken.write_text("{not json")
    monkeypatch.setenv("RAPID7_ORGS_FILE", str(broken))

    assert redact_secret("single-org-key-value") == "***REDACTED***"


def test_a_tool_error_carrying_an_org_key_reaches_the_caller_redacted(orgs, tmp_path, monkeypatch):
    from src import mcp_server

    usable = tmp_path / "usable.json"
    usable.write_text(json.dumps({"orgs": [{"label": "alpha", "key_ref": "R7_KEY_ALPHA", "region": "us"}]}))
    monkeypatch.setenv("RAPID7_ORGS_FILE", str(usable))
    monkeypatch.setattr(mcp_server, "_DATA_DIR", tmp_path)

    def _fail(cfg):
        raise ValueError(f"upstream echoed the request: X-Api-Key={cfg['api_key']}")

    monkeypatch.setattr(mcp_server, "create_vulnerability_export", _fail)

    result = mcp_server.start_rapid7_export(export_type="vulnerability", org_label="alpha")

    assert ORG_KEYS["R7_KEY_ALPHA"] not in result
    assert "X-Api-Key=***REDACTED***" in result


@pytest.mark.parametrize(
    "url",
    [
        "https://rapid7-exports.s3.us-west-2.amazonaws.com/file.parquet?X-Amz-Signature=abc",
        "https://example.com/file.parquet",
        "http://us.api.insight.rapid7.com/file.parquet",
        "https://us.api.insight.rapid7.com.evil.example/file.parquet",
    ],
)
@responses.activate
def test_a_download_sends_no_key_to_any_host_but_rapid7s_api(url):
    responses.add(responses.GET, url.split("?")[0], body=b"PAR1")

    download_parquet_file(url, "secret-key")

    assert "X-Api-Key" not in responses.calls[0].request.headers


@responses.activate
def test_a_download_from_rapid7s_api_still_sends_the_key():
    url = "https://us.api.insight.rapid7.com/export/files/file.parquet"
    responses.add(responses.GET, url, body=b"PAR1")

    download_parquet_file(url, "secret-key")

    assert responses.calls[0].request.headers["X-Api-Key"] == "secret-key"
