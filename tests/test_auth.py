"""Tests for inbound bearer-token authentication.

The verification semantics (accept a good token, reject a bad one) are exercised
against the exact provider objects ``build_auth`` returns — a ``JWTVerifier``, or
a ``MultiAuth`` of several — but constructed from a local ``RSAKeyPair`` public
key rather than a live JWKS endpoint, so no identity provider is stood up. The
environment-parsing and fail-closed behaviour is tested through ``build_auth``
and the server entrypoint directly.
"""

import os
import subprocess  # nosec B404
import sys

import anyio
import pytest
from fastmcp.server.auth import MultiAuth
from fastmcp.server.auth.providers.jwt import JWTVerifier, RSAKeyPair

from src.auth import (
    ENV_AUDIENCE,
    ENV_ISSUER,
    ENV_JWKS_URI,
    ENV_REQUIRED_SCOPES,
    build_auth,
)

AUDIENCE = "rapid7-mcp"
ISSUER_A = "https://issuer-a.example.com"
ISSUER_B = "https://issuer-b.example.com"


def _verify(provider, token):
    """Drive the async verify_token to completion synchronously.

    The suite avoids a pytest-asyncio dependency by running the single coroutine
    through anyio, which FastMCP already pulls in, rather than adding a test-only
    plugin.
    """
    return anyio.run(provider.verify_token, token)


def _verifier(keypair, *, issuer=ISSUER_A, audience=AUDIENCE, required_scopes=None):
    """A JWTVerifier bound to a local public key — the offline analogue of the
    JWKS-backed verifier build_auth constructs, so accept/reject logic runs the
    same code path without a network fetch."""
    return JWTVerifier(
        public_key=keypair.public_key,
        issuer=issuer,
        audience=audience,
        required_scopes=required_scopes,
    )


@pytest.fixture
def keypair_a():
    return RSAKeyPair.generate()


@pytest.fixture
def keypair_b():
    return RSAKeyPair.generate()


# --- Verification semantics -------------------------------------------------


def test_valid_token_accepted(keypair_a):
    verifier = _verifier(keypair_a)
    token = keypair_a.create_token(issuer=ISSUER_A, audience=AUDIENCE, subject="user-1")

    result = _verify(verifier, token)

    assert result is not None
    assert result.claims["iss"] == ISSUER_A
    assert result.claims["aud"] == AUDIENCE


def test_wrong_audience_rejected(keypair_a):
    verifier = _verifier(keypair_a)
    token = keypair_a.create_token(issuer=ISSUER_A, audience="some-other-audience", subject="user-1")

    assert _verify(verifier, token) is None


def test_wrong_issuer_rejected(keypair_a):
    verifier = _verifier(keypair_a)
    token = keypair_a.create_token(issuer="https://evil.example.com", audience=AUDIENCE, subject="user-1")

    assert _verify(verifier, token) is None


def test_expired_token_rejected(keypair_a):
    verifier = _verifier(keypair_a)
    token = keypair_a.create_token(
        issuer=ISSUER_A,
        audience=AUDIENCE,
        subject="user-1",
        expires_in_seconds=-60,
    )

    assert _verify(verifier, token) is None


def test_unsigned_token_rejected(keypair_a, keypair_b):
    """The wrong-key / unsigned case: a well-formed JWT whose signature does not
    verify against the configured public key is rejected."""
    verifier = _verifier(keypair_a)
    token = keypair_b.create_token(issuer=ISSUER_A, audience=AUDIENCE, subject="user-1")

    assert _verify(verifier, token) is None


def test_required_scope_enforced(keypair_a):
    verifier = _verifier(keypair_a, required_scopes=["rapid7.read"])
    without_scope = keypair_a.create_token(issuer=ISSUER_A, audience=AUDIENCE, subject="user-1")
    with_scope = keypair_a.create_token(
        issuer=ISSUER_A,
        audience=AUDIENCE,
        subject="user-1",
        scopes=["rapid7.read"],
    )

    assert _verify(verifier, without_scope) is None
    assert _verify(verifier, with_scope) is not None


# --- Multiple issuers -------------------------------------------------------


def test_two_issuers_both_accepted(keypair_a, keypair_b):
    multi = MultiAuth(verifiers=[_verifier(keypair_a, issuer=ISSUER_A), _verifier(keypair_b, issuer=ISSUER_B)])
    token_a = keypair_a.create_token(issuer=ISSUER_A, audience=AUDIENCE, subject="user-a")
    token_b = keypair_b.create_token(issuer=ISSUER_B, audience=AUDIENCE, subject="user-b")

    assert _verify(multi, token_a) is not None
    assert _verify(multi, token_b) is not None


def test_multi_issuer_rejects_untrusted(keypair_a, keypair_b):
    multi = MultiAuth(verifiers=[_verifier(keypair_a, issuer=ISSUER_A), _verifier(keypair_b, issuer=ISSUER_B)])
    stranger = RSAKeyPair.generate()
    token = stranger.create_token(issuer=ISSUER_A, audience=AUDIENCE, subject="user-x")

    assert _verify(multi, token) is None


# --- build_auth environment wiring ------------------------------------------


def test_build_auth_returns_none_when_unconfigured(monkeypatch):
    for var in (ENV_JWKS_URI, ENV_ISSUER, ENV_AUDIENCE, ENV_REQUIRED_SCOPES):
        monkeypatch.delenv(var, raising=False)

    assert build_auth() is None


def test_build_auth_single_issuer(monkeypatch):
    monkeypatch.setenv(ENV_JWKS_URI, "https://issuer-a.example.com/.well-known/jwks.json")
    monkeypatch.setenv(ENV_ISSUER, ISSUER_A)
    monkeypatch.setenv(ENV_AUDIENCE, AUDIENCE)
    monkeypatch.delenv(ENV_REQUIRED_SCOPES, raising=False)

    assert isinstance(build_auth(), MultiAuth)


def test_build_auth_multiple_issuers(monkeypatch):
    monkeypatch.setenv(ENV_JWKS_URI, "https://shared.example.com/.well-known/jwks.json")
    monkeypatch.setenv(ENV_ISSUER, f"{ISSUER_A}, {ISSUER_B}")
    monkeypatch.setenv(ENV_AUDIENCE, AUDIENCE)

    assert isinstance(build_auth(), MultiAuth)


def test_build_auth_jwks_without_issuer_raises(monkeypatch):
    monkeypatch.setenv(ENV_JWKS_URI, "https://issuer-a.example.com/.well-known/jwks.json")
    monkeypatch.delenv(ENV_ISSUER, raising=False)
    monkeypatch.setenv(ENV_AUDIENCE, AUDIENCE)

    with pytest.raises(ValueError, match=ENV_ISSUER):
        build_auth()


def test_build_auth_jwks_without_audience_raises(monkeypatch):
    monkeypatch.setenv(ENV_JWKS_URI, "https://issuer-a.example.com/.well-known/jwks.json")
    monkeypatch.setenv(ENV_ISSUER, ISSUER_A)
    monkeypatch.delenv(ENV_AUDIENCE, raising=False)

    with pytest.raises(ValueError, match=ENV_AUDIENCE):
        build_auth()


# --- Transport fail-closed / stdio behaviour --------------------------------
#
# Exercised by launching the real entrypoint as a subprocess so the transport
# branch in main() is what is under test, not a reimplementation of it.


def _run_server(env_overrides, args=(), timeout=30, data_dir=None):
    env = dict(os.environ)
    # Start from a clean auth + transport slate so the host environment cannot
    # mask the case under test.
    for var in ("MCP_TRANSPORT", ENV_JWKS_URI, ENV_ISSUER, ENV_AUDIENCE, ENV_REQUIRED_SCOPES):
        env.pop(var, None)
    if data_dir is not None:
        env["DATA_DIR"] = str(data_dir)
    env.update(env_overrides)
    return subprocess.run(  # nosec B603
        [sys.executable, "-m", "src.mcp_server", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_http_transport_without_auth_fails_at_startup(tmp_path):
    result = _run_server({"MCP_TRANSPORT": "http"}, data_dir=tmp_path)

    assert result.returncode != 0
    stderr = result.stderr.lower()
    assert "no inbound authentication configured" in stderr or "refusing to start" in stderr


def test_help_text_lists_auth_env_vars(tmp_path):
    """The stdio path is unaffected by absent auth config: the entrypoint runs
    and documents the auth variables rather than erroring."""
    result = _run_server({}, args=["--help"], data_dir=tmp_path)

    assert result.returncode == 0
    assert ENV_JWKS_URI in result.stdout
    assert ENV_ISSUER in result.stdout
    assert ENV_AUDIENCE in result.stdout
