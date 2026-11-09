"""Tests for the read/write scope separation on the MCP tools.

Phase 4 authenticates the caller; this asserts what a caller may then *do*. The
mutating tools carry a ``write`` tag and a ``restrict_tag`` auth check requiring a
write scope; the read tools carry neither and stay open to any authenticated
caller. Published to a broad Copilot audience with no per-user data filtering,
that split is the control keeping casual users off the expensive export and the
destructive purge.

The checks run exactly as the server runs them — for each tool with an auth
check, an ``AuthContext`` is built and driven through ``run_auth_checks`` (the
same call ``FastMCP`` makes before listing or calling a tool). Tokens are minted
and verified through a ``JWTVerifier`` bound to a local ``RSAKeyPair`` public key,
so a real ``AccessToken`` with real scopes reaches the check without standing up
an identity provider — the offline pattern ``tests/test_auth.py`` already uses.
"""

import anyio
import pytest
from fastmcp.server.auth.providers.jwt import JWTVerifier, RSAKeyPair
from fastmcp.utilities.authorization import AuthContext, run_auth_checks

from src import mcp_server
from src.mcp_server import WRITE_SCOPE

AUDIENCE = "rapid7-mcp"
ISSUER = "https://issuer.example.com"

WRITE_TOOLS = (
    "start_rapid7_export",
    "download_rapid7_export",
    "load_rapid7_parquet",
    "purge_rapid7_data",
)
READ_TOOLS = (
    "query_rapid7",
    "get_rapid7_schema",
    "get_rapid7_stats",
    "list_rapid7_exports",
    "check_rapid7_export_status",
)


@pytest.fixture
def keypair():
    return RSAKeyPair.generate()


@pytest.fixture
def verifier(keypair):
    return JWTVerifier(public_key=keypair.public_key, issuer=ISSUER, audience=AUDIENCE)


def _access_token(keypair, verifier, *, scopes):
    """Mint a token with the given scopes and verify it into an AccessToken.

    Verifying rather than hand-constructing the token means ``scopes`` lands on
    the AccessToken exactly as it would for a live request."""
    raw = keypair.create_token(issuer=ISSUER, audience=AUDIENCE, subject="user", scopes=scopes)
    token = anyio.run(verifier.verify_token, raw)
    assert token is not None, "test token failed to verify"
    return token


def _tools_by_name():
    """All registered tools, bypassing the request-context auth filter that
    list_tools applies (there is no request context in a unit test)."""
    tools = anyio.run(mcp_server.mcp._local_provider._list_tools)
    return {t.name: t for t in tools}


def _allowed(tool, token):
    """Run the tool's auth checks the way the server does. A tool with no auth
    check is unconditionally allowed, matching the server's ``tool.auth is None``
    short-circuit."""
    if tool.auth is None:
        return True
    ctx = AuthContext(token=token, component=tool)
    return anyio.run(run_auth_checks, tool.auth, ctx)


# --- Tagging ----------------------------------------------------------------


def test_write_tools_are_tagged_and_gated():
    tools = _tools_by_name()
    for name in WRITE_TOOLS:
        tool = tools[name]
        assert "write" in tool.tags, f"{name} should carry the write tag"
        assert tool.auth is not None, f"{name} should have a scope check"


def test_read_tools_are_open():
    tools = _tools_by_name()
    for name in READ_TOOLS:
        tool = tools[name]
        assert "write" not in tool.tags, f"{name} must not be tagged write"
        assert tool.auth is None, f"{name} must not be scope-gated"


# --- A caller without the write scope ---------------------------------------


def test_token_without_write_scope_refused_on_write_tools(keypair, verifier):
    token = _access_token(keypair, verifier, scopes=["rapid7.read"])
    tools = _tools_by_name()
    for name in WRITE_TOOLS:
        assert _allowed(tools[name], token) is False, f"{name} must be refused without the write scope"


def test_token_without_write_scope_permitted_on_read_tools(keypair, verifier):
    token = _access_token(keypair, verifier, scopes=["rapid7.read"])
    tools = _tools_by_name()
    for name in READ_TOOLS:
        assert _allowed(tools[name], token) is True, f"{name} must be readable without the write scope"


# --- A caller with the write scope ------------------------------------------


def test_token_with_write_scope_permitted_on_all_tools(keypair, verifier):
    token = _access_token(keypair, verifier, scopes=["rapid7.read", WRITE_SCOPE])
    tools = _tools_by_name()
    for name in WRITE_TOOLS + READ_TOOLS:
        assert _allowed(tools[name], token) is True, f"{name} must be allowed with the write scope"


def test_configured_write_scope_is_what_is_enforced(keypair, verifier):
    """The gate enforces the module-level WRITE_SCOPE constant specifically — a
    caller holding some other write-shaped scope is still refused."""
    token = _access_token(keypair, verifier, scopes=["some.other.write"])
    tools = _tools_by_name()
    for name in WRITE_TOOLS:
        assert _allowed(tools[name], token) is False


# --- stdio is unaffected ----------------------------------------------------


def test_stdio_transport_skips_component_auth():
    """On stdio the client owns the process, so FastMCP skips component auth
    wholesale — the write gate is inert and behaviour is unchanged. Assert the
    server's own decision point rather than reimplementing it."""
    from fastmcp.server import context as fastmcp_context
    from fastmcp.server import server as fastmcp_server

    reset = fastmcp_context._current_transport.set("stdio")
    try:
        skip_auth, token = fastmcp_server._get_auth_context()
    finally:
        fastmcp_context._current_transport.reset(reset)

    assert skip_auth is True
    assert token is None
