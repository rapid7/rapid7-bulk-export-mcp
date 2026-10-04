"""Inbound bearer-token authentication for the remote (HTTP) transport.

The server is a *resource server*, not an authorization server: a client
presents an OIDC bearer token and we only verify its signature, issuer,
audience and (optionally) scopes. All of that verification is FastMCP's
``JWTVerifier`` — we add no crypto and no JWT-parsing logic of our own.

FastMCP 3.4.4 has no environment-variable or declarative auth configuration;
auth is a constructor argument. This module is the thin wiring layer that turns
deploy-time environment variables into that argument, which is what lets a
single image be configured per deployment with no rebuild.

The JWKS URI is passed straight through to ``JWTVerifier`` rather than derived
from the issuer via OIDC discovery: a pure pass-through keeps this module
logic-free, so there is nothing here to get wrong.
"""

import os
import sys
from typing import List, Optional

from fastmcp.server.auth import MultiAuth
from fastmcp.server.auth.providers.jwt import JWTVerifier

# Environment variables read at startup. Names are shared with the deployment
# templates and the authentication docs, so treat them as a public contract.
ENV_JWKS_URI = "MCP_AUTH_JWKS_URI"
ENV_ISSUER = "MCP_AUTH_ISSUER"
ENV_AUDIENCE = "MCP_AUTH_AUDIENCE"
ENV_REQUIRED_SCOPES = "MCP_AUTH_REQUIRED_SCOPES"


def _split_csv(value: str) -> List[str]:
    """Split a comma-separated environment value, dropping blanks and whitespace."""
    return [item.strip() for item in value.split(",") if item.strip()]


def build_auth() -> Optional[MultiAuth]:
    """Build the inbound auth provider from the environment.

    Returns ``None`` when no JWKS URI is configured, which is how the caller
    tells "unconfigured" (stdio's normal state) apart from "misconfigured".
    A JWKS URI present but issuer or audience missing is a misconfiguration and
    raises, because starting an HTTP server that cannot actually validate a
    token is exactly the silent failure this exists to prevent.

    A single issuer builds one ``JWTVerifier``. Several issuers each get their
    own verifier composed with ``MultiAuth``, which tries them in turn — useful
    for a tenant migrating between identity providers. The return type is always
    ``MultiAuth`` so the caller has one wrapper to reason about regardless of
    issuer count.
    """
    jwks_uri = os.environ.get(ENV_JWKS_URI, "").strip()
    if not jwks_uri:
        return None

    issuers = _split_csv(os.environ.get(ENV_ISSUER, ""))
    if not issuers:
        raise ValueError(
            f"{ENV_JWKS_URI} is set but {ENV_ISSUER} is not. "
            f"Set {ENV_ISSUER} to the token issuer (comma-separated to accept several)."
        )

    audience = os.environ.get(ENV_AUDIENCE, "").strip()
    if not audience:
        raise ValueError(
            f"{ENV_JWKS_URI} is set but {ENV_AUDIENCE} is not. "
            f"Set {ENV_AUDIENCE} to the audience this server is registered as."
        )

    required_scopes = _split_csv(os.environ.get(ENV_REQUIRED_SCOPES, ""))
    scopes_arg = required_scopes or None

    verifiers = [
        JWTVerifier(
            jwks_uri=jwks_uri,
            issuer=issuer,
            audience=audience,
            required_scopes=scopes_arg,
        )
        for issuer in issuers
    ]

    if len(verifiers) > 1:
        print(
            f"Inbound auth: verifying bearer tokens against {len(verifiers)} issuers",
            file=sys.stderr,
        )
    return MultiAuth(verifiers=verifiers)
