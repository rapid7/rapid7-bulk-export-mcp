# Authenticating the remote endpoint

When you run the server in remote (Docker / streamable HTTP) mode, it exposes a
single `/mcp` endpoint over the network. Securing that endpoint is a **supported
feature of the server, not something you are left to build yourself**: the server
validates OIDC bearer tokens for you, and it works with **any standards-compliant
identity provider**. Nothing here is Microsoft-specific — if your identity provider
issues signed JWTs and publishes a JWKS endpoint, this server can already verify
its tokens.

This document is deliberately standalone. If you run Docker with no Microsoft
footprint at all, everything you need to secure the endpoint is on this page.

## Why it works with any provider

The server is a **resource server**, not an authorisation server. It does not log
users in, mint tokens, or run an OAuth flow. It does one thing: for each request on
`/mcp` it takes the bearer token the client presents and verifies it —
the signature, the issuer, the audience and (optionally) the required scopes.

Verifying an RS256 JWT against a JWKS endpoint is the **same operation for every
provider**. Entra ID, Okta, Auth0 and Keycloak all sign tokens the same way and all
publish their public keys at a JWKS URL. So supporting a new provider is a matter of
**configuration, not code** — you point the server at that provider's JWKS URL,
issuer and audience, and it works. There is no per-provider adapter to wait for and
no allowlist of "supported" providers.

## Configuration

Auth is configured entirely through environment variables, read **once at startup**.
That means a container is configured at deploy time with **no image rebuild** — the
same image runs unauthenticated on stdio locally and authenticated behind your IdP
in production, decided purely by the environment it starts in.

| Variable | Required for HTTP | What it is |
| --- | --- | --- |
| `MCP_AUTH_JWKS_URI` | Yes | Your provider's JWKS endpoint (the URL that publishes its signing keys). Presence of this variable is what turns auth on. |
| `MCP_AUTH_ISSUER` | Yes | The token issuer (the `iss` claim the provider stamps). Comma-separated to accept several — see [Accepting several issuers](#accepting-several-issuers-at-once). |
| `MCP_AUTH_AUDIENCE` | Yes | The audience this server is registered as (the `aud` claim). This is the API/app identifier you created for the server in your IdP. |
| `MCP_AUTH_REQUIRED_SCOPES` | No | Comma-separated scopes every caller must present. Optional; leave unset to accept any validly-signed token for this audience. |
| `MCP_AUTH_WRITE_SCOPE` | No | The scope that gates the write tools. Defaults to `rapid7.write`. See [Read and write scopes](#read-and-write-scopes). |

The JWKS URL is passed straight through to the verifier — the server does **not** do
OIDC discovery, so give it the JWKS URL directly rather than a discovery document or
a bare issuer.

If `MCP_AUTH_JWKS_URI` is set but `MCP_AUTH_ISSUER` or `MCP_AUTH_AUDIENCE` is
missing, the server treats that as a misconfiguration and refuses to start — starting
an HTTP server that cannot actually validate a token is exactly the silent failure
this is meant to prevent.

## Worked examples, side by side

The four examples below configure four different identity providers. **Only the
values change** — the variables, and what the server does with them, are identical.
Substitute your own tenant/domain and the identifiers you assigned when you
registered the server as an API/application in each console.

### Entra ID (Microsoft)

Entra has **two token versions**, and the issuer and audience must come from the
same one. Mixing them is the most common cause of a 401 here, and the pairing below
was wrong in an earlier version of this document.

**v1 tokens — the default for an app with an Application ID URI**, and what a Power
Platform custom connector receives:

```bash
MCP_AUTH_JWKS_URI=https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys
MCP_AUTH_ISSUER=https://sts.windows.net/<tenant-id>/
MCP_AUTH_AUDIENCE=api://<application-client-id>
```

Note the issuer host is `sts.windows.net`, **with a trailing slash** — the comparison
is exact. The `api://` audience is what a v1 token carries in `aud`.

**v2 tokens — only if the app registration sets
`api.requestedAccessTokenVersion: 2`:**

```bash
MCP_AUTH_JWKS_URI=https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys
MCP_AUTH_ISSUER=https://login.microsoftonline.com/<tenant-id>/v2.0
MCP_AUTH_AUDIENCE=<application-client-id>
```

A v2 token's `aud` is the **bare client-id GUID**, not the `api://` URI.

> **Do not pair the v2 issuer with an `api://` audience.** Entra never issues that
> combination, so every token is rejected. The server names the offending claim in
> its log, which is the fastest way to tell which version you are actually getting:
>
> ```
> Bearer token rejected: issuer mismatch (got 'https://sts.windows.net/<tenant>/',
>   expected 'https://login.microsoftonline.com/<tenant>/v2.0')
> ```


### Okta

```bash
MCP_AUTH_JWKS_URI=https://<your-org>.okta.com/oauth2/<auth-server-id>/v1/keys
MCP_AUTH_ISSUER=https://<your-org>.okta.com/oauth2/<auth-server-id>
MCP_AUTH_AUDIENCE=api://rapid7-mcp
```

### Auth0

```bash
MCP_AUTH_JWKS_URI=https://<your-tenant>.auth0.com/.well-known/jwks.json
MCP_AUTH_ISSUER=https://<your-tenant>.auth0.com/
MCP_AUTH_AUDIENCE=https://rapid7-mcp.example.com
```

### Keycloak

```bash
MCP_AUTH_JWKS_URI=https://<host>/realms/<realm>/protocol/openid-connect/certs
MCP_AUTH_ISSUER=https://<host>/realms/<realm>
MCP_AUTH_AUDIENCE=rapid7-mcp
```

### Any other OIDC provider

The list above is illustrative, not an allowlist — any standards-compliant OIDC
provider works. In its admin console, find:

- **JWKS URI** — usually published in the provider's OpenID configuration document
  at `https://<issuer>/.well-known/openid-configuration` under the `jwks_uri` key,
  or listed directly as the "signing keys" / "JWKS" / "certs" endpoint.
- **Issuer** — the `issuer` value in that same configuration document; it must match
  the `iss` claim on the tokens the provider issues. (Note the trailing-slash
  conventions differ between providers, as the examples above show — copy it exactly.)
- **Audience** — the identifier of the API/application you registered for this
  server. It must match the `aud` claim your provider puts on tokens minted for it.

Set those three values and the server will validate that provider's tokens with no
further changes.

## Accepting several issuers at once

`MCP_AUTH_ISSUER` accepts a comma-separated list. When you give it more than one, the
server builds a verifier per issuer and tries each in turn; a token is accepted if it
validates against **any** configured issuer (all still checked against the same
audience and scopes).

```bash
MCP_AUTH_ISSUER=https://old-idp.example.com,https://new-idp.example.com
```

This is useful when:

- You are **migrating between providers** and both are live during the cutover — you
  do not have to take the endpoint down or run two deployments.
- One server **serves two populations** authenticated by different issuers.

## Fail-closed by design

If you set `MCP_TRANSPORT=http` **without** configuring auth, the server **refuses to
start** and exits with an error rather than serving an open endpoint:

```
Refusing to start HTTP transport with no inbound authentication configured.
Set MCP_AUTH_JWKS_URI, MCP_AUTH_ISSUER and MCP_AUTH_AUDIENCE (see docs/authentication.md),
or use stdio for local, client-owned use.
```

This is intentional. An unauthenticated HTTP deployment exposes the whole dataset to
anyone who can reach the endpoint, so the server treats "HTTP with no auth" as a
deployment mistake and stops, rather than coming up quietly and leaking data.

**stdio stays unauthenticated by design.** In local (stdio) mode the client spawns
the server as its own child process and owns it directly, so there is no network
surface and no token to check — the operating system's process boundary is the
security boundary. This is native behaviour, not a special case in our code: FastMCP
skips inbound auth checks entirely on the stdio transport, so the write-scope gate
below is inert there too and local behaviour is unchanged whether or not auth
variables happen to be set.

## Read and write scopes

Because the endpoint serves a shared dataset with **no per-user data filtering**, the
one authorisation control that must hold is keeping ordinary callers off the
expensive and destructive operations. The tools are split into two classes.

**Write tools** — tagged and gated behind the write scope:

- `start_rapid7_export` — triggers a new bulk export on the Rapid7 platform.
- `download_rapid7_export` — downloads an export and loads it into the local database.
- `load_rapid7_parquet` — loads a Parquet file into the local database.
- `purge_rapid7_data` — permanently deletes all local Rapid7 data.

**Read tools** — open to any authenticated caller, no extra scope:

- `query_rapid7`
- `get_rapid7_schema`
- `get_rapid7_stats`
- `list_rapid7_exports`
- `check_rapid7_export_status`

A caller whose token does not carry the write scope can use every read tool but is
refused on every write tool. A caller whose token carries it can use both.

### Granting the write scope selectively

The write scope defaults to `rapid7.write` and is configurable via
`MCP_AUTH_WRITE_SCOPE` — it is a deploy-time contract with your IdP, so set it once
and grant it deliberately:

- **Everyday users** get a token that carries only the read scope (or no scope,
  if you leave `MCP_AUTH_REQUIRED_SCOPES` unset). They can query the data and nothing
  more.
- **An operator or a service principal** that runs refreshes gets a token that also
  carries the write scope. In your IdP, grant that scope only to the specific
  user/group or client that needs it — do not add it to the default set every user
  receives.

The gate enforces the exact configured scope: a token holding some other
write-shaped scope is still refused, so there is no accidental back door through a
similarly-named permission.

## Prefer delegated user tokens over client credentials

Where you have the choice, issue **delegated user tokens** (a token minted for the
signed-in human) rather than **client-credentials tokens** (a single token
representing an application).

The reason is specific to this server: there is **no per-user data filtering**. Every
authenticated caller sees the whole dataset, so the server cannot record *who* asked
a given question beyond what is in the token itself. A delegated user token carries
the individual's identity (`sub`, and usually name/email claims), so your IdP's
sign-in and token-issuance logs are the audit trail of who queried the data. A shared
client-credentials token collapses everyone into one application identity and throws
that trail away. Use client credentials only for genuine machine-to-machine callers
(such as the scheduled refresh job), and keep the write scope on those rather than on
human tokens.
