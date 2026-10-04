# Hosting for Microsoft Copilot Studio (private)

This guide covers running the Rapid7 Bulk Export MCP server privately on Azure so
a Microsoft Copilot Studio agent can reach it — with **no public inbound path**.
It replaces the earlier `docs/copilot-studio-setup.md` proposal.

The endpoint holds your organisation's complete vulnerability posture, so the
whole design keeps it off the public internet: the Power Platform connector
runtime runs inside your own tenant and reaches the server across a delegated
subnet, so no public ingress is ever required. Agent 365 "bring your own MCP
server" registration is deliberately **not** a path here — see
[Where users talk to the agent](#where-users-talk-to-the-agent).

For the deployment template itself and the Azure CLI steps, see
[`deploy/azure/README.md`](../deploy/azure/README.md). For the authentication
mechanism — which is generic and applies to any remote deployment, Microsoft or
not — see [`docs/authentication.md`](authentication.md). This guide links to that
document rather than restating it.

## Prerequisites

These gate everything. Confirm them before promising a timeline — three of them
are things only the customer's tenant admins can provide, and one is a licensing
fact that otherwise costs a day to discover.

- **A Power Platform Managed Environment.** Virtual-network (subnet-delegation)
  support for custom connectors requires it. Managed Environments enforcement
  needs a premium licence per active user, which a Copilot Studio licence already
  satisfies — no separate Power Apps licence is needed.
- **Delegated subnets in the region pair matching your Power Platform region.**
  There are **two** delegations, to two different services, and they are not
  interchangeable:
  - the Container Apps **infrastructure subnet**, delegated to
    `Microsoft.App/environments` — you pass its resource id as
    `infrastructureSubnetId`;
  - the Power Platform **connector subnet(s)**, delegated to
    `Microsoft.PowerPlatform/enterprisePolicies`, in **both** regions of the
    region pair so failover works.

  Both are customer-owned and **not** provisioned by the template. Put them in
  the **same virtual network**: a same-VNet caller reaches an internal Container
  Apps environment directly through its internal load balancer, so **no private
  endpoint is required**. A private endpoint is only needed to cross into a
  different, non-injected VNet, and it would additionally force a
  workload-profiles environment with a `/27`-or-larger subnet.

- **An Entra app registration exposing a scope** for the server. This is a
  Microsoft Graph object, not an ARM resource, so it cannot be created in Bicep —
  create it with the Azure CLI after deployment (steps in
  [`deploy/azure/README.md`](../deploy/azure/README.md)). Its Application ID URI
  becomes the token audience.
- **Copilot Studio licensing sufficient for generative orchestration.** MCP tools
  require **generative** orchestration. A Teams plan alone is **not** sufficient:
  Teams-plan makers are limited to classic orchestration and cannot invoke MCP
  tools at all. A standalone Copilot Studio subscription or Microsoft 365 Copilot
  covers it. This is a documented prerequisite, not a blocker — state it up front
  so the next reader does not lose a day to it. End users need no licence to *chat*
  with a published agent, though their usage draws Copilot Credits unless they hold
  Microsoft 365 Copilot.

## Private deployment

A service-to-service diagram of this topology, with the protocol and authentication on
each edge, is in [`architecture-diagrams.md`](architecture-diagrams.md).

Nothing in the request path has a public IP. The topology is:

```
Copilot Studio agent
      │  MCP tool → Power Platform connector runtime (your tenant)
      ▼
delegated subnet ──► internal load balancer ──► Container App (no public IP)
                     (same VNet, no private
                      endpoint required)          │  reads a local, read-only copy
                                              ▼
                                         rapid7_bulk_export.db  (local ephemeral disk, never network)
                                              ▲
                                              │ downloads current artifact over HTTPS
                                         Blob Storage  ◄── publishes versioned artifact
                                              ▲
                                    Container Apps Job (scheduled, own ephemeral disk)
```

What the template provisions (see the table in
[`deploy/azure/README.md`](../deploy/azure/README.md)):

- **A serving Container App reachable only from the virtual network.** The
  environment is `internal: true`, which is what removes the public IP: an
  internal environment is provisioned with an **internal load balancer only**.
  There is no Front Door, no WAF and no Private Link origin resource — there is
  nothing public to protect. It is pinned to a **single replica** with no HTTP
  scale rule.

  The app itself sets `ingress.external: true`, and that is **deliberate — it
  does not mean internet-facing.** `external` is scoped to the *environment*, not
  the internet. On an internal environment it selects publication at the internal
  load balancer; setting it to `false` would restrict the app to other container
  apps inside the same environment and return **HTTP 404** to every other caller
  in the virtual network, the Power Platform connector runtime included. Microsoft's
  guidance for this topology is explicit: *"After you create an internal
  environment, set each app's ingress to external so that clients in your virtual
  network can reach it."* The property to assert on for the no-public-endpoint
  requirement is the environment's `internal`, surfaced as the
  `environmentIsInternal` output.
- **A private DNS zone for the environment, linked to your virtual network.**
  Internal environments do **not** get one automatically, so without it nothing in
  the VNet can resolve the app's FQDN and the request fails before it reaches the
  load balancer. The template creates a zone named after the environment's
  `defaultDomain` with a wildcard `A` record pointing at its internal load
  balancer IP. Set `createPrivateDnsZone: false` if your tenant provisions these
  centrally, and create the equivalent zone yourself from the
  `environmentDefaultDomain` and `environmentStaticIp` outputs.
- **Blob Storage for versioned artifacts, reached over HTTPS and never mounted.**
  Container Apps cannot mount Blob, and DuckDB must never run on the SMB/NFS mounts
  that are the only alternative. So the database is treated as a courier payload,
  not a filesystem: the refresh job builds the `.db` on its own local ephemeral
  disk, uploads it to Blob as a versioned artifact, and each serving replica
  downloads one copy to its own local disk and opens it **read-only**. A version
  becomes resolvable only once a completion marker lands beside its data blob, so a
  replica that resolves mid-upload sees no complete version rather than a truncated
  database.
- **A scheduled refresh job** (`Microsoft.App/jobs`). It runs on a cron schedule
  (default 02:00 UTC daily) or as a manual on-demand job when
  `enableScheduledRefresh` is `false` — the same code path either way. After a clean
  build it publishes the artifact to Blob as a new version. **Rolling a new Container
  App revision so a replica picks that version up is an operator or pipeline action,
  not something the job does** — see [Known limitations](#known-limitations). Once a
  revision does roll, the new replica downloads the new data and takes traffic, the
  old one drains, and revision history is the rollback path. Queries are always
  served from a complete database — there is no partial-data window.

## Known limitations

### A first deployment serves no data until restarted after its first refresh

Only the refresh job publishes an artifact, so a freshly deployed serving app finds
none. It starts anyway, and its read tools reply that no data is loaded yet and the
scheduled refresh has not completed, rather than returning empty results that would
read as "no vulnerabilities". Any other download failure — Blob unreachable, denied,
or a corrupt file — still stops the app, because a replica that can never obtain data
must not look healthy.

What it does not do is pick up the first artifact by itself: the replica keeps
replying "no data" until something restarts it. Step 6 of
[`../deploy/azure/SETUP.md`](../deploy/azure/SETUP.md) covers this by hand. Two ways
to make it self-heal:

- A **bootstrap watcher**: a background thread polls for the first complete version
  and exits cleanly when one appears, so the platform restarts the replica into it.
  No new permissions, and no swapping a DuckDB file under a live read-only handle.
- **Have the job roll a revision** through ARM after publishing. Closes the loop
  properly and also automates ongoing flips, but it grants the job control-plane
  write access to the container app, which it currently does not have.

Do **not** poll-and-reload in place: DuckDB refuses a read-only connection while a
read-write handle is writing the same file in-process, which is the constraint the
whole artifact-courier design exists to respect.

### Rolling a revision after a refresh is not automated

The job publishes a new artifact version; nothing rolls a revision to pick it up. A
running replica keeps serving the version it downloaded at start. Until the fix above
lands, refreshing what users actually see is an operator or pipeline step:

```bash
az containerapp revision restart -g <RG> -n <app> --revision <latest>
```

The managed identity is documented as needing permission for this, but no code uses
it — the permission grant is for an operator or pipeline acting as that identity, not
for the job.

### No per-user data filtering

Any user who can chat with the published agent can query the entire dataset. The
agent's publishing audience is the access boundary — see
[Access control](#access-control).

## Authentication

The mechanism is generic and fully documented in
[`docs/authentication.md`](authentication.md): the server is a resource server
that validates OIDC bearer tokens against a JWKS endpoint, configured entirely
through environment variables read once at startup, with **no image rebuild**. It
fails closed — the HTTP transport refuses to start with no auth configured. Read
that document for the full picture; only the Entra-specific values are repeated
here.

For an Entra ID tenant, the three values map to the app registration you create as
a prerequisite:

```bash
MCP_AUTH_JWKS_URI=https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys
MCP_AUTH_ISSUER=https://sts.windows.net/<tenant-id>/
MCP_AUTH_AUDIENCE=api://<application-client-id>
```

Note the issuer host is `sts.windows.net` **with a trailing slash**, not the
`login.microsoftonline.com/.../v2.0` form. An app registration with an Application ID
URI issues **v1** tokens by default, and a v1 token's issuer is `sts.windows.net`
while its `aud` is the `api://` URI. Pairing the v2 issuer with an `api://` audience
is a combination Entra never issues, so every token is rejected — see
[`docs/authentication.md`](authentication.md) for the v2 variant.

Prefer **delegated user tokens** over client credentials for the connector, so the
signed-in user's identity flows into the logs — there is no per-user data
filtering, so the token is the only record of who asked what. Use client
credentials only for genuine machine-to-machine callers such as the scheduled
refresh job.

## Securing the Rapid7 API key

The Rapid7 API key is the most valuable secret in the deployment: a long-lived
bearer credential granting read access to your entire vulnerability posture.
Rapid7's platform offers no federated-credential alternative, so a long-lived key
is unavoidable — the controls are on how it is stored, delivered, scoped and
rotated, and on how far the damage spreads if it leaks.

**The primary control is separation, and it is a security feature you can verify
in the template.** Only the refresh job needs the key; the serving replica — the
component accepting inbound requests — carries **no Rapid7 credential at all**. In
`deploy/azure/main.bicep` the key is a Key Vault secret reference on the *job*
resource only, so a compromise of the request-handling process yields no route
back to the Rapid7 platform. The write tools consequently cannot run on the
serving replica and fail closed with an explanatory message rather than an obscure
API error.

Delivery and storage:

- The key lives in **Azure Key Vault** (`publicNetworkAccess: Disabled`). A
  **user-assigned managed identity** is granted `get` on that one secret (the
  Key Vault Secrets User role), and the Container Apps Job consumes it as a Key
  Vault secret reference resolved at runtime by that identity.
- It is **never** a Bicep `@secure()` parameter materialised into container
  configuration — that would leave the value readable from the resource and would
  require a revision update to rotate. The template only names the secret; you set
  its value out-of-band.
- Use a **dedicated, least-privileged key per deployment**, scoped to bulk export,
  so revocation is surgical — it affects one integration rather than the whole
  organisation.

### Rotation runbook

Rotate entirely in Key Vault — **no redeploy and no rebuild**:

```bash
az keyvault secret set --vault-name <keyVaultName> --name rapid7-api-key --value '<new-key>'
```

The next job run resolves the current secret version at runtime. Then revoke the
old key in the Rapid7 console. Because the deployment uses its own dedicated key,
revocation touches only this integration.

## Wiring the connector in Copilot Studio

For the full ordered procedure — Azure through to publishing — see
[`deploy/azure/SETUP.md`](../deploy/azure/SETUP.md). The summary below covers the
ordering constraints specific to the connector.

Order matters here, and two steps cannot be prepared in advance.

1. **Link the enterprise policy to the environment first.** Admin centre → Security →
   Data and privacy → Azure Virtual Network policies. Only Managed Environments are
   listed. Confirm it took by checking `properties.enterprisePolicies.vNets.linkStatus`
   reads `Linked` on the environment.
2. **Create the custom connector after that link exists.** Tools → Add a tool → New
   tool → Model Context Protocol. Server URL is the app's internal FQDN plus `/mcp`;
   auth is OAuth 2.0 with the app registration's client id and secret, the token URL
   repeated as the refresh URL, and a scope of `api://<appId>/rapid7.read`.
3. **Register the redirect URI Entra rejects.** The connector's redirect URI contains a
   per-connector suffix derived from its generated internal name, e.g.
   `https://global.consent.azure-apim.net/redirect/crffc-5frapid7-5f<hash>`. It
   **cannot be known in advance**, so the only way through is to attempt the connection,
   read the exact URI out of the `AADSTS50011` failure, add it to the app registration,
   and retry. Recreating or renaming the connector produces a new one.
4. **Resave the connector if it fails to reach the server.** A connector created before
   VNet injection took effect will not use it, and the symptom is a `serviceUnavailable`
   with nothing arriving at the server at all. Opening the connector and saving it
   unchanged fixes this, and it is the first thing to try — injection can take minutes
   to become effective after the policy is linked.
5. **Turn on generative orchestration** on the agent. Classic orchestration cannot
   invoke MCP tools, and the failure mode is the agent silently ignoring the tool
   rather than reporting anything.

Diagnosing failures: the connector's own error is nearly useless, but the server logs
the exact reason. `az containerapp logs show` names the rejected claim, the HTTP method
and the source address, which is how the issuer mismatch above was found in one read.

## Where users talk to the agent

A single published Copilot Studio agent reaches several Microsoft surfaces at no
extra integration cost. This is a selling point of the Copilot Studio path.

- **Microsoft Teams** — the agent as a chat participant. The agent must be
  **published at least once** before anyone can interact with it.
- **The Microsoft 365 Copilot app** — across web, desktop and Windows. This is the
  **work** Copilot app, signed in with an **Entra (work) account** — **not** the
  consumer Copilot built into Windows 11. They share a name and a look; only the
  work app, on a work account, can reach a Copilot Studio agent. Say this to users
  before they ask, because the confusion otherwise arrives as a support question.
- **SharePoint and Direct Line** — free additional reach from the same agent.
  SharePoint surfaces the agent in that context; Direct Line lets you embed it in a
  custom application. Both are available without further integration work. Setup
  steps are out of scope here — we have not tested them.
- **Agent 365 BYO registration is deliberately not supported.** Its Tooling Gateway
  dials the registered server from a Microsoft-hosted service and therefore requires
  a **publicly accessible endpoint** — which defeats the entire purpose of this
  private deployment. Microsoft publishes no egress IP range, service tag or gateway
  FQDN to allowlist, and offers no Private Link option, so there is no way to keep
  the endpoint private and still register it. Use the Copilot Studio connector path,
  which reaches the server privately from within your tenant.

### Access control

This is the most consequential operational decision in the guide.

**There is no row-level or per-user data filtering.** Any principal who can reach
the agent can read the **whole organisation's dataset**. There is no finer control
inside the server — so the **publishing audience is the access boundary.**

Scope the published agent to **named users or a specific security group** — never
to the whole organisation. If you publish the agent organisation-wide, you have
granted everyone in the tenant full read access to your complete vulnerability
posture, including which systems are unpatched and how. Treat the publishing
audience with the same care you would treat read access to the Rapid7 console
itself, and sign off on it knowingly.

## Conversational caveats

Two behaviours are worth explaining to operators so responses do not surprise them.

- **Query timeout.** A watchdog bounds each query's wall-clock time. The server
  applies no limit by default; the template sets `DUCKDB_QUERY_TIMEOUT_SECONDS` to
  **90 seconds** (parameter `serveQueryTimeoutSeconds`) — comfortably under the
  Copilot Studio tool budget of roughly 100 seconds (the documented 120-second
  connector timeout is the looser of the two, so the design targets the tighter
  one). When a query exceeds the limit it is **cancelled**,
  and the user sees a clear, actionable message rather than a stalled call or a raw
  error:

  > Query cancelled: it ran longer than the 90-second limit. Narrow the query with
  > a more selective WHERE filter or add a LIMIT, then try again.

- **Data-age annotation.** A successful query response carries a freshness line so
  a caller knows how current the answer is:

  > Data last loaded 6 hours ago.

  This is sourced from a load-metadata table inside the database itself, so it
  travels with the artifact and reads correctly in hosted mode. It is fail-soft: if
  the metadata cannot be read the note is simply omitted and the query still
  succeeds. It appears only when the server is serving a Blob artifact; local stdio
  and Docker users loaded the data themselves, so their query output is unchanged.
