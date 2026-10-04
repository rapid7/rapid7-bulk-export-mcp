# Private Azure deployment — Copilot Studio hosting

This package deploys the Rapid7 Bulk Export MCP server to Azure Container Apps
with **no public inbound path**. It replaces the earlier external-ingress
package (PR #15), whose endpoint was reachable from the public internet.

- `main.bicep` — the deployment template.
- `main.example.bicepparam` — example parameters (copy and fill in).

> **Setting this up for the first time?** Follow
> [`SETUP.md`](SETUP.md) — an ordered end-to-end runbook covering Azure, Power
> Platform and Copilot Studio, with every trap recorded at the step where it bites.
> This file documents what the template deploys and why.

## What this deploys

| Resource | Purpose | Public IP? |
|---|---|---|
| Container Apps environment | VNet-injected, `internal: true` | No |
| Container App (serving) | `internal` ingress, **1 replica**, no scale rule | No |
| Container Apps Job (refresh) | scheduled/manual, holds the Rapid7 key | No |
| Blob Storage + container | versioned DB artifacts, reached over HTTPS (not mounted) | No |
| Key Vault | stores the Rapid7 API key (job-only access) | No (`publicNetworkAccess: Disabled`) |
| User-assigned managed identity | Blob (app + job), Key Vault (job) | — |
| Log Analytics | environment diagnostics | — |

No resource in the request path has a public IP. There is **no Front Door, no
WAF, no Private Link origin, and no Azure Files mount** — those either created a
public entry point or (Azure Files) reintroduced the SMB shared-database
corruption path this rewrite exists to remove.

### The three earlier defects, and how they are avoided

1. **Public ingress.** The environment is `internal: true`, which provisions an
   **internal load balancer only** — no public IP exists. The app sets
   `ingress.external: true`, which on an internal environment means "published at
   the internal load balancer", *not* internet-facing; `false` would return HTTP
   404 to every caller in the VNet. Assert on the `environmentIsInternal` output,
   not on the app's ingress flag.
2. **Multi-replica over one shared DB file.** `maxReplicas` is pinned to `1`
   and there is no HTTP scale rule. Single-tenant by design.
3. **`RAPID7_API_KEY` as a `@secure()` param baked into container config.** The
   key lives in Key Vault and is consumed as a secret reference **on the job
   only**; the serving app has no Rapid7 credential in its environment.

### Sizing

- Serving app: **1.25 vCPU / 2.5 GiB** — >1 vCPU reaches the 8 GiB ephemeral
  storage tier the artifact download needs. `DUCKDB_MEMORY_LIMIT=2000MB` sits
  below the 2.5 GiB container ceiling (the shipped default is `4GB`, which a
  2 GiB container cannot honour).
- Refresh job: **2.0 vCPU / 4 GiB** with `DUCKDB_MEMORY_LIMIT=3GB` — more
  headroom for the build role, still >1 vCPU for the 8 GiB tier, DuckDB limit
  below the container memory.

## Prerequisites (customer-provided, gate everything)

- A Power Platform **Managed Environment**.
- Delegated **subnets in the region pair** matching the tenant's Power Platform
  region. The Container Apps infrastructure subnet must be delegated to
  `Microsoft.App/environments`; pass its resource id as `infrastructureSubnetId`.
  The Power Platform connector subnet(s) are delegated to
  `Microsoft.PowerPlatform/enterprisePolicies` instead — a different delegation,
  needed in both regions of the pair.
- **No private endpoint is required** when the connector subnet and the Container
  Apps environment share a virtual network: a same-VNet caller reaches the
  internal load balancer directly. A private endpoint is only for crossing into a
  different, non-injected VNet, and would additionally require a workload-profiles
  environment with a `/27`-or-larger subnet.
- An **Entra app registration exposing a scope** for the server. Create it
  **before** deploying — its values are required parameters (see below).
- **Copilot Studio licensing** sufficient for generative orchestration. Teams-
  plan makers are classic-orchestration-only and **cannot** invoke MCP tools.

## Deploy

### Required tags (check this first)

If your tenant enforces required tags with Azure Policy, **every** resource is
affected — the CLI prerequisites and all three templates. A deployment that omits
them fails at the first resource with `RequestDisallowedByPolicy`, which reads like
a template bug rather than a governance rule:

```
(RequestDisallowedByPolicy) Resource 'vnet-...' was disallowed by policy.
Reasons: '"Owner_Email" tag is required upon resource creation.'
```

Pass them through the `tags` parameter, which is applied to every taggable resource
the templates create. Role assignments and the storage child resources are not
taggable and policy does not apply to them.

```bash
--parameters tags='{"Owner_Email":"you@example.com"}'
```

Discover what your tenant mandates with:

```bash
az policy assignment list --query "[].{name:displayName, scope:scope}" -o table
```

### Which image?

**A released version, from the public registry.** `rapid7/bulk-export-mcp:<version>`
on Docker Hub is anonymously pullable, so leave `containerRegistryName` empty and
no registry credentials are involved:

```bash
az deployment group create \
  --resource-group <rg> \
  --template-file main.bicep \
  --parameters main.example.bicepparam
```

**Unreleased code, from a private registry.** Use this when deploying changes that
are not in a published release yet — the public image will NOT contain them, and a
deployment that appears to succeed will be running different code than you built.
This is a genuine three-step sequence: the registry must exist before the image can
be pushed, and the image must exist before the app can start a healthy revision.

```bash
# 1. Create the registry.
az deployment group create -g <rg> \
  --template-file registry.bicep \
  --parameters registryName=<globally-unique-name>

# 2. Build and push. Use `az acr build`, NOT a local `docker build` — this builds
#    on Azure's amd64 agents. A local build on Apple Silicon produces an arm64
#    image that Container Apps pulls successfully and then fails to start, with an
#    exec-format error that looks nothing like an architecture problem. The build
#    context is the repository root, where the Dockerfile is.
az acr build --registry <registryName> \
  --image rapid7-bulk-export-mcp:dev ../..

# 3. Deploy everything else, naming the registry so the identity gets AcrPull.
az deployment group create -g <rg> \
  --template-file main.bicep \
  --parameters main.example.bicepparam \
    containerRegistryName=<registryName> \
    containerImage=<registryName>.azurecr.io/rapid7-bulk-export-mcp:dev
```

The registry is authenticated with the same user-assigned managed identity used
for Key Vault and Blob, granted **AcrPull** (pull-only). `adminUserEnabled` is
false, so no registry username or password exists. Image pull is an *outbound*
call from the Container Apps subnet and does not make the MCP server reachable
from the internet.

## Post-deployment: set the Rapid7 API key in Key Vault

The key is never a template parameter. Set it out-of-band after deployment; use
a **dedicated, least-privileged key per deployment** so revocation is surgical.

> **This is a deliberate two-pass deployment.** Deploy with
> `rapid7ApiKeyConfigured=false` (the default), set the secret, then redeploy with
> `rapid7ApiKeyConfigured=true`.
>
> Container Apps resolves a Key Vault secret reference when the job is created, by
> actually fetching the secret. The vault is created by this template, so on a
> first deployment the secret cannot exist yet and referencing it fails everything
> with `InvalidParameterValueInContainerTemplate`. The serving app is unaffected —
> it holds no secret at all.
>
> The split also avoids an Azure RBAC propagation race: the identity's Key Vault
> role is assigned in pass 1, so it has propagated by pass 2. Wiring both in one
> pass can fail intermittently even when the secret does exist.

```bash
az keyvault secret set \
  --vault-name <keyVaultName-from-output> \
  --name rapid7-api-key \
  --value '<the-rapid7-api-key>'
```

### Rotation runbook

Rotate entirely in Key Vault — **no redeploy, no rebuild**:

```bash
az keyvault secret set --vault-name <kv> --name rapid7-api-key --value '<new-key>'
```

The next job run resolves the current secret version at runtime. Revoke the old
key in the Rapid7 console. Because each deployment uses its own dedicated key,
revocation affects only this integration, not the whole organisation.

## Post-deployment: Entra app registration (Azure CLI — not Bicep)

**Entra app registrations cannot be provisioned in Bicep.** They are Microsoft
Graph objects, not ARM resources, so this template deliberately does not attempt
them.

> **Do this BEFORE deploying, not after.** `authJwksUri`, `authIssuer` and
> `authAudience` are **required parameters with no defaults**, so the deployment
> cannot run until these values exist. The registration depends on nothing in
> Azure, so there is no ordering conflict — create it first and pass the values in
> on the initial deployment. There is no second "redeploy" pass.

```bash
# 1. Create the app registration and expose an API scope.
appId=$(az ad app create --display-name "rapid7-bulk-export-mcp" --query appId -o tsv)

# 2. Set the Application ID URI (this is the token audience -> authAudience).
az ad app update --id "$appId" --identifier-uris "api://$appId"

# 3. Expose a scope for callers (and the write scope used by authWriteScope).
#    Do this in the portal (Expose an API) or via `az rest` against Graph —
#    `az ad app` has no first-class oauth2PermissionScopes editor.

# 4. These are the three values to pass to the deployment:
tenantId=$(az account show --query tenantId -o tsv)
echo "authAudience = api://$appId"
echo "authIssuer   = https://sts.windows.net/$tenantId/"   # v1 issuer, matches an api:// audience
echo "authJwksUri  = https://login.microsoftonline.com/$tenantId/discovery/v2.0/keys"
```

Grant the managed identity permission to roll a new Container App revision. Note
**no code uses this** — the refresh job publishes an artifact but does not roll a
revision, so this permission is for an operator or pipeline acting as that identity.
See [Known limitations](../../docs/copilot-studio-hosting.md#known-limitations).

Grant the managed identity permission to trigger a new Container App revision
(the artifact flip) — this is intentionally left to the operator so the template
does not hand the identity broad rights at author time:

```bash
az role assignment create \
  --assignee <managedIdentityPrincipalId-from-output> \
  --role "Container Apps Contributor" \
  --scope <serving-container-app-resource-id>
```

## Verification checklist (against the spec acceptance criteria)

- No public IP on any resource in the path (`ingressIsExternal` output is
  `false`; environment `internal: true`).
- No DuckDB file opened from SMB/NFS — Blob is a courier, never a mount.
- Serving replica has no `RAPID7_API_KEY` in its environment.
- Refresh job runs on schedule (or on demand when
  `enableScheduledRefresh = false`) and produces a loadable Blob artifact.
- Rapid7 key rotates in Key Vault with no redeploy.

## Access control (read before publishing)

There is **no row-level security**. The publishing audience *is* the access
boundary — any principal who can reach the agent reads the whole organisation's
dataset. Scope the published agent to named users or groups, never the whole
organisation.
