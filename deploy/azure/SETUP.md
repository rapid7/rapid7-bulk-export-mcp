# Setup runbook: Rapid7 MCP server behind Copilot Studio

An ordered, end-to-end procedure. Every step that cost time during the first
deployment carries a note explaining the trap, because most of them fail in ways
that point at the wrong cause.

For a diagram of the topology these steps build, see
[`../../docs/architecture-diagrams.md`](../../docs/architecture-diagrams.md).

Placeholders, resolved as you go:

| Placeholder | Meaning |
|---|---|
| `<RG>` | Azure resource group |
| `<AZ_REGION>` | Azure region for the Container Apps deployment |
| `<VNET>` | Virtual network holding the Container Apps environment |
| `<TENANT>` | Entra tenant id |
| `<APP_ID>` | Entra application (client) id for the server |
| `<PP_GEO>` | Power Platform **geography** of the environment, e.g. `unitedkingdom` |
| `<PP_REGION>` | Azure region the Power Platform environment was **assigned** |
| `<ENV_ID>` | Power Platform environment GUID |

`<PP_GEO>` and `<PP_REGION>` cannot be chosen — see step 8.

---

## 0. Licensing and capacity

- **Copilot Studio licence, or Microsoft 365 Copilot.** MCP tools require
  **generative** orchestration. A Teams plan alone is *not* sufficient: Teams-plan
  makers are limited to classic orchestration and cannot invoke MCP tools at all.
- **Managed Environments** enforcement needs a premium licence per active user,
  which a Copilot Studio licence satisfies. Pay-as-you-go via an Azure billing
  policy also satisfies it — verified.
- **At least 1 GB of Dataverse database capacity.** Most tenants already have
  this; a tenant with none cannot create the environment at all.

> **Trap.** Pay-as-you-go can fail to unblock capacity in the current admin centre:
> billing policies are scoped to *specific* geographies (`United Kingdom`, `Europe`)
> while environment creation offers only *macro* ones (`Europe & UK`). The picker
> filters by whichever specific region the macro resolves to, and tells you which —
> read that line and create a matching policy, or reclaim prepaid capacity instead.
> Purging a soft-deleted environment releases its allocation immediately.

## 1. Entra app registration — before anything else

Its values are **required template parameters**, so the deployment cannot run
without them.

```bash
APP_ID=$(az ad app create --display-name rapid7-bulk-export-mcp --query appId -o tsv)
az ad app update --id "$APP_ID" --identifier-uris "api://$APP_ID"
TENANT=$(az account show --query tenantId -o tsv)
```

Expose the scopes the server enforces, and add a client secret:

```bash
OID=$(az ad app show --id "$APP_ID" --query id -o tsv)
SID=$(python3 -c "import uuid;print(uuid.uuid4())"); WID=$(python3 -c "import uuid;print(uuid.uuid4())")
az rest --method patch --url "https://graph.microsoft.com/v1.0/applications/$OID" \
  --headers "Content-Type=application/json" \
  --body "{\"api\":{\"oauth2PermissionScopes\":[
    {\"id\":\"$SID\",\"value\":\"rapid7.read\",\"type\":\"User\",\"isEnabled\":true,
     \"adminConsentDisplayName\":\"Query Rapid7 data\",\"adminConsentDescription\":\"Query Rapid7 vulnerability data\",
     \"userConsentDisplayName\":\"Query Rapid7 data\",\"userConsentDescription\":\"Query Rapid7 vulnerability data on your behalf\"},
    {\"id\":\"$WID\",\"value\":\"rapid7.write\",\"type\":\"Admin\",\"isEnabled\":true,
     \"adminConsentDisplayName\":\"Manage Rapid7 exports\",\"adminConsentDescription\":\"Start exports and purge data\"}]}}"
az ad app credential reset --id "$APP_ID" --append --display-name copilot-studio-connector --years 1 --query password -o tsv
```

`rapid7.write` is **Admin**-consent only: it guards the export and purge tools, and
an ordinary user must not be able to grant it to themselves. The secret is shown
once — custom connectors cannot use certificates.

The three auth values for the deployment:

```
authAudience = api://<APP_ID>
authIssuer   = https://sts.windows.net/<TENANT>/        # v1 issuer, trailing slash
authJwksUri  = https://login.microsoftonline.com/<TENANT>/discovery/v2.0/keys
```

> **Trap.** The issuer and audience must come from the **same Entra token version**.
> An app with an Application ID URI issues **v1** tokens by default: issuer
> `sts.windows.net/<tenant>/`, audience `api://<appId>`. A **v2** token instead
> carries the bare client-id GUID as its audience. Pairing the v2 issuer with an
> `api://` audience is a combination Entra never issues, and every token is
> rejected. See [`../docs/authentication.md`](../docs/authentication.md).

## 2. Network

Two subnets in the same virtual network, with different requirements:

```bash
az network vnet create -g <RG> -n <VNET> --address-prefix 10.0.0.0/16 --tags <required-tags>
az network vnet subnet create -g <RG> --vnet-name <VNET> -n snet-cae \
  --address-prefix 10.0.0.0/23 --delegations Microsoft.App/environments
az network vnet subnet create -g <RG> --vnet-name <VNET> -n snet-pe \
  --address-prefix 10.0.3.0/24
```

`snet-cae` must be **/23 or larger** and delegated to Container Apps. `snet-pe`
must **not** be delegated — private endpoints cannot occupy a delegated subnet.

> **Trap.** If the tenant enforces required tags with Azure Policy, every resource
> and both templates need them, and the failure (`RequestDisallowedByPolicy`) reads
> like a template bug. Discover them with
> `az policy assignment list --query "[].displayName" -o table`.

## 3. Container image

For a released version, `rapid7/bulk-export-mcp:<version>` on Docker Hub is
anonymously pullable and needs no registry configuration.

For **unreleased** code, build into a private registry — the published image is
built from a release tag and will silently run different code than you tested:

```bash
az deployment group create -g <RG> --template-file registry.bicep --parameters registryName=<acr>
az acr build --registry <acr> --image rapid7-bulk-export-mcp:dev .
```

> **Trap.** Use `az acr build`, not a local `docker build`. It builds on Azure's
> amd64 agents; a local build on Apple Silicon produces arm64, which Container Apps
> pulls successfully and then fails to start with an exec-format error.

## 4. Deploy — first pass, without the Rapid7 key

```bash
az deployment group create -g <RG> --name r7mcp --template-file main.bicep \
  --parameters containerImage=<image> \
    infrastructureSubnetId=<snet-cae id> privateEndpointSubnetId=<snet-pe id> \
    containerRegistryName=<acr-or-omit> \
    authAudience="api://$APP_ID" \
    authIssuer="https://sts.windows.net/$TENANT/" \
    authJwksUri="https://login.microsoftonline.com/$TENANT/discovery/v2.0/keys" \
    keyVaultOfficerPrincipalId=$(az ad signed-in-user show --query id -o tsv) \
    operatorIpAddress=$(curl -s ifconfig.me) \
    rapid7ApiKeyConfigured=false \
    tags=<required-tags-json>
```

> **Trap.** `rapid7ApiKeyConfigured=false` is required on a first deployment.
> Container Apps resolves a Key Vault secret reference by *fetching* it when the job
> is created, and this template creates the vault — so the secret cannot exist yet
> and referencing it fails the entire deployment.

> **Trap.** `keyVaultOfficerPrincipalId` and `operatorIpAddress` are not optional in
> practice. The vault uses RBAC authorization, so Owner on the subscription does
> **not** permit writing a secret; and the vault denies public traffic, so your own
> address must be allowlisted or you cannot reach its data plane at all.

## 5. Set the key, then deploy again

```bash
az keyvault secret set --vault-name <keyVaultName output> --name rapid7-api-key \
  --value "$(op read 'op://<vault>/<item>/password')"
# then re-run step 4 with rapid7ApiKeyConfigured=true
```

The second pass also avoids an RBAC propagation race: the identity's vault role was
assigned in pass 1, so it has propagated by now.

## 6. Produce the first artifact

**A fresh deployment has no data until the refresh job has run.** Only the job
publishes an artifact, so the serving app starts empty, logging *"No artifact has
been published yet; starting with no data"*, and its read tools reply that no data
is loaded. Trigger the first run by hand rather than waiting for the schedule — see
[Known limitations](../../docs/copilot-studio-hosting.md#known-limitations):

```bash
az containerapp job start -g <RG> -n <job>
az containerapp job logs show -g <RG> -n <job> --container mcp-refresh --follow --tail 100 --format text
```

Wait for `published artifact version: …` — roughly 6–10 minutes, most of it waiting
on Rapid7. Then restart the app so it downloads the artifact.

> **Trap.** Blob Storage and Key Vault report a *network* denial as an
> **authorization** error (`AuthorizationFailure` / `Forbidden`), so a missing or
> misnamed private DNS zone looks exactly like a missing RBAC role. Before
> investigating permissions, resolve the account name from inside the VNet and
> confirm it returns a private address.

## 7. Verify the private path before involving Power Platform

```bash
az network vnet subnet create -g <RG> --vnet-name <VNET> -n snet-probe \
  --address-prefix 10.0.2.0/24 --delegations Microsoft.ContainerInstance/containerGroups
az container create -g <RG> -n probe -l <AZ_REGION> --image curlimages/curl:latest \
  --vnet <VNET> --subnet snet-probe --restart-policy Never --os-type Linux --cpu 0.5 --memory 0.5 \
  --command-line "curl -sS -o /dev/null -w 'HTTP=%{http_code} tls=%{ssl_verify_result} ip=%{remote_ip}\n' --max-time 25 https://<fqdn>/mcp"
```

**`HTTP=401` is the pass** — it proves DNS, TLS, routing and authentication all work
in a single response. Not 200.

> **Trap.** `000` with `tls=0` and a populated `ip=` is a **timeout, not a TLS
> failure** — it means no listener, usually a replica that exited because it could
> not download the artifact. A blank `ip=` is DNS. `404` means the app's
> `ingress.external` is `false`, which on an internal environment hides it from the
> whole VNet rather than making it more private.

## 8. Power Platform environment — and discovering its region

Create a **Sandbox** environment with **Dataverse**, **Managed Environments on**,
and **Dynamics 365 apps off** (irreversible, and not needed).

**You cannot choose the Azure region.** The form takes a *macro* geography and
assigns a specific region by capacity. Read back what you actually got:

```bash
az rest --method get --resource "https://api.bap.microsoft.com/" \
  --url "https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?api-version=2020-10-01" \
  -o json > /tmp/e.json
python3 -c "
import json
for e in json.load(open('/tmp/e.json')).get('value',[]):
    p=e.get('properties',{})
    print(p.get('displayName'),'| id',e.get('name'),'| geo',e.get('location'),
          '| region',p.get('azureRegion'),'| managed',p.get('governanceConfiguration',{}).get('protectionLevel'))
"
```

`geo` is `<PP_GEO>` — the exact string the enterprise policy needs. `azureRegion` is
`<PP_REGION>`, which determines whether step 9 is trivial or not.

## 9. Connector subnet

**If `<PP_REGION>` matches `<AZ_REGION>`** — add one subnet to the existing VNet and
you are done:

```bash
az network vnet subnet create -g <RG> --vnet-name <VNET> -n snet-pp \
  --address-prefix 10.0.4.0/24 --delegations Microsoft.PowerPlatform/enterprisePolicies
```

**If it differs** — the subnet must be in `<PP_REGION>`, so it needs its own VNet,
peering, **and** a private DNS zone link:

```bash
az network vnet create -g <RG> -n <VNET_PP> -l <PP_REGION> --address-prefix 10.2.0.0/16 --tags <tags>
az network vnet subnet create -g <RG> --vnet-name <VNET_PP> -n snet-pp \
  --address-prefix 10.2.1.0/24 --delegations Microsoft.PowerPlatform/enterprisePolicies
az network vnet peering create -g <RG> -n a-to-b --vnet-name <VNET> --remote-vnet <VNET_PP id> --allow-vnet-access
az network vnet peering create -g <RG> -n b-to-a --vnet-name <VNET_PP> --remote-vnet <VNET id> --allow-vnet-access
ZONE=$(az network private-dns zone list -g <RG> --query "[?ends_with(name,'azurecontainerapps.io')].name" -o tsv)
az network private-dns link vnet create -g <RG> -z "$ZONE" -n link-pp \
  --virtual-network <VNET_PP id> --registration-enabled false
```

> **Trap.** **Peering gives connectivity, not name resolution.** Without the DNS
> zone link the connector reaches the network and cannot resolve the FQDN, which
> presents as a timeout rather than a DNS error. This is the single most likely
> mistake in a cross-region setup.

Note that a probe container cannot be placed in `snet-pp` — it is delegated — so use
a separate subnet delegated to `Microsoft.ContainerInstance/containerGroups` in the
same VNet if you want to test reachability from that region.

## 10. Enterprise policy

```bash
az provider register --namespace Microsoft.PowerPlatform --wait
az deployment group create -g <RG> --template-file powerplatform-policy.bicep \
  --parameters policyName=<name> geography=<PP_GEO> \
    primaryVnetId=<vnet holding snet-pp> primarySubnetName=snet-pp \
    tags=<tags> --query "properties.outputs.policyArmId.value" -o tsv
```

Add `pairedVnetId`/`pairedSubnetName` only if the geography requires two regions;
try primary-only first.

> **Trap.** `geography` is a Power Platform geography, **not** an Azure region, and a
> mismatch is silent — the policy simply never appears in the picker.

Link it: admin centre → **Security** → *Data and privacy* → **Azure Virtual Network
policies** → select the environment → choose the policy → Save. Only Managed
Environments are listed. Confirm it took:

```bash
az rest --method get --resource "https://api.bap.microsoft.com/" \
  --url "https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments/<ENV_ID>?api-version=2020-10-01" \
  --query "properties.enterprisePolicies.vNets.linkStatus" -o tsv
```

Expect `Linked`.

## 11. Custom connector

Copilot Studio → **Tools → Add a tool → New tool → Model Context Protocol**:

| Field | Value |
|---|---|
| Server URL | `https://<fqdn>/mcp` |
| Auth | OAuth 2.0 → Manual |
| Client ID / secret | from step 1 |
| Authorization URL | `https://login.microsoftonline.com/<TENANT>/oauth2/v2.0/authorize` |
| Token URL **and** Refresh URL | `https://login.microsoftonline.com/<TENANT>/oauth2/v2.0/token` |
| Scope | `api://<APP_ID>/rapid7.read` |

> **Trap.** The redirect URI carries a **per-connector suffix** derived from the
> connector's generated internal name, e.g.
> `https://global.consent.azure-apim.net/redirect/cr…-5f<hash>`. It cannot be known
> in advance. Attempt the connection, read the exact URI from the `AADSTS50011`
> failure, add it to the app registration, and retry. Recreating or renaming the
> connector produces a new one.

> **Trap.** A connector created before VNet injection took effect will not use it.
> The symptom is `serviceUnavailable` with **nothing arriving at the server at all**.
> Open the connector and save it unchanged — this is the first thing to try, and
> injection can take several minutes to become effective after linking.

> **Trap.** If connections work and then fail about an hour later, add
> `offline_access` to the scope field so Entra issues a refresh token.

## 12. Agent

Create the agent, **turn generative orchestration ON**, add the connector as a tool.
Classic orchestration cannot invoke MCP tools, and the failure mode is the agent
silently ignoring the tool rather than reporting anything.

## 13. Verify scope enforcement before publishing

Ask the agent what Rapid7 tools it has. With a `rapid7.read` connection it must list
only the read tools — `query_rapid7`, `get_rapid7_schema`, `get_rapid7_stats`,
`list_rapid7_exports`, `check_rapid7_export_status` — and **none** of
`start_rapid7_export`, `download_rapid7_export`, `load_rapid7_parquet`,
`purge_rapid7_data`.

To confirm the filter is genuinely scope-driven rather than the write tools being
inert, temporarily set `MCP_AUTH_WRITE_SCOPE` to a scope the token already holds and
confirm they appear, then revert. Do not invoke them during that window.

## 14. Publish

Publish the agent, add the Teams or Microsoft 365 Copilot channel, and **scope the
publishing audience to named users or groups**. There is no per-user data filtering:
anyone who can chat with the agent can query the whole dataset, and the audience is
the access boundary.

---

## Diagnosing anything that goes wrong

**Read the server's logs, not the connector's error.** The connector reports generic
HTTP statuses; the server names the exact cause.

```bash
az containerapp logs show -g <RG> -n <app> --tail 40
```

That is how the issuer mismatch above was found — one line naming the claim, the
expected value and the received value. A connector error of `unauthorized` or
`serviceUnavailable` says nothing about which, and whether a request arrived at all
is the most useful single fact when diagnosing the network path.
