// =============================================================================
// Private hosting for Microsoft Copilot Studio — Azure deployment package
// =============================================================================
//
//
// This template provisions a PRIVATE deployment with NO public inbound path:
//   * Container App with INTERNAL ingress only — no public IP anywhere.
//   * The Power Platform connector runtime reaches it from a customer-delegated
//     subnet via a private endpoint (the subnet/PE are customer-owned and are
//     NOT provisioned here — see the prerequisites in README.md).
//   * Blob Storage (reached over HTTPS, NOT mounted) for versioned DB artifacts.
//   * A user-assigned managed identity + Key Vault, with the Rapid7 key consumed
//     as a Key Vault secret reference ON THE REFRESH JOB ONLY. The serving app
//     carries no Rapid7 credential (design R8: separation is the primary control).
//
// Three defects from the earlier template are deliberately NOT reproduced:
//   1. external ingress            -> internal ingress, no public IP.
//   2. maxReplicas=3 + HTTP scale rule over one shared DuckDB file
//                                  -> pinned to a single replica, no scale rule.
//   3. RAPID7_API_KEY as a @secure() Bicep param materialised into container
//      config -> Key Vault secret reference on the job's managed identity.
//
// This template authors resources only. Entra app registrations CANNOT be
// provisioned in Bicep and require the Azure CLI steps documented after the
// deployment (see README.md "Post-deployment: Entra app registration").
// =============================================================================

targetScope = 'resourceGroup'

// -----------------------------------------------------------------------------
// Parameters
// -----------------------------------------------------------------------------

@description('Base name used to derive resource names. Lowercase letters and digits.')
@minLength(3)
@maxLength(20)
param namePrefix string = 'r7bulk'

@description('Azure region for all resources. Defaults to the resource group location.')
param location string = resourceGroup().location

@description('Container image reference, e.g. myregistry.azurecr.io/rapid7-bulk-export-mcp:0.6.0')
param containerImage string

// --- Networking (private only) ----------------------------------------------
// The customer supplies an existing Container Apps environment infrastructure
// subnet (delegated to Microsoft.App/environments) so the environment is
// VNet-injected and internal-only. We take its resource id rather than creating
// the VNet, because subnet delegation and the private endpoint live in the
// customer's network and are out of this template's scope.
@description('Resource id of the existing subnet delegated to Microsoft.App/environments for the Container Apps environment (customer-provided).')
param infrastructureSubnetId string

@description('''Resource id of a subnet for the Blob private endpoint. Must be in the SAME
virtual network as the infrastructure subnet, and must NOT be delegated — private endpoints
cannot live in a delegated subnet, so this cannot be the Container Apps infrastructure subnet.

Required because the storage account sets publicNetworkAccess: 'Disabled'. Without a private
endpoint the replicas cannot reach Blob, and the failure appears as AuthorizationFailure —
indistinguishable at a glance from a missing RBAC role.''')
param privateEndpointSubnetId string

@description('''Object id of the principal that will WRITE the Rapid7 secret into Key Vault —
normally the person or service principal running the deployment. Granted Key Vault Secrets
Officer on the vault.

The vault uses RBAC authorization, so Owner or Contributor on the subscription does NOT allow
writing a secret: `az keyvault secret set` fails with ForbiddenByRbac. Get it with
`az ad signed-in-user show --query id -o tsv`. Leave empty if secrets are provisioned by a
separate process that already holds the role.''')
param keyVaultOfficerPrincipalId string = ''

@description('''Public IP address allowed to reach the Key Vault data plane, so an operator can
set the Rapid7 secret. The vault denies all other public traffic and the workload path is a
private endpoint, so leaving this empty makes the vault unreachable from outside the VNet —
including by you. Get it with `curl -s ifconfig.me`.''')
param operatorIpAddress string = ''

// --- Auth (inbound bearer-token validation; see src/auth.py) -----------------
// These map 1:1 to the environment variables src/auth.py reads at startup.
// build_auth() fails closed: the HTTP transport refuses to start unless the
// JWKS URI + issuer + audience are all present.
@description('OIDC JWKS URI the server validates inbound bearer tokens against (MCP_AUTH_JWKS_URI). Taken from the IdP console.')
param authJwksUri string

@description('Accepted token issuer(s), comma-separated to accept several IdPs (MCP_AUTH_ISSUER).')
param authIssuer string

@description('Audience this server is registered as (MCP_AUTH_AUDIENCE).')
param authAudience string

@description('Optional comma-separated required scopes for any authenticated call (MCP_AUTH_REQUIRED_SCOPES). Empty to require none.')
param authRequiredScopes string = ''

@description('Scope that gates the write tools (MCP_AUTH_WRITE_SCOPE). The server defaults this to rapid7.write.')
param authWriteScope string = 'rapid7.write'

// --- Rapid7 (job only) -------------------------------------------------------
@description('Rapid7 region for the export endpoint (RAPID7_REGION): us, us2, us3, eu, ca, au, ap.')
@allowed([
  'us'
  'us2'
  'us3'
  'eu'
  'ca'
  'au'
  'ap'
])
param rapid7Region string = 'us'

// The Rapid7 API key is NEVER a @secure() Bicep parameter (defect #3). It is
// placed directly in Key Vault out-of-band (Azure CLI, see README.md) and
// referenced by the job at runtime. The template only names the secret.
@description('Name of the Key Vault secret holding the Rapid7 API key. Its VALUE is set out-of-band, never through this template.')
param rapid7ApiKeySecretName string = 'rapid7-api-key'

// --- Refresh job schedule (optional / disable-able) --------------------------
@description('Enable the scheduled refresh trigger. When false, the job is deployed as a Manual job an operator can run on demand.')
param enableScheduledRefresh bool = true

@description('Cron schedule for the refresh job (UTC). Ignored when enableScheduledRefresh is false. Default: 02:00 daily.')
param refreshCronExpression string = '0 2 * * *'

@description('''Comma-separated export types the refresh job builds. Each becomes a separate
`--type <value>` argument, because the CLI option is repeatable and validated against a fixed
choice list — passing a comma-joined string as one value fails validation. Leave empty to let
the CLI default to all snapshot types plus remediation.

Valid values: vulnerability, policy, asset_software, remediation.''')
param refreshExportTypes string = 'vulnerability'

@description('''How many complete artifact versions to keep in Blob. Every refresh publishes a
whole new database, so without pruning the container grows by one copy per run forever, holding
data nothing ever reads, and becomes the largest cost in the deployment.

Retention is by COUNT rather than age deliberately. A storage lifecycle policy cannot express
"keep the newest N", and an age rule would delete the last good artifact if refreshes failed for
longer than the threshold, leaving a replica nothing to serve.''')
@minValue(1)
param artifactRetainVersions int = 3

// --- DuckDB tuning -----------------------------------------------------------
// db_utils.py defaults memory_limit to '4GB' unless DUCKDB_MEMORY_LIMIT
// overrides it. The serving container below is 2.5Gi, so we MUST set the DuckDB
// limit BELOW that ceiling (defect #2). Kept in a deliberate relationship, not
// left to drift: 2000MB DuckDB inside a 2.5Gi container.
@description('DuckDB memory limit for the serving replica (DUCKDB_MEMORY_LIMIT). MUST stay below the serving container memory.')
param serveDuckdbMemoryLimit string = '2000MB'

@description('DuckDB memory limit for the refresh job (DUCKDB_MEMORY_LIMIT). MUST stay below the job container memory.')
param jobDuckdbMemoryLimit string = '3GB'

// The server applies no query limit unless one is set. Copilot Studio kills a
// tool call after roughly 100 seconds, so cancel below that and return a
// message the user can act on instead of a platform timeout.
@description('Query time limit in seconds for the serving replica (DUCKDB_QUERY_TIMEOUT_SECONDS). Keep below the ~100-second Copilot Studio tool budget.')
@minValue(1)
@maxValue(99)
param serveQueryTimeoutSeconds int = 90

@description('Log Analytics data retention in days.')
@minValue(30)
@maxValue(730)
param logRetentionDays int = 30

@description('''Whether the Rapid7 API key secret already EXISTS in the Key Vault this template
creates. Leave false on the FIRST deployment.

Container Apps resolves a Key Vault secret reference when the job is created, by actually
fetching the secret — so referencing a secret that does not exist yet fails the whole
deployment with InvalidParameterValueInContainerTemplate. The vault is created by this
template, so on a first deployment the secret cannot possibly exist yet.

Sequence:
  1. deploy with this false (serving app is fully functional; refresh job has no credential)
  2. az keyvault secret set --vault-name <keyVaultName output> --name rapid7-api-key --value '<key>'
  3. redeploy with this true to wire the credential into the refresh job

The second pass also avoids an Azure RBAC propagation race: the identity's Key Vault role is
assigned in pass 1, so by pass 2 it has propagated. Wiring both in one pass can fail
intermittently even when the secret does exist.''')
param rapid7ApiKeyConfigured bool = false

@description('''Tags applied to every taggable resource this template creates.

Many enterprise tenants enforce required tags with Azure Policy — a deployment that
omits them is rejected with RequestDisallowedByPolicy at the first resource, which
looks like a template bug rather than a governance rule. Pass whatever your tenant
requires, e.g. { Owner_Email: 'you@example.com' }.

Role assignments and the storage child resources are not taggable and are therefore
untagged; policy does not apply to them.''')
param tags object = {}

@description('''Name of an Azure Container Registry holding the image, in this resource group.
Set this when `containerImage` is in a PRIVATE registry: it adds the registry to both the app
and the job and grants the managed identity AcrPull. Leave empty for a publicly pullable image.

Without it a private image fails to pull, and the failure surfaces as an unhealthy revision
rather than a deployment error. Deploy registry.bicep and push the image first — see the
ordering note in that file.''')
param containerRegistryName string = ''

@description('''Create a private DNS zone for the Container Apps environment and link it to
the virtual network holding the infrastructure subnet. Required for anything in the VNet to
resolve the app's FQDN — internal environments do NOT get a zone automatically. Set to false
only if the zone is managed centrally (hub-and-spoke tenants often provision it by policy);
in that case create a zone named after the `environmentDefaultDomain` output with a wildcard
A record pointing at `environmentStaticIp`.''')
param createPrivateDnsZone bool = true

// -----------------------------------------------------------------------------
// Naming
// -----------------------------------------------------------------------------

var suffix = uniqueString(resourceGroup().id, namePrefix)
var logAnalyticsName = '${namePrefix}-law-${suffix}'
var identityName = '${namePrefix}-uami-${suffix}'
var keyVaultName = take('${namePrefix}kv${suffix}', 24)
var storageAccountName = take('${namePrefix}st${suffix}', 24)
var artifactContainerName = 'db-artifacts'
var environmentName = '${namePrefix}-cae-${suffix}'
var serveAppName = '${namePrefix}-app'
var refreshJobName = '${namePrefix}-refresh'

// Serving container resources. >1 vCPU (1.25) reaches the 8 GiB ephemeral tier,
// which the artifact download + read-only DuckDB open needs (design R2/storage
// section). Memory 2.5Gi sits above the 2000MB DuckDB limit above.
var serveCpu = json('1.25')
var serveMemory = '2.5Gi'

// Job build role gets more headroom than the serving role: it constructs the
// database from scratch. Still >1 vCPU for the 8 GiB ephemeral tier.
var jobCpu = json('2.0')
var jobMemory = '4Gi'

// Key Vault secret reference name used inside the Container Apps Job's secrets
// collection. The job env var RAPID7_API_KEY points at this.
var jobKvSecretRef = 'rapid7-api-key'

// The CLI exposes `--type` (singular) as a REPEATABLE option validated against a
// fixed choice list, so one comma-joined value is rejected. Expand the parameter
// into one flag per type. An empty parameter yields no arguments, letting the CLI
// apply its own default of all snapshot types plus remediation.
var refreshTypeList = empty(trim(refreshExportTypes)) ? [] : split(replace(refreshExportTypes, ' ', ''), ',')
var refreshArgs = flatten(map(refreshTypeList, t => ['--type', t]))

// -----------------------------------------------------------------------------
// Log Analytics (Container Apps environment diagnostics)
// -----------------------------------------------------------------------------

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsName
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: logRetentionDays
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
  }
}

// -----------------------------------------------------------------------------
// User-assigned managed identity
// -----------------------------------------------------------------------------
// One identity carries two capabilities for the JOB:
//   * Key Vault 'get' on the Rapid7 secret (secret user role assignment below).
//   * (documented) permission to trigger a new Container Apps revision for the
//     artifact flip — granted via a Contributor-scoped role on the app in a
//     real deployment; left to the operator here so this template does not
//     hand the identity broad rights it does not need at author time.
// The SERVING app also uses this identity, but ONLY to read Blob artifacts over
// HTTPS. It is never granted the Key Vault secret, so it holds no Rapid7 path.

resource uami 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
  tags: tags
}

// -----------------------------------------------------------------------------
// Key Vault (RBAC authorization) — Rapid7 key lives here, job-only access
// -----------------------------------------------------------------------------

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true
    // The workload path is the private endpoint above. The public endpoint stays
    // enabled ONLY so an operator IP can be allowlisted to set the secret —
    // defaultAction is Deny, so with no operatorIpAddress supplied nothing on the
    // internet can reach it. Fully disabling public access would also lock out
    // the human who has to write the secret, since the vault is unreachable from
    // outside the VNet.
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
      ipRules: empty(operatorIpAddress) ? [] : [
        {
          value: operatorIpAddress
        }
      ]
    }
  }
}

// Built-in role: Key Vault Secrets User (read secret values).
var keyVaultSecretsUserRoleId = '4633458b-17de-408a-b874-0445c86b69e6'
// Built-in role: Key Vault Secrets Officer (WRITE secret values).
var keyVaultSecretsOfficerRoleId = 'b86a8fe4-44ce-4948-aee5-eccb2c155cd7'

resource kvSecretUserAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, uami.id, keyVaultSecretsUserRoleId)
  scope: keyVault
  properties: {
    principalId: uami.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsUserRoleId)
  }
}

// -----------------------------------------------------------------------------
// Storage account + Blob container for versioned DB artifacts
// -----------------------------------------------------------------------------
// Blob is a COURIER, not a filesystem: Container Apps cannot mount Blob, so the
// job uploads the finished .db here over HTTPS and the serving replica downloads
// it to local ephemeral disk. There is deliberately NO Azure Files share and NO
// storage/volume link on the environment (defect: the shared SMB mount).

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false // force managed-identity/AAD auth, no account keys
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Disabled'
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
    }
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
}

resource artifactContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: artifactContainerName
  properties: {
    publicAccess: 'None'
  }
}

// -----------------------------------------------------------------------------
// Private endpoints for Blob and Key Vault
//
// Both services set publicNetworkAccess 'Disabled' / defaultAction 'Deny', so
// WITHOUT these the workloads cannot reach them at all — and both report the
// failure as an authorization error rather than a network one, which points at
// identity instead of networking. Observed symptoms were "could not download the
// current artifact: This request is not authorized" (Blob) and a job that cannot
// resolve its Key Vault secret reference.
//
// These are NOT the private endpoint the hosting guide says is unnecessary. That
// one was for reaching the Container App from the same VNet. These are the
// workloads reaching storage and the vault — the opposite direction.
// -----------------------------------------------------------------------------

module blobPrivateEndpoint 'modules/private-endpoint.bicep' = {
  name: 'pe-blob-${suffix}'
  params: {
    name: '${namePrefix}-pe-blob-${suffix}'
    location: location
    subnetId: privateEndpointSubnetId
    targetResourceId: storageAccount.id
    groupId: 'blob'
    // NOTE THE DOT after 'blob'. az.environment().suffixes.storage is
    // 'core.windows.net' with NO leading dot — unlike suffixes.keyvaultDns and
    // suffixes.acrLoginServer, which DO carry one. Omitting it silently yields
    // 'privatelink.blobcore.windows.net', and Azure does not validate that a private
    // DNS zone group's zone matches the sub-resource: it creates records in the
    // nonsense zone and reports success. The account name then resolves to its
    // PUBLIC address, and the storage firewall rejects the request as
    // AuthorizationFailure — an error that reads as a missing RBAC role.
    privateDnsZoneName: 'privatelink.blob.${az.environment().suffixes.storage}'
    vnetResourceId: vnetResourceId
    tags: tags
  }
}

module keyVaultPrivateEndpoint 'modules/private-endpoint.bicep' = {
  name: 'pe-kv-${suffix}'
  params: {
    name: '${namePrefix}-pe-kv-${suffix}'
    location: location
    subnetId: privateEndpointSubnetId
    targetResourceId: keyVault.id
    groupId: 'vault'
    // Literal for the same reason as the blob zone above. Key Vault's private-link
    // zone is vaultcore, NOT the public vault.azure.net suffix, so it could not be
    // derived even if deriving were safe. Sovereign clouds differ.
    privateDnsZoneName: 'privatelink.vaultcore.azure.net'
    vnetResourceId: vnetResourceId
    tags: tags
  }
}

// The vault uses RBAC authorization, so Owner or Contributor on the subscription
// does NOT grant data-plane access: creating the vault and writing a secret are
// different permissions. Without this the operator's own `az keyvault secret set`
// fails with ForbiddenByRbac and "Assignment: (not found)".
resource kvSecretOfficerAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(keyVaultOfficerPrincipalId)) {
  name: guid(keyVault.id, keyVaultOfficerPrincipalId, keyVaultSecretsOfficerRoleId)
  scope: keyVault
  properties: {
    principalId: keyVaultOfficerPrincipalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsOfficerRoleId)
  }
}

// Built-in role: Storage Blob Data Contributor (job writes, server reads).
// The serving app strictly only needs Reader, but the two roles are granted on
// the same identity here; a hardened deployment can split identities. Job needs
// write, server needs read — Contributor covers both on the shared identity.
var blobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource blobDataAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, uami.id, blobDataContributorRoleId)
  scope: storageAccount
  properties: {
    principalId: uami.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', blobDataContributorRoleId)
  }
}

// -----------------------------------------------------------------------------
// Private container registry access (optional)
//
// Only wired up when containerRegistryName is supplied. Both the serving app and
// the refresh job authenticate to the registry with the SAME user-assigned
// identity that already reaches Key Vault and Blob — no registry username or
// password exists anywhere in this template.
//
// AcrPull is pull-only: neither workload can push or delete an image.
// -----------------------------------------------------------------------------

var useRegistry = !empty(containerRegistryName)
var acrPullRoleId = '7f951dda-4ed3-4680-a7ca-43fe172d538d'
// `az.` prefix is required: this template has a resource symbol named
// `environment` (the managed environment), which shadows the environment()
// function. Using the suffix rather than hardcoding .azurecr.io keeps this
// correct in sovereign clouds.
var acrLoginServer = '${containerRegistryName}${az.environment().suffixes.acrLoginServer}'

resource containerRegistry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: useRegistry ? containerRegistryName : 'placeholder'
}

resource acrPullAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (useRegistry) {
  name: guid(containerRegistry.id, uami.id, acrPullRoleId)
  scope: containerRegistry
  properties: {
    principalId: uami.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', acrPullRoleId)
  }
}

var registryConfig = useRegistry
  ? [
      {
        server: acrLoginServer
        identity: uami.id
      }
    ]
  : []

// -----------------------------------------------------------------------------
// Container Apps environment — VNet-injected, internal only
// -----------------------------------------------------------------------------
// internal: true is the environment-level switch that gives the environment an
// internal-only load balancer (no public static IP). Combined with the app's
// ingress.external: false below, there is no public inbound path anywhere.

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: location
  tags: tags
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
    vnetConfiguration: {
      internal: true // internal-only environment: no public IP
      infrastructureSubnetId: infrastructureSubnetId
    }
  }
}

// -----------------------------------------------------------------------------
// Private DNS for the internal environment
//
// An internal Container Apps environment does NOT get a private DNS zone
// automatically, so without this nothing in the virtual network can resolve the
// app's FQDN — the request fails before it ever reaches the load balancer.
//
// The zone is named after the environment's own defaultDomain (NOT a
// `privatelink.*` zone — that one belongs to the private-endpoint topology,
// which a same-VNet caller does not need). It has to go through a module
// because a resource name must be computable at the start of the deployment and
// defaultDomain is not; see the comment in modules/private-dns.bicep.
//
// The VNet id is derived from the infrastructure subnet id so the caller does
// not have to pass the same network twice.
// -----------------------------------------------------------------------------

var vnetResourceId = substring(infrastructureSubnetId, 0, indexOf(infrastructureSubnetId, '/subnets/'))

module privateDns 'modules/private-dns.bicep' = if (createPrivateDnsZone) {
  name: 'private-dns-${suffix}'
  params: {
    zoneName: environment.properties.defaultDomain
    vnetResourceId: vnetResourceId
    environmentStaticIp: environment.properties.staticIp
    linkName: 'link-${environmentName}'
    tags: tags
  }
}

// -----------------------------------------------------------------------------
// Serving Container App — VNet-internal only, single replica, no Rapid7 key
// -----------------------------------------------------------------------------

resource serveApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: serveAppName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${uami.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      // ---------------------------------------------------------------------
      // DO NOT change `external` to false. It does NOT mean "internet-facing".
      //
      // `external` is scoped to the ENVIRONMENT, not to the internet. What
      // removes the public IP is `internal: true` on the managed environment
      // above: an internal environment is provisioned with an internal load
      // balancer ONLY and has no public IP at all.
      //
      //   internal env + external: true   -> published on the INTERNAL load
      //                                      balancer. Reachable from the
      //                                      virtual network. NOT reachable
      //                                      from the public internet.
      //   internal env + external: false  -> reachable ONLY by other container
      //                                      apps inside this same environment.
      //                                      Everything else in the VNet — the
      //                                      Power Platform connector runtime
      //                                      included — receives HTTP 404.
      //
      // Microsoft's guidance for this exact topology: "After you create an
      // internal environment, set each app's ingress to external so that
      // clients in your virtual network can reach it."
      // https://learn.microsoft.com/en-us/azure/container-apps/ingress-overview
      //
      // The `environmentIsInternal` output asserts the property that actually
      // matters for the no-public-endpoint requirement.
      // ---------------------------------------------------------------------
      ingress: {
        external: true
        transport: 'http'
        targetPort: 8000
        allowInsecure: false
      }
      registries: registryConfig
    }
    template: {
      containers: [
        {
          name: 'mcp-server'
          image: containerImage
          resources: {
            cpu: serveCpu
            memory: serveMemory
          }
          env: [
            // Remote HTTP transport (see src/mcp_server.py main()).
            {
              name: 'MCP_TRANSPORT'
              value: 'http'
            }
            {
              name: 'MCP_HOST'
              value: '0.0.0.0'
            }
            {
              name: 'MCP_PORT'
              value: '8000'
            }
            // Inbound auth — fail-closed wiring from src/auth.py.
            {
              name: 'MCP_AUTH_JWKS_URI'
              value: authJwksUri
            }
            {
              name: 'MCP_AUTH_ISSUER'
              value: authIssuer
            }
            {
              name: 'MCP_AUTH_AUDIENCE'
              value: authAudience
            }
            {
              name: 'MCP_AUTH_REQUIRED_SCOPES'
              value: authRequiredScopes
            }
            {
              name: 'MCP_AUTH_WRITE_SCOPE'
              value: authWriteScope
            }
            // DuckDB limit kept deliberately BELOW the 2.5Gi container memory
            // (defect #2). Do not raise this to the shipped 4GB default here.
            {
              name: 'DUCKDB_MEMORY_LIMIT'
              value: serveDuckdbMemoryLimit
            }
            {
              name: 'DUCKDB_QUERY_TIMEOUT_SECONDS'
              value: string(serveQueryTimeoutSeconds)
            }
            // Blob artifact source (read over HTTPS to local ephemeral disk).
            {
              name: 'ARTIFACT_BLOB_ACCOUNT_URL'
              value: storageAccount.properties.primaryEndpoints.blob
            }
            {
              name: 'ARTIFACT_BLOB_CONTAINER'
              value: artifactContainerName
            }
            // Identity the SDK uses to read Blob. This app is never granted the
            // Key Vault Rapid7 secret, so it holds NO Rapid7 credential — the
            // separation property is the whole point of R8.
            {
              name: 'AZURE_CLIENT_ID'
              value: uami.properties.clientId
            }
          ]
        }
      ]
      // SINGLE-TENANT DESIGN: pinned to exactly one replica and NO HTTP scale
      // rule (defect #2). Each replica holds its own private read-only artifact
      // copy, so this is a cost/simplicity choice, not a correctness one — but
      // for a single-tenant deployment one replica is deliberate. Do not add a
      // scale rule that would mount/serve multiple replicas over shared data.
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
  dependsOn: [
    blobDataAssignment
  ]
}

// -----------------------------------------------------------------------------
// Refresh job — scheduled or manual, holds the Rapid7 key, builds the artifact
// -----------------------------------------------------------------------------
// This is the ONLY component with a Rapid7 credential. The key arrives as a Key
// Vault secret reference (keyVaultUrl + identity), NOT a @secure() param baked
// into config (defect #3). Rotating in Key Vault is picked up without redeploy.

resource refreshJob 'Microsoft.App/jobs@2024-03-01' = {
  name: refreshJobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${uami.id}': {}
    }
  }
  properties: {
    environmentId: environment.id
    configuration: {
      // Schedule optional/disable-able: Schedule trigger when enabled, else a
      // Manual job the operator runs on demand (same code path either way).
      triggerType: enableScheduledRefresh ? 'Schedule' : 'Manual'
      registries: registryConfig
      replicaTimeout: 3600 // 1h ceiling for a full build; job exits non-zero on window failure
      replicaRetryLimit: 1
      scheduleTriggerConfig: enableScheduledRefresh ? {
        cronExpression: refreshCronExpression
        parallelism: 1
        replicaCompletionCount: 1
      } : null
      manualTriggerConfig: enableScheduledRefresh ? null : {
        parallelism: 1
        replicaCompletionCount: 1
      }
      // Key Vault secret reference: the platform fetches the value at runtime
      // using the job's managed identity. The template never sees the value.
      secrets: rapid7ApiKeyConfigured
        ? [
            {
              name: jobKvSecretRef
              keyVaultUrl: '${keyVault.properties.vaultUri}secrets/${rapid7ApiKeySecretName}'
              identity: uami.id
            }
          ]
        : []
    }
    template: {
      containers: [
        {
          name: 'mcp-refresh'
          image: containerImage
          // Foreground CLI refresh entrypoint (Phase 6a). Single process, no
          // threads, non-zero exit on any failed window.
          command: [
            'rapid7-refresh'
          ]
          args: refreshArgs
          resources: {
            cpu: jobCpu
            memory: jobMemory
          }
          env: concat(
            // Rapid7 credential — secret reference, JOB ONLY. Omitted entirely
            // until the secret exists, because Container Apps validates a Key
            // Vault reference at job-creation time and fails the deployment if
            // it cannot fetch it. The job errors clearly on a missing key.
            rapid7ApiKeyConfigured
              ? [
                  {
                    name: 'RAPID7_API_KEY'
                    secretRef: jobKvSecretRef
                  }
                ]
              : [],
            [
            {
              name: 'RAPID7_REGION'
              value: rapid7Region
            }
            // Build-role DuckDB limit, below the 4Gi job container memory.
            {
              name: 'DUCKDB_MEMORY_LIMIT'
              value: jobDuckdbMemoryLimit
            }
            // Blob artifact destination (upload over HTTPS after a clean build).
            {
              name: 'ARTIFACT_BLOB_ACCOUNT_URL'
              value: storageAccount.properties.primaryEndpoints.blob
            }
            {
              name: 'ARTIFACT_BLOB_CONTAINER'
              value: artifactContainerName
            }
            {
              name: 'ARTIFACT_RETAIN_VERSIONS'
              value: string(artifactRetainVersions)
            }
            // Same identity, used here for Key Vault + Blob write (+ documented
            // revision-trigger permission the operator grants post-deploy).
            {
              name: 'AZURE_CLIENT_ID'
              value: uami.properties.clientId
            }
            ]
          )
        }
      ]
    }
  }
  dependsOn: [
    kvSecretUserAssignment
    blobDataAssignment
  ]
}

// -----------------------------------------------------------------------------
// Outputs
// -----------------------------------------------------------------------------

@description('Internal FQDN of the serving Container App. Resolvable only from inside the virtual network. This is the value to put in the Copilot Studio MCP connector Server URL, as https://<fqdn>/mcp')
output serveAppFqdn string = serveApp.properties.configuration.ingress.fqdn

@description('''Whether the Container Apps ENVIRONMENT is internal. MUST be true — this is the
property that guarantees no public IP exists. Do not assert on the app's ingress.external
instead: on an internal environment that flag selects internal-load-balancer publication
versus environment-only visibility, and false there causes HTTP 404 for every VNet caller
rather than making anything more private.''')
output environmentIsInternal bool = environment.properties.vnetConfiguration.internal

@description('Container Apps environment default domain. This is the private DNS zone name.')
output environmentDefaultDomain string = environment.properties.defaultDomain

@description('Internal load balancer IP of the environment. The private DNS wildcard A record points here.')
output environmentStaticIp string = environment.properties.staticIp

@description('User-assigned managed identity client id (used by the app for Blob, by the job for Key Vault + Blob).')
output managedIdentityClientId string = uami.properties.clientId

@description('User-assigned managed identity principal id (for any additional role assignments, e.g. revision trigger).')
output managedIdentityPrincipalId string = uami.properties.principalId

@description('Key Vault name — set the Rapid7 API key secret here out-of-band (never through this template).')
output keyVaultName string = keyVault.name

@description('Storage account holding the Blob artifact container.')
output artifactStorageAccount string = storageAccount.name

@description('Blob container name for versioned DB artifacts.')
output artifactContainerName string = artifactContainerName

@description('Refresh job name (trigger manually, or leave to the schedule).')
output refreshJobName string = refreshJob.name
