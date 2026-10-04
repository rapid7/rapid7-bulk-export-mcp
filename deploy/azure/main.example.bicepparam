// Example parameters for main.bicep — copy and fill in per deployment.
//
// deploy with:
//   az deployment group create \
//     --resource-group <rg> \
//     --template-file main.bicep \
//     --parameters main.example.bicepparam
//
// NOTE: the Rapid7 API key is NOT here. It is set directly in Key Vault after
// deployment (see README.md), never passed as a template parameter.

using './main.bicep'

param namePrefix = 'r7bulk'

// Required tags. Many enterprise tenants enforce these with Azure Policy, and a
// deployment that omits them is rejected at the FIRST resource with
// RequestDisallowedByPolicy — which reads like a template bug rather than a
// governance rule. Check your tenant's policy assignments for what is mandatory.
param tags = {
  Owner_Email: 'you@example.com'
}

// Official published image — Docker Hub, publicly pullable, so Container Apps
// needs no registry credentials and containerRegistryName can stay empty.
//
// IMPORTANT: this image is built from a RELEASE. It does not contain unreleased
// local changes. To deploy code that is not in a published release, build it into
// a private registry instead (see "Which image?" in README.md) and set BOTH:
//
//   param containerRegistryName = '<registryName>'
//   param containerImage = '<registryName>.azurecr.io/rapid7-bulk-export-mcp:dev'
//
// Setting containerImage to a private registry WITHOUT containerRegistryName
// leaves the pull unauthenticated: the deployment reports success and the
// revision then fails to start.
param containerImage = 'rapid7/bulk-export-mcp:0.6.1'

// Subnet for the Blob private endpoint. SAME vnet, and NOT delegated — a private
// endpoint cannot live in a delegated subnet, so this cannot be the Container Apps
// infrastructure subnet. Required because the storage account has
// publicNetworkAccess disabled; without it the replicas cannot reach Blob and the
// failure appears as AuthorizationFailure, which looks like a missing RBAC role.
param privateEndpointSubnetId = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/<rg>/providers/Microsoft.Network/virtualNetworks/<vnet>/subnets/<pe-subnet>'

// Existing subnet delegated to Microsoft.App/environments, in the customer's
// VNet, region-paired with their Power Platform region. Customer-provided.
param infrastructureSubnetId = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/<rg>/providers/Microsoft.Network/virtualNetworks/<vnet>/subnets/<cae-infra-subnet>'

// Inbound auth (values from the Entra app registration created post-deploy).
param authJwksUri = 'https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys'
param authIssuer = 'https://login.microsoftonline.com/<tenant-id>/v2.0'
param authAudience = 'api://<app-client-id>'
param authRequiredScopes = ''
param authWriteScope = 'rapid7.write'

// Rapid7 (job only).
param rapid7Region = 'us'
param rapid7ApiKeySecretName = 'rapid7-api-key'

// Scheduled refresh (set enableScheduledRefresh = false for operator-triggered only).
param enableScheduledRefresh = true
param refreshCronExpression = '0 2 * * *'
param refreshExportTypes = 'vulnerability'

// DuckDB limits kept below their container memory ceilings.
param serveDuckdbMemoryLimit = '2000MB'
param jobDuckdbMemoryLimit = '3GB'

param logRetentionDays = 30
