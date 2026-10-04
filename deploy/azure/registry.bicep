// -----------------------------------------------------------------------------
// Private container registry — deploy this FIRST, before main.bicep.
//
// Why this is a separate template rather than part of main.bicep: the registry
// has to exist before the image can be pushed, and the image has to exist before
// the Container App can start a healthy revision. That is a genuine ordering
// dependency across a step Azure cannot perform for you (the push), so it is an
// honest two-phase deployment rather than something to hide behind a flag.
//
//   1. az deployment group create -f registry.bicep      <- this file
//   2. az acr build ...                                  <- push the image
//   3. az deployment group create -f main.bicep \
//        --parameters containerRegistryName=<name>       <- everything else
//
// Prefer `az acr build` over a local `docker build`: it builds on Azure's own
// amd64 agents. A local build on Apple Silicon produces an arm64 image, and
// Container Apps will pull it and then fail to start with an exec-format error
// that looks nothing like an architecture problem.
// -----------------------------------------------------------------------------

@description('Globally unique registry name. Alphanumeric only, 5-50 chars.')
@minLength(5)
@maxLength(50)
param registryName string

@description('Azure region. Should match the region used for main.bicep.')
param location string = resourceGroup().location

@description('''Registry SKU. Basic is sufficient for this deployment: the image is
pulled over the registry's public endpoint using the managed identity, which is an
OUTBOUND call from the Container Apps subnet and does not make the MCP server
reachable from the internet. Choose Premium only if policy requires the registry
itself to be private-endpoint-only.''')
@allowed([
  'Basic'
  'Standard'
  'Premium'
])
param sku string = 'Basic'

@description('''Tags applied to the registry. Enterprise tenants often enforce required
tags with Azure Policy; a deployment that omits them fails with RequestDisallowedByPolicy.''')
param tags object = {}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: registryName
  location: location
  tags: tags
  sku: {
    name: sku
  }
  properties: {
    // Authentication is via managed identity + AcrPull, assigned in main.bicep.
    // The admin account is a shared username/password and is deliberately off.
    adminUserEnabled: false
  }
}

@description('Pass this to main.bicep as containerRegistryName.')
output registryName string = registry.name

@description('Login server, e.g. myregistry.azurecr.io — the image prefix for az acr build.')
output loginServer string = registry.properties.loginServer
