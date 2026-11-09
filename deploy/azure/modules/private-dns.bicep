// -----------------------------------------------------------------------------
// Private DNS for an internal Container Apps environment.
//
// This lives in a module rather than in main.bicep for a hard ARM reason: a
// resource NAME must be computable at the start of the deployment, and the zone
// name here is the environment's `defaultDomain`, which is only known after the
// environment has been created. Passing it into a module works because the
// module is a nested deployment — by the time it runs the value is resolved, and
// inside the module the name is an ordinary parameter.
//
// Attempting this inline in main.bicep fails with BCP120.
// -----------------------------------------------------------------------------

@description('Private DNS zone name. Must be the Container Apps environment defaultDomain.')
param zoneName string

@description('Resource id of the virtual network to link the zone to.')
param vnetResourceId string

@description('Internal load balancer IP of the Container Apps environment.')
param environmentStaticIp string

@description('Name suffix for the virtual network link, so repeat links are distinguishable.')
param linkName string

@description('Tags applied to the zone and the virtual network link. A records are not taggable.')
param tags object = {}

resource zone 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: zoneName
  location: 'global'
  tags: tags
}

resource link 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: zone
  name: linkName
  location: 'global'
  tags: tags
  properties: {
    // Auto-registration is for VM hostnames; this zone only carries the
    // wildcard record for the Container Apps environment.
    registrationEnabled: false
    virtualNetwork: {
      id: vnetResourceId
    }
  }
}

// One wildcard record covers every app in the environment, current and future,
// so adding an app later needs no DNS change.
resource wildcard 'Microsoft.Network/privateDnsZones/A@2020-06-01' = {
  parent: zone
  name: '*'
  properties: {
    ttl: 3600
    aRecords: [
      {
        ipv4Address: environmentStaticIp
      }
    ]
  }
}

@description('Name of the created private DNS zone.')
output zoneName string = zone.name
