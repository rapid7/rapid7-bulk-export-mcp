// -----------------------------------------------------------------------------
// A private endpoint plus its private DNS zone, link, and zone group.
//
// Factored out because this deployment needs the same shape twice — once for
// Blob (the artifact courier) and once for Key Vault (the Rapid7 credential) —
// and both accounts deny public network access, so without these the workloads
// cannot reach either service.
//
// Both services answer a network-ACL denial with an AUTHORIZATION error rather
// than a network one, so a missing private endpoint presents as a missing RBAC
// role and sends you looking in the wrong place. Blob says
// "AuthorizationFailure"; Key Vault says "Forbidden".
//
// privateDnsZoneGroups lets Azure create and maintain the A record, so nothing
// here hardcodes a private IP.
//
// Zone names are literals, so unlike the Container Apps environment zone these
// can be declared without a nested deployment (no BCP120).
// -----------------------------------------------------------------------------

@description('Name for the private endpoint resource.')
param name string

@description('Azure region.')
param location string

@description('Undelegated subnet for the private endpoint. A private endpoint cannot live in a delegated subnet.')
param subnetId string

@description('Resource id of the service being reached privately.')
param targetResourceId string

@description('Target sub-resource, e.g. blob for storage, vault for Key Vault.')
param groupId string

@description('Private DNS zone name, e.g. privatelink.blob.core.windows.net or privatelink.vaultcore.azure.net.')
param privateDnsZoneName string

@description('Resource id of the virtual network the zone is linked to.')
param vnetResourceId string

@description('Tags applied to every taggable resource here.')
param tags object = {}

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2023-11-01' = {
  name: name
  location: location
  tags: tags
  properties: {
    subnet: {
      id: subnetId
    }
    privateLinkServiceConnections: [
      {
        name: groupId
        properties: {
          privateLinkServiceId: targetResourceId
          groupIds: [
            groupId
          ]
        }
      }
    ]
  }
}

resource zone 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: privateDnsZoneName
  location: 'global'
  tags: tags
}

resource link 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: zone
  name: 'link-${uniqueString(vnetResourceId)}'
  location: 'global'
  tags: tags
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnetResourceId
    }
  }
}

resource zoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2023-11-01' = {
  parent: privateEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: groupId
        properties: {
          privateDnsZoneId: zone.id
        }
      }
    ]
  }
  // The zone must be linked to the VNet before records are useful in it.
  dependsOn: [
    link
  ]
}

@description('Private DNS zone created for this endpoint.')
output zoneName string = zone.name
