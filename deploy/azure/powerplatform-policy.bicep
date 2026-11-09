// -----------------------------------------------------------------------------
// Power Platform virtual-network support: the network-injection enterprise policy.
//
// This is the ARM half of Power Platform VNet support. Creating the policy is an
// ARM operation; LINKING it to a Power Platform environment is NOT — that is
// `Enable-SubnetInjection` from the Microsoft.PowerPlatform.EnterprisePolicies
// PowerShell module. See deploy/azure/README.md.
//
// Three things about this resource type are easy to get wrong:
//
//   1. `location` is a Power Platform GEOGRAPHY, not an Azure region.
//      'unitedkingdom', not 'uksouth'.
//   2. The virtual network is referenced by its FULL RESOURCE ID, but the subnet
//      by its BARE NAME. Not a subnet resource id.
//   3. Geographies with more than one region require TWO entries, in different
//      regions — the pair, for failover. Since a VNet is regional, that means two
//      VNets.
//
// There is no GA api version; 2020-10-30-preview is the only one published.
//
// IMPORTANT consequence of (3): the second region's subnet is in a different VNet
// from the Container Apps environment, so on failover the connector runtime cannot
// reach the app unless that VNet is PEERED to the one holding the environment.
// Peering is not created here — it is a deliberate network decision, and the
// alternative is a second Container Apps environment in the paired region.
// -----------------------------------------------------------------------------

@description('Name for the enterprise policy resource.')
param policyName string

@description('''Power Platform GEOGRAPHY, not an Azure region: e.g. unitedkingdom, europe,
unitedstates. Must match the geography of the Power Platform environment you will link this
policy to, or the link is rejected.''')
param geography string

@description('Resource id of the virtual network in the PRIMARY region (the one holding the Container Apps environment).')
param primaryVnetId string

@description('Name of the delegated subnet in the primary virtual network. A NAME, not a resource id.')
param primarySubnetName string

@description('''Resource id of the virtual network in the PAIRED region. Required for geographies
with more than one region. Leave empty only for a single-region geography.''')
param pairedVnetId string = ''

@description('Name of the delegated subnet in the paired virtual network. A NAME, not a resource id.')
param pairedSubnetName string = ''

@description('Tags applied to the policy.')
param tags object = {}

var primaryEntry = [
  {
    id: primaryVnetId
    subnet: {
      name: primarySubnetName
    }
  }
]

var pairedEntry = empty(pairedVnetId)
  ? []
  : [
      {
        id: pairedVnetId
        subnet: {
          name: pairedSubnetName
        }
      }
    ]

resource policy 'Microsoft.PowerPlatform/enterprisePolicies@2020-10-30-preview' = {
  name: policyName
  location: geography
  kind: 'NetworkInjection'
  tags: tags
  properties: {
    networkInjection: {
      virtualNetworks: concat(primaryEntry, pairedEntry)
    }
  }
}

@description('Pass this to Enable-SubnetInjection -PolicyArmId to link the policy to an environment.')
output policyArmId string = policy.id

@description('Policy name, for the admin centre UI path.')
output policyName string = policy.name
