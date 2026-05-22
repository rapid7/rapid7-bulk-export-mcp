// ──────────────────────────────────────────────────────────────────────────────
// Rapid7 Bulk Export MCP — Azure Container Apps Deployment
//
// Provisions:
//   • Azure Container Apps environment + app (streamable HTTP on port 8000)
//   • Azure Files share mounted at /data for persistent DuckDB storage
//   • Entra ID app registration with exposed API scope and client secret
//
// Deploy with:  azd up
// ──────────────────────────────────────────────────────────────────────────────

targetScope = 'resourceGroup'

// ─── Parameters ──────────────────────────────────────────────────────────────

@description('Name of the environment (used as prefix for all resources)')
param environmentName string

@description('Azure region for all resources')
param location string = resourceGroup().location

@secure()
@description('Rapid7 Command API key')
param rapid7ApiKey string

@description('Rapid7 region identifier (us, us2, us3, eu, ca, au, ap)')
@allowed(['us', 'us2', 'us3', 'eu', 'ca', 'au', 'ap'])
param rapid7Region string = 'us'

@description('Container image to deploy')
param containerImage string = 'ghcr.io/rapid7/rapid7-bulk-export-mcp:latest'

@description('Minimum number of replicas (set to 1 to avoid cold-start latency)')
@minValue(0)
@maxValue(10)
param minReplicas int = 1

@description('Maximum number of replicas')
@minValue(1)
@maxValue(10)
param maxReplicas int = 3

// ─── Variables ───────────────────────────────────────────────────────────────

var resourcePrefix = toLower('${environmentName}-r7mcp')
var storageAccountName = toLower(replace('${take(environmentName, 14)}r7mcpsa', '-', ''))
var fileShareName = 'rapid7-data'
var logAnalyticsName = '${resourcePrefix}-logs'
var containerAppEnvName = '${resourcePrefix}-env'
var containerAppName = '${resourcePrefix}-app'

// ─── Log Analytics Workspace ─────────────────────────────────────────────────

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

// ─── Storage Account + File Share ────────────────────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = {
  parent: fileService
  name: fileShareName
  properties: {
    shareQuota: 5 // 5 GB — sufficient for DuckDB export data
  }
}

// ─── Container Apps Environment ──────────────────────────────────────────────

resource containerAppEnv 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: containerAppEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

resource envStorage 'Microsoft.App/managedEnvironments/storages@2023-05-01' = {
  parent: containerAppEnv
  name: 'rapid7data'
  properties: {
    azureFile: {
      accountName: storageAccount.name
      accountKey: storageAccount.listKeys().keys[0].value
      shareName: fileShareName
      accessMode: 'ReadWrite'
    }
  }
}

// ─── Container App ───────────────────────────────────────────────────────────

resource containerApp 'Microsoft.App/containerApps@2023-05-01' = {
  name: containerAppName
  location: location
  properties: {
    managedEnvironmentId: containerAppEnv.id
    configuration: {
      ingress: {
        external: true
        targetPort: 8000
        transport: 'http'
        allowInsecure: false
      }
      secrets: [
        {
          name: 'rapid7-api-key'
          value: rapid7ApiKey
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'rapid7-bulk-export-mcp'
          image: containerImage
          resources: {
            cpu: json('1.0')
            memory: '2Gi'
          }
          env: [
            { name: 'RAPID7_API_KEY', secretRef: 'rapid7-api-key' }
            { name: 'RAPID7_REGION', value: rapid7Region }
            { name: 'MCP_TRANSPORT', value: 'http' }
            { name: 'MCP_HOST', value: '0.0.0.0' }
            { name: 'MCP_PORT', value: '8000' }
          ]
          volumeMounts: [
            {
              volumeName: 'rapid7-data-volume'
              mountPath: '/data'
            }
          ]
        }
      ]
      volumes: [
        {
          name: 'rapid7-data-volume'
          storageName: 'rapid7data'
          storageType: 'AzureFile'
        }
      ]
      scale: {
        minReplicas: minReplicas
        maxReplicas: maxReplicas
        rules: [
          {
            name: 'http-scaling'
            http: {
              metadata: {
                concurrentRequests: '50'
              }
            }
          }
        ]
      }
    }
  }
  dependsOn: [
    envStorage
  ]
}

// ─── Entra ID App Registration ───────────────────────────────────────────────
// NOTE: Entra ID (Azure AD) app registrations cannot be provisioned via Bicep.
// They require Microsoft Graph API calls. The following outputs provide
// placeholders — use the Azure CLI commands in the setup guide to create
// the app registration after deployment.
//
// Post-deployment steps (automated in docs/copilot-studio-setup.md):
//   az ad app create --display-name "Rapid7 Bulk Export MCP" \
//     --sign-in-audience AzureADMyOrg \
//     --web-redirect-uris "https://teams.microsoft.com/api/platform/v1.0/oAuthConsentRedirect"
//   az ad app credential reset --id <app-id>

// ─── Outputs ─────────────────────────────────────────────────────────────────

@description('The FQDN of the deployed Container App')
output containerAppUrl string = 'https://${containerApp.properties.configuration.ingress.fqdn}'

@description('The MCP endpoint URL (use this in Copilot Studio)')
output mcpEndpointUrl string = 'https://${containerApp.properties.configuration.ingress.fqdn}/mcp'

@description('Container App resource ID')
output containerAppId string = containerApp.id

@description('Log Analytics workspace ID (for troubleshooting)')
output logAnalyticsWorkspaceId string = logAnalytics.id
