# Azure + Copilot Studio Deployment Guide

Deploy the Rapid7 Bulk Export MCP server to Azure Container Apps and connect it to Microsoft Copilot Studio with Entra ID authentication.

## Prerequisites

- **Azure subscription** with Contributor access
- **Microsoft 365 Copilot Studio** license (or trial)
- **Rapid7 API key** — Platform Admin-level key from InsightVM ([how to generate](../README.md#0-get-your-rapid7-api-key-and-region))
- **Azure CLI** (`az`) installed — [install guide](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- **Azure Developer CLI** (`azd`) installed — [install guide](https://learn.microsoft.com/en-us/azure/developer/azure-developer-cli/install-azd)

## 1. Deploy to Azure Container Apps

### Initialize and deploy

```bash
cd deploy/azure

# Log in to Azure
azd auth login

# Initialize the environment (choose a name and region)
azd init

# Deploy infrastructure and container
azd up
```

You'll be prompted for:
| Parameter | Description |
|-----------|-------------|
| `environmentName` | Prefix for all Azure resources (e.g. `prod-r7mcp`) |
| `location` | Azure region (e.g. `eastus`, `westeurope`) |
| `rapid7ApiKey` | Your Rapid7 InsightVM API key |
| `rapid7Region` | Your Rapid7 data region (`us`, `us2`, `us3`, `eu`, `ca`, `au`, `ap`) |

### Capture outputs

After deployment completes, note the outputs:

```bash
azd show

# Or query specific values:
az containerapp show \
  --name <environmentName>-r7mcp-app \
  --resource-group rg-<environmentName> \
  --query "properties.configuration.ingress.fqdn" -o tsv
```

Save the **Container App URL** — you'll need it for Copilot Studio (e.g. `https://<app-name>.<region>.azurecontainerapps.io`).

### Verify the MCP endpoint

```bash
curl https://<your-container-app-url>/mcp
```

You should get a response indicating the MCP server is running.

## 2. Register Entra ID Application

Copilot Studio requires OAuth 2.0 authorization code flow. Create an Entra ID app registration:

```bash
# Create the app registration
az ad app create \
  --display-name "Rapid7 Bulk Export MCP" \
  --sign-in-audience AzureADMyOrg \
  --web-redirect-uris "https://teams.microsoft.com/api/platform/v1.0/oAuthConsentRedirect"
```

Capture the **Application (client) ID** from the output:

```bash
APP_ID=$(az ad app list --display-name "Rapid7 Bulk Export MCP" --query "[0].appId" -o tsv)
echo "Client ID: $APP_ID"
```

### Get your Tenant ID

```bash
TENANT_ID=$(az account show --query "tenantId" -o tsv)
echo "Tenant ID: $TENANT_ID"
```

### Create a client secret

```bash
az ad app credential reset --id $APP_ID --display-name "Copilot Studio" --years 2
```

Save the **password** value — this is your **Client Secret** (shown only once).

### Expose an API scope

```bash
# Set the Application ID URI
az ad app update --id $APP_ID --identifier-uris "api://$APP_ID"

# Add the 'mcp' scope
az ad app update --id $APP_ID --set api.oauth2PermissionScopes='[{"adminConsentDescription":"Access Rapid7 MCP tools","adminConsentDisplayName":"MCP Access","id":"'$(uuidgen)'","isEnabled":true,"type":"User","userConsentDescription":"Access Rapid7 MCP vulnerability data","userConsentDisplayName":"MCP Access","value":"mcp"}]'
```

### Summary of values

At this point you should have:

| Value | Source |
|-------|--------|
| Container App URL | `azd up` output |
| Client ID | `az ad app list` output |
| Client Secret | `az ad app credential reset` output |
| Tenant ID | `az account show` output |
| Scope | `api://<client-id>/mcp` |

## 3. Configure Copilot Studio

### Create a new agent

1. Go to [Copilot Studio](https://copilotstudio.microsoft.com)
2. Click **Create** → **New agent**
3. Name it (e.g. "Rapid7 Vulnerability Analyst")
4. Provide a description and instructions for the agent

### Add the MCP Server as a tool

1. In your agent, go to **Tools** → **Add a tool**
2. Select **MCP Server**
3. Enter the MCP endpoint URL: `https://<your-container-app-url>/mcp`
4. Click **Next**

### Configure OAuth 2.0 authentication

In the authentication configuration dialog:

1. Select **Manual** configuration mode
2. Fill in the following:

| Field | Value |
|-------|-------|
| Authorization URL | `https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/authorize` |
| Token URL | `https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token` |
| Client ID | Your Entra app Client ID |
| Client Secret | Your Entra app Client Secret |
| Scope | `api://<client-id>/mcp openid profile` |

3. Click **Connect**

### Update Entra redirect URI

After saving the tool configuration, Copilot Studio may display a redirect URI. If it differs from the one you registered:

1. Copy the redirect URI from Copilot Studio
2. Update the Entra app registration:

```bash
az ad app update --id $APP_ID \
  --web-redirect-uris \
    "https://teams.microsoft.com/api/platform/v1.0/oAuthConsentRedirect" \
    "<copilot-studio-redirect-uri>"
```

## 4. (Optional) Agent 365 BYO MCP Registration

For organizations using Microsoft 365 admin governance, you can register the MCP server as a managed tool:

### Register via CLI

```bash
# Install the Agents 365 CLI extension (if not already installed)
az extension add --name agents365

# Register the MCP server
az agents365 register-mcp-server \
  --name "Rapid7 Bulk Export MCP" \
  --endpoint "https://<your-container-app-url>/mcp" \
  --app-id $APP_ID \
  --description "Rapid7 InsightVM vulnerability data analysis via SQL queries"
```

### Admin approval flow

1. Go to the [Microsoft 365 Admin Center](https://admin.microsoft.com)
2. Navigate to **Settings** → **Integrated apps** → **Agent tools**
3. Find "Rapid7 Bulk Export MCP" in the pending list
4. Review permissions and click **Approve**
5. Assign to specific users or groups as needed

Once approved, the MCP server appears as an available tool for all authorized Copilot Studio agents in your tenant.

## 5. Publish the Agent

1. In Copilot Studio, click **Publish** on your agent
2. Choose distribution channels:
   - **Teams** — Available as a Teams app for selected users/groups
   - **Microsoft 365 Copilot** — Appears as a plugin in M365 Copilot
   - **Custom website** — Embed via iframe or direct link
3. Under **Availability**, select the users or security groups who should have access
4. Click **Publish**

Users can then interact with the agent to query Rapid7 vulnerability data using natural language:

```
Show me the top 10 critical vulnerabilities with known exploits
```

```
What's the severity distribution across my cloud assets?
```

## Troubleshooting

### Container App not responding

```bash
# Check container logs
az containerapp logs show \
  --name <environmentName>-r7mcp-app \
  --resource-group rg-<environmentName> \
  --follow

# Check replica status
az containerapp replica list \
  --name <environmentName>-r7mcp-app \
  --resource-group rg-<environmentName>
```

### OAuth errors in Copilot Studio

- Verify the redirect URI in Entra matches exactly what Copilot Studio expects
- Confirm the scope format is `api://<client-id>/mcp openid profile`
- Check that the client secret hasn't expired
- Ensure the app registration's `sign-in-audience` is set to `AzureADMyOrg`

### Data persistence

The DuckDB database is stored on an Azure Files share mounted at `/data`. Data persists across container restarts and scaling events. To verify:

```bash
# Check the file share
az storage file list \
  --account-name <storage-account-name> \
  --share-name rapid7-data \
  --output table
```

### Cold-start latency

The deployment sets `minReplicas: 1` by default to avoid cold-start delays. If cost is a concern and occasional latency is acceptable, you can set it to 0 in the Bicep parameters.

## Architecture

```
┌─────────────────────┐     OAuth 2.0      ┌──────────────────┐
│   Copilot Studio    │◄───────────────────►│    Entra ID      │
│   (Agent + Tools)   │                     │  (App Reg + JWT) │
└────────┬────────────┘                     └──────────────────┘
         │
         │ Streamable HTTP (/mcp)
         ▼
┌─────────────────────────────────────────────────────────────┐
│              Azure Container Apps                             │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  rapid7-bulk-export-mcp (ghcr.io/rapid7/...)           │  │
│  │  Port 8000 • MCP_TRANSPORT=http                        │  │
│  │                                                        │  │
│  │  /data/rapid7_bulk_export.db  ← Azure Files mount      │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
         │
         │ HTTPS (Bulk Export API)
         ▼
┌─────────────────────┐
│  Rapid7 InsightVM   │
│  (Command Platform) │
└─────────────────────┘
```
