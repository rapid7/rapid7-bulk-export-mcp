# Architecture diagram

Service-to-service view of the deployed topology. Every edge names its protocol and
how it authenticates. Dotted edges are identity and telemetry; solid edges carry data.

Mermaid source, so it renders in GitHub, Teams, Confluence and most slide tools.

See [`copilot-studio-hosting.md`](copilot-studio-hosting.md) for how it works, and
[`../deploy/azure/SETUP.md`](../deploy/azure/SETUP.md) for how to build it.

```mermaid
flowchart TB
    subgraph m365["Microsoft 365 / Power Platform — customer tenant"]
        teams["Microsoft Teams<br/>or Microsoft 365 Copilot"]
        cs["Copilot Studio agent<br/><i>Dataverse component</i>"]
    end

    subgraph azure["Customer Azure subscription"]
        subgraph vnet["Azure Virtual Network — no public inbound"]
            runtime["Power Platform connector runtime<br/><i>injected into a delegated subnet</i>"]
            aca["Azure Container Apps<br/><i>internal load balancer, no public IP</i>"]
            job["Azure Container Apps Job<br/><i>scheduled refresh</i>"]
            pe(["Azure Private Endpoints"])
        end

        subgraph paas["Azure PaaS — public network access disabled"]
            blob[("Azure Blob Storage")]
            kv["Azure Key Vault"]
        end

        law["Azure Monitor<br/>Log Analytics"]
    end

    entra["Microsoft Entra ID"]
    r7["Rapid7 Insight Platform"]

    teams -->|"chat"| cs
    cs -->|"MCP streamable HTTP"| runtime
    runtime -->|"HTTPS 443 · Bearer<br/>delegated user token"| aca
    aca -->|"managed identity<br/>Blob Data Contributor"| pe
    job -->|"managed identity<br/>Secrets User"| pe
    pe --> blob
    pe --> kv
    job -->|"HTTPS · API key<br/><b>outbound only</b>"| r7
    cs -.->|"OAuth 2.0"| entra
    aca -.->|"JWKS"| entra
    aca -.-> law
    job -.-> law

    classDef inside fill:#e8f4ea,stroke:#2e7d32,stroke-width:2px
    classDef locked fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    classDef outside fill:#fafafa,stroke:#bdbdbd,stroke-dasharray:4 3
    class runtime,aca,job,pe inside
    class blob,kv locked
    class entra,r7 outside
```

Everything inside the outer box runs in the customer's **own Azure subscription**.
Only Microsoft Entra ID and the Rapid7 platform sit outside it, and both are reached
outbound.

## Notes a reviewer will want

**The connector runtime is inside the customer's network.** That is the whole basis of
the design — Power Platform VNet support injects it into a delegated subnet, so its
call to the Container App never traverses the internet. Confirmed by the server
logging an inbound request from that subnet.

**No secrets on any edge except one.** Both Azure workloads authenticate to Blob and
Key Vault with a user-assigned managed identity holding narrowly-scoped roles. The
only long-lived credential is the Rapid7 API key, held in Key Vault and read solely by
the refresh job — the serving container has no Rapid7 credential at all.

**The user's identity reaches the server.** The connector sends a delegated user
token, not an application token. There is no per-user data filtering, so that token is
the only record of who asked what.

**The two Entra edges are outbound and unavoidable.** The agent obtains a token and
the server fetches signing keys, both to public Microsoft endpoints. Neither carries
customer data and neither accepts inbound connections.

**The dataset is a point-in-time copy.** The refresh job publishes a complete snapshot
and the serving container opens a local read-only copy, so queries are never served
from a partially-loaded database. Answers state how old the data is.

**The connector runtime's region is assigned, not chosen.** If Power Platform places
the environment in a different Azure region from the Container Apps environment, the
delegated subnet lives in its own virtual network and needs peering *plus* a private
DNS zone link. Peering alone gives connectivity without name resolution — see the trap
in [`../deploy/azure/SETUP.md`](../deploy/azure/SETUP.md).

## What is deliberately absent

No Front Door, no WAF, no Application Gateway and no public origin. There is nothing
publicly addressable to protect, so adding those would be cost without a control.
