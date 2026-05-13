# Rapid7 Bulk Export MCP

AI-powered analysis for Rapid7 Command Platform data using MCP ([Model Context Protocol](https://modelcontextprotocol.io/docs/getting-started/intro)) & [AgentSkills](https://agentskills.io/home).

This tool is a best effort support, due to the bespoke and ever-changing nature of tools and workflows which would utilize this tool we cannot provide support or guidance outside of the MCP Code & AgentSkill Content.

## What is This?

This tool exports data from Rapid7 Command Platform, via the [Rapid7 Bulk Export API](https://docs.rapid7.com/insightvm/bulk-export-api/) and makes it queryable in GenAI and Agentic workflows.

- **MCP Server**: Embeds tools which allow the getting, processing and querying of data
- **Agent Skill / Kiro Power**: Gives additional context, schema knowledge and instructions on how to use the MCP tools
- **DuckDB Database**: Local file-based database to allow structured rapid querying

## Demo

![Demo in Claude Code](./docs/question_asking_200x.gif)

## Quickstart for Claude Desktop

Download the mcpb and zip from the Github Releases on the right hand side. For other AI Tools and more detailed instructions see the getting started guide further down.

### Install

![Installing the Bulk Export MCP & Skill](./docs/bulk_export_mcp_setup_150x.gif)

### Initialize Data

This is done once a day and takes 1-5mins depending on the size of your org.

![Load the data](./docs/first_time_data_load_150x.gif)

## Features

- **AI-Powered Analysis**: Use with Kiro, Claude Desktop, or any MCP-compatible AI assistant
- **On-Demand Data Loading**: Automatically fetch and load data from Rapid7 via the export tools
- **Export Reuse**: Automatically reuses exports from the same day to avoid redundant API calls
- **Natural Language Queries**: Ask questions in plain English
- **SQL Query Execution**: Run complex SQL queries against vulnerability, asset and other data
- **Schema Exploration**: Discover available data fields
- **Statistics & Insights**: Get instant summaries and distributions

## MCP Server Tools

- `start_rapid7_export(export_type, start_date, end_date)` - Start a new export job (non-blocking, returns export ID)
- `check_rapid7_export_status(export_id)` - Check export progress (non-blocking)
- `download_rapid7_export(export_id, export_type)` - Download completed export and load into database
- `load_rapid7_parquet(parquet_path)` - Load existing local Parquet files (skip export)
- `query_rapid7(sql)` - Execute SQL queries against loaded data
- `get_rapid7_schema()` - View table schemas and column types
- `get_rapid7_stats()` - Get summary statistics and distributions
- `list_rapid7_exports(limit)` - View recent exports and their metadata

## Model Requirements

This tool requires an AI model with:

- **Tool/function calling support** — The model must be able to invoke MCP tools (start exports, run SQL queries, etc.)
- **Strong reasoning capabilities** — Multi-step workflows (export → poll → download → query) require models that can plan and track state across tool calls
- **What works well:** Claude, ChatGPT, Gemini, and other frontier models with native tool-calling support.
- **What doesn't work well:** Smaller models (typically <30B parameters) and models without native tool-calling.

## Quick Start

### 0. Get Your Rapid7 API Key and Region

Before you begin, you'll need credentials from your Rapid7 Insight Platform account.

**Generate an API Key:**

1. Log in to the [Rapid7 Insight Platform](https://insight.rapid7.com)
2. Navigate to Administration → API Key Management
3. Choose the API key type (Platform Admin role required):
   - **User Key**: Inherits your account permissions (any user can create)
   - **Organization Key**: Full admin permissions
4. Click "Generate New User Key" (or "Generate New Admin Key" for org keys)
5. Select your organization and provide a name for the key
6. Copy the key immediately - you won't be able to view it again!

**Find Your Region:**

Your region determines which API endpoint to use. To find your region:

1. Go to [insight.rapid7.com](https://insight.rapid7.com) and sign in
2. Look for the "Data Storage Region" tag in the upper right corner below your account name

For more details, see:
- [Managing Platform API Keys](https://docs.rapid7.com/insight/managing-platform-api-keys)
- [Product APIs and Regions](https://docs.rapid7.com/insight/product-apis)

### 1. Install

```bash
# Using pip
pip install git+https://github.com/rapid7/rapid7-bulk-export-mcp.git

# Or using uv
uv pip install git+https://github.com/rapid7/rapid7-bulk-export-mcp.git
```

### 2. Configure MCP Server

**Configuration Notes:**
The configuration format and location varies by AI assistant. Below are some examples for popular tools:

<details>
<summary><b>Kiro</b></summary>

Create or edit `.kiro/settings/mcp.json`:

```json
{
  "mcpServers": {
    "rapid7-bulk-export": {
      "command": "rapid7-mcp-server",
      "args": [],
      "env": {
        "RAPID7_API_KEY": "your-api-key-here",
        "RAPID7_REGION": "your-region"
      }
    }
  }
}
```

**Configuration Notes:**
- `RAPID7_API_KEY` - Required: Your Rapid7 InsightVM API key
- `RAPID7_REGION` - Required: Your region (`us`, `eu`, `ca`, `au`, or `ap`)
- For user-based installations: You may need to specify the absolute path to the server in your mcp.json configuration:
`"command": "/Users/<your-username>/path/to/bulkexport/.venv/bin/rapid7-mcp-server"`


</details>

<details>
<summary><b>Claude Code (IDE)</b></summary>

Use the Claude Code CLI to add the MCP server:

```bash
claude mcp add --transport stdio \
  --env RAPID7_API_KEY=your-api-key-here \
  --env RAPID7_REGION=your-region \
  rapid7-bulk-export \
  -- rapid7-mcp-server
```

Or manually edit `~/.claude.json` (user scope) or `.mcp.json` (project scope):

```json
{
  "mcpServers": {
    "rapid7-bulk-export": {
      "command": "rapid7-mcp-server",
      "args": [],
      "env": {
        "RAPID7_API_KEY": "your-api-key-here",
        "RAPID7_REGION": "your-region"
      }
    }
  }
}
```

**Configuration Notes:**
- `RAPID7_API_KEY` - Required: Your Rapid7 InsightVM API key
- `RAPID7_REGION` - Required: Your region (`us`, `eu`, `ca`, `au`, or `ap`)
- Use `--scope user` for cross-project access or `--scope project` for team sharing
- For user-based installations: You may need to specify the absolute path to the server in your mcp.json configuration:
`"command": "/Users/<your-username>/path/to/bulkexport/.venv/bin/rapid7-mcp-server"`

</details>

<details>
<summary><b>Claude Desktop</b></summary>

Edit `claude_desktop_config.json`:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "rapid7-bulk-export": {
      "command": "rapid7-mcp-server",
      "args": [],
      "env": {
        "RAPID7_API_KEY": "your-api-key-here",
        "RAPID7_REGION": "your-region"
      }
    }
  }
}
```

**Configuration Notes:**
- `RAPID7_API_KEY` - Required: Your Rapid7 InsightVM API key
- `RAPID7_REGION` - Required: Your region (`us`, `eu`, `ca`, `au`, or `ap`)
- For user-based installations: You may need to specify the absolute path to the server in your mcp.json configuration:
`"command": "/Users/<your-username>/path/to/bulkexport/.venv/bin/rapid7-mcp-server"`

</details>

<details>
<summary><b>GitHub Copilot (VS Code)</b></summary>

Edit MCP settings in VS Code:
- Use Command Palette: "MCP: Edit Configuration"
- Or manually edit: `.vscode/mcp.json` (workspace) or user settings

```json
{
  "mcpServers": {
    "rapid7-bulk-export": {
      "command": "rapid7-mcp-server",
      "args": [],
      "env": {
        "RAPID7_API_KEY": "your-api-key-here",
        "RAPID7_REGION": "your-region"
      }
    }
  }
}
```

**Configuration Notes:**
- `RAPID7_API_KEY` - Required: Your Rapid7 InsightVM API key
- `RAPID7_REGION` - Required: Your region (`us`, `eu`, `ca`, `au`, or `ap`)
- For user-based installations: You may need to specify the absolute path to the server in your mcp.json configuration:
`"command": "/Users/<your-username>/path/to/bulkexport/.venv/bin/rapid7-mcp-server"`

</details>

<details>
<summary><b>Gemini CLI</b></summary>

Edit `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "rapid7-bulk-export": {
      "command": "rapid7-mcp-server",
      "args": [],
      "env": {
        "RAPID7_API_KEY": "your-api-key-here",
        "RAPID7_REGION": "your-region"
      }
    }
  }
}
```


**Configuration Notes:**
- `RAPID7_API_KEY` - Required: Your Rapid7 InsightVM API key
- `RAPID7_REGION` - Required: Your region (`us`, `eu`, `ca`, `au`, or `ap`)
- Your `settings.json` may already have other configuration (e.g., auth settings). Merge the `mcpServers` block into the existing file.
- For user-based installations: You may need to specify the absolute path to the server in your settings.json configuration:
`"command": "/Users/<your-username>/path/to/bulkexport/.venv/bin/rapid7-mcp-server"`

</details>


### 3. Install Agent Skill / Power

The Agent Skill (or Kiro Power) provides domain expertise for vulnerability analysis.

**What It Provides:**
- Understanding of bulk export data schema
- SQL query patterns and examples
- Best practices for security analysis
- Guidance on risk prioritization

<details>
<summary><b>Universal (AgentSkills CLI)</b></summary>

Install the skill across all supported AI tools at once using the [AgentSkills](https://agentskills.io) CLI:

```bash
npx skills add rapid7/rapid7-bulk-export-mcp --global
```

This automatically places the skill in the correct location for Kiro, Claude Code, Gemini CLI, GitHub Copilot, and other compatible tools.

</details>

<details>
<summary><b>Kiro (Power — recommended)</b></summary>

Install the Kiro Power for the best experience. The Power bundles the MCP server configuration, domain knowledge, and analysis workflows together.

1. Open Kiro → Powers panel → **Add power from GitHub**
2. Enter the repository URL: `https://github.com/rapid7/rapid7-bulk-export-mcp`
3. Select the `power-rapid7-bulk-export` directory
4. Set your `RAPID7_API_KEY` and `RAPID7_REGION` when prompted

The Power activates automatically when you mention keywords like "rapid7", "vulnerability", "insightvm", or "bulk-export" in chat.

Alternatively, install from a local clone:

1. Open Kiro → Powers panel → **Add power from Local Path**
2. Select the `power-rapid7-bulk-export/` directory in this repository

</details>

<details>
<summary><b>Kiro (Skill — manual alternative)</b></summary>

If you prefer the standalone skill file instead of the Power:

```bash
# User-level (available in all workspaces)
cp rapid7-bulk-export-skill/SKILL.md ~/.kiro/skills/rapid7-bulk-export.md

# Or workspace-level (only in current workspace)
cp rapid7-bulk-export-skill/SKILL.md .kiro/skills/rapid7-bulk-export.md
```

Activate the skill in chat:
```
#rapid7-bulk-export
```

</details>

<details>
<summary><b>Claude Code (IDE)</b></summary>

```bash
# User-level (available in all projects)
mkdir -p ~/.claude/skills/rapid7-bulk-export
cp rapid7-bulk-export-skill/SKILL.md ~/.claude/skills/rapid7-bulk-export/

# Or project-level (only in current project)
mkdir -p .claude/skills/rapid7-bulk-export
cp rapid7-bulk-export-skill/SKILL.md .claude/skills/rapid7-bulk-export/
```

Claude Code will automatically discover and use the skill when relevant.

</details>

<details>
<summary><b>GitHub Copilot (VS Code)</b></summary>

```bash
# Project-level (recommended, stored in repository)
mkdir -p .github/skills/rapid7-bulk-export
cp rapid7-bulk-export-skill/SKILL.md .github/skills/rapid7-bulk-export/

# Or user-level (available across all projects)
mkdir -p ~/.copilot/skills/rapid7-bulk-export
cp rapid7-bulk-export-skill/SKILL.md ~/.copilot/skills/rapid7-bulk-export/
```

Use the skill as a slash command in chat:
```
/rapid7-bulk-export
```

GitHub Copilot will also automatically load the skill when relevant to your request.

</details>

<details>
<summary><b>Gemini CLI</b></summary>

```bash
# User-level (available in all projects)
mkdir -p ~/.gemini/skills/rapid7-bulk-export
cp rapid7-bulk-export-skill/SKILL.md ~/.gemini/skills/rapid7-bulk-export/

# Or workspace-level (only in current project)
mkdir -p .gemini/skills/rapid7-bulk-export
cp rapid7-bulk-export-skill/SKILL.md .gemini/skills/rapid7-bulk-export/
```

Verify the skill is available by running `gemini` and typing `/skills list`.

</details>

<details>
<summary><b>Other AI Assistants</b></summary>

For Claude Desktop and other AI assistants, you can manually include the skill content in your prompts or conversations as needed.

</details>

**Note:** The skill/power provides knowledge and guidance. The MCP server (configured in step 2) executes the actual queries. If using the Kiro Power, the MCP server is configured automatically as part of the power installation.

### 4. Verify Installation

<details>
<summary><b>Kiro</b></summary>

1. Restart or reconnect MCP servers (Command Palette → "MCP: Reconnect All Servers")
2. Check MCP panel for "rapid7-bulk-export" server (should show "Connected")
3. If using the Power: verify it appears in the Powers panel as installed

</details>

<details>
<summary><b>Claude Code (IDE)</b></summary>

1. Restart Claude Code or reload the window
2. Type `/mcp` in chat to check server status
3. Verify "rapid7-bulk-export" appears in the list

</details>

<details>
<summary><b>Claude Desktop</b></summary>

1. Restart Claude Desktop
2. Look for the MCP server icon in the chat interface

</details>

<details>
<summary><b>GitHub Copilot (VS Code)</b></summary>

1. Reload VS Code window
2. Check MCP status in the status bar or output panel

</details>

<details>
<summary><b>Gemini CLI</b></summary>

1. Open your terminal and run `gemini`
2. Type `/mcp` to verify the server is connected

</details>

Try a query:
```
Load the latest vulnerability data from Rapid7
```

**Note:** The first export can take 5+ minutes. Once complete, the data is cached and subsequent loads reuse the same export (if run on the same day).

### 5. Start Analyzing

```
Show me the top 10 critical vulnerabilities
```

Or:

```
What's the severity distribution of my vulnerabilities?
```


## Architecture

```mermaid
graph TB
    subgraph "AI Layer"
        LLM[LLM/AI Assistant<br/>Copilot, Kiro, Claude Desktop, etc.]
    end

    subgraph "Rapid7 Bulk Export MCP Tool"
        MCP[MCP Server<br/>rapid7-bulk-export]
        Skill[Agent Skill / Power<br/>rapid7-bulk-export-skill]
    end

    subgraph "Data Layer"
        DB[(DuckDB<br/>rapid7_bulk_export.db)]
        Tracker[(Export Tracker<br/>rapid7_bulk_export_tracking.db)]
    end

    subgraph "Rapid7 API"
        R7[Rapid7 Bulk Export API<br/>/export/graphql ]
    end

    LLM <-->|Model Context Protocol| MCP
    LLM -.->|Enhanced Context| Skill
    MCP -->|SQL Queries| DB
    MCP -->|Track Exports| Tracker
    MCP -->|Fetch Data| R7
    R7 -->|Parquet Files| MCP
    MCP -->|Load Data| DB

    style LLM fill:#e1f5ff
    style MCP fill:#fff4e1
    style Skill fill:#f0e1ff
    style DB fill:#e8f5e9
    style Tracker fill:#e8f5e9
    style R7 fill:#ffe1e1
```


## Development Quick Start

Changes to the AgentSkill and MCP can be done locally to allow you to tailor to your environment - contributions are welcome back to this repository.

### 1. Clone and Install

```bash
# Clone the repository
git clone https://github.com/rapid7/rapid7-bulk-export-mcp.git
cd rapid7-bulk-export-mcp

# Install dependencies
uv sync

# Or using pip
pip install -e .
```

### 2. Configure MCP Server for Development

Create or edit `.kiro/settings/mcp.json`:

**Option A: Using uv (recommended)**

```json
{
  "mcpServers": {
    "rapid7-bulk-export": {
      "command": "uv",
      "args": ["run", "rapid7-mcp-server"],
      "cwd": "/absolute/path/to/rapid7-bulk-export-mcp",
      "env": {
        "RAPID7_API_KEY": "your-api-key-here",
        "RAPID7_REGION": "your-region"
      }
    }
  }
}
```

**Option B: Using Python directly**

```json
{
  "mcpServers": {
    "rapid7-bulk-export": {
      "command": "python",
      "args": ["-m", "src.mcp_server"],
      "cwd": "/absolute/path/to/rapid7-bulk-export-mcp",
      "env": {
        "RAPID7_API_KEY": "your-api-key-here",
        "RAPID7_REGION": "your-region"
      }
    }
  }
}
```