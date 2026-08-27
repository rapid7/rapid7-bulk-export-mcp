# Rapid7 InsightIDR Investigations Expert - Agent Skill

This is a standalone agent skill that provides domain expertise for triaging, reviewing, and closing Rapid7 InsightIDR investigations via the InsightIDR REST API (Investigations v2, preview).

## What is This?

An agent skill is a markdown file that gives AI assistants (like Claude, Kiro) knowledge about a specific domain. This skill provides:

- Understanding of InsightIDR investigation and alert data
- The mandatory safety flow for closing an investigation (preview, then explicit confirmation)
- Status and disposition reference values
- Common triage patterns
- Error handling guidance

## Installation

### For Kiro

Copy the skill file to your Kiro skills directory:

```bash
# User-level (available in all workspaces)
cp SKILL.md ~/.kiro/skills/rapid7-insightidr.md

# Workspace-level (only in current workspace)
cp SKILL.md .kiro/skills/rapid7-insightidr.md
```

### For Other AI Assistants

The skill file is just a markdown document. You can:
- Include it in your prompts
- Add it to your context
- Reference it in conversations

## Usage

### In Kiro

Activate the skill in chat:

```
#rapid7-insightidr
```

Or reference it when asking questions:

```
Using #rapid7-insightidr, show me all open critical investigations
```

### What It Does

The skill provides:
- ✅ Domain knowledge about InsightIDR investigations and alerts
- ✅ The required preview-then-confirm flow for closing investigations
- ✅ Status/disposition/priority reference
- ✅ Triage and analysis patterns
- ❌ Does NOT call the API itself (see MCP server for that)

## Skill-Only vs Full MCP Setup

### Skill Only (This Package)
- **What you get**: AI guidance on triage patterns and safe close procedure
- **What you need**: Just copy the .md file
- **Actions**: You review and act on investigations manually
- **Best for**: Learning the workflow, understanding the safety flow before automating it

### With MCP Server (Full Package)
- **What you get**: AI can actually list, inspect, and close investigations
- **What you need**: Install the full package and configure the MCP server (`RAPID7_API_KEY` + `RAPID7_REGION` — same credentials as the InsightVM Bulk Export tools)
- **Actions**: AI executes `get_investigations`, `get_investigation_details`, and `close_investigation` directly
- **Best for**: Interactive triage, SOC workflows

See the [main package documentation](https://github.com/rozumeyroman/rapid7-bulk-export-mcp) for MCP server setup.

## Currently Available MCP Tools

As of this version, the skill covers three MCP tools:

| Tool | Purpose |
|---|---|
| `get_investigations(status?, priorities?, assignee_email?, limit?)` | List/search investigations |
| `get_investigation_details(investigation_id)` | Full record + associated alerts for one investigation |
| `close_investigation(investigation_id, disposition, confirm?)` | Close one investigation — two-step confirm required |

Not yet covered by this skill: Log Search (LEQL), Detection Rules management, Comments, assigning investigations to a user, and setting priority/disposition without closing. These will be added here as the corresponding MCP tools are implemented.

## Example Usage

### Listing open investigations

```
You: #rapid7-insightidr Show me all open critical and high priority investigations

AI: Found 3 investigation(s):
- [CRITICAL] Suspicious Authentication - ...
    status: OPEN  assignee: Unassigned
- [HIGH] ...
...
```

### Getting investigation details

```
You: #rapid7-insightidr Give me full details on investigation <id>

AI: [returns the full investigation record and its associated alerts —
     detection rule, alert source, timestamps]
```

### Closing an investigation (safety flow)

```
You: #rapid7-insightidr Close investigation <id> as benign

AI: ⚠️ This will CLOSE the following investigation with disposition 'BENIGN'.
    No changes have been made yet.
    Title: ...
    ...
    To proceed, call close_investigation(..., confirm=True)

You: yes, go ahead

AI: ✓ Investigation closed.
```

## Limitations

This skill provides knowledge but cannot:
- Call the InsightIDR API itself
- Close investigations in bulk by filter (the underlying tool intentionally only closes one explicit investigation ID at a time)
- Guarantee API behavior — the Investigations v2 API is in **preview mode** and may change without notice

For actual API access, install the full package with MCP server support.

## Related Resources

- [Main Package with MCP Server](https://github.com/rozumeyroman/rapid7-bulk-export-mcp)
- [Rapid7 InsightIDR REST API Documentation](https://docs.rapid7.com/insightidr/insightidr-rest-api/)
- [Rapid7 Command Platform API Overview](https://docs.rapid7.com/insight/api-overview/)

## License

MIT License - See main package for details

## Contributing

Found an issue or have a suggestion? Please open an issue in the main repository.
