# Rapid7 Bulk Export Analysis Expert - Agent Skill

This is a standalone agent skill that provides domain expertise for analyzing Rapid7 InsightVM data exported via the Bulk Export API.

## What is This?

An agent skill is a markdown file that gives AI assistants (like Kiro) knowledge about a specific domain. This skill provides:

- Understanding of vulnerability data schema from Bulk Export API
- Common analysis patterns
- SQL query examples
- Best practices for security analysis

## Installation

### For Kiro

Copy the skill file to your Kiro skills directory:

```bash
# User-level (available in all workspaces)
cp SKILL.md ~/.kiro/skills/rapid7-bulk-export.md

# Workspace-level (only in current workspace)
cp SKILL.md .kiro/skills/rapid7-bulk-export.md
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
#rapid7-bulk-export
```

Or reference it when asking questions:

```
Using #rapid7-bulk-export, show me how to find critical vulnerabilities
```

### What It Does

The skill provides:
- ✅ Domain knowledge about vulnerability analysis
- ✅ SQL query patterns and examples
- ✅ Schema documentation
- ✅ Best practices and tips
- ❌ Does NOT execute queries (see MCP server for that)

## Skill-Only vs Full MCP Setup

### Skill Only (This Package)
- **What you get**: AI guidance and query suggestions
- **What you need**: Just copy the .md file
- **Queries**: You execute manually
- **Best for**: Learning, getting suggestions, understanding patterns

### With MCP Server (Full Package)
- **What you get**: AI can actually query your database
- **What you need**: Install full package + configure MCP
- **Queries**: AI executes automatically
- **Best for**: Interactive analysis, automated insights

See the [full package documentation](https://github.com/yourusername/rapid7-vulnerability-export) for MCP server setup.

## Example Usage

### Getting Query Suggestions

```
You: #rapid7-bulk-export How do I find vulnerabilities with known exploits?

AI: Here's a SQL query to find vulnerabilities with known exploits:

SELECT vulnId, title, cvssV3Score, exploits, severity
FROM vulnerabilities
WHERE exploits IS NOT NULL
ORDER BY cvssV3Score DESC
LIMIT 20;

This will show you the top 20 vulnerabilities that have known exploits,
ordered by CVSS score...
```

### Understanding the Schema

```
You: #rapid7-bulk-export What columns are available for filtering?

AI: The vulnerabilities table has several key columns for filtering:

Identification:
- vulnId, assetId, instanceId

Severity/Risk:
- severity (Critical, Severe, Moderate, etc.)
- cvssV3Score (0-10)
- riskScore

Temporal:
- firstFoundTimestamp
- lastSeenTimestamp
...
```

### Analysis Guidance

```
You: #rapid7-bulk-export How should I prioritize remediation?

AI: For effective vulnerability remediation prioritization, consider:

1. Risk Factors:
   - CVSS score >= 9.0 (critical)
   - Known exploits present
   - Public-facing assets

2. Query Pattern:
   SELECT assetId, vulnId, title, cvssV3Score
   FROM vulnerabilities
   WHERE severity = 'Critical'
     AND exploits IS NOT NULL
   ORDER BY cvssV3Score DESC
...
```

## Limitations

This skill provides knowledge but cannot:
- Execute queries against your database
- Access your actual vulnerability data
- Automatically analyze your specific environment

For those capabilities, install the full package with MCP server support.

## Updating

To update the skill:

```bash
# Download new version
curl -O https://raw.githubusercontent.com/rapid7/rapid7-bulk-export-mcp/main/rapid7-bulk-export-skill/SKILL.md

# Copy to skills directory
cp SKILL.md ~/.kiro/skills/rapid7-bulk-export.md
```

## Related Resources

- [Full Package with MCP Server](https://github.com/rapid7/rapid7-bulk-export-mcp)
- [Rapid7 InsightVM Documentation](https://docs.rapid7.com/insightvm/)
- [Rapid7 Bulk Export API](https://docs.rapid7.com/insightvm/bulk-export-api/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)

## License

MIT License - See main package for details

## Contributing

Found an issue or have a suggestion? Please open an issue in the main repository.
