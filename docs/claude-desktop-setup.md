# Running the development version in Claude Desktop

For someone who has never installed an MCP server into Claude Desktop. Written for
macOS. It covers what you must build first, the two ways to install, and how to point
Claude Desktop at your local working copy rather than at a published release.

## What you are installing, in plain terms

Two separate things, and the demo needs both:

1. **The MCP server.** A small Python program that Claude Desktop launches in the
   background. It holds the tools (`start_rapid7_multi_org_export` and the rest).
2. **The agent skill.** A markdown instruction file that teaches Claude how and when to
   use those tools. Without it Claude will keep reaching for the single-org tools and
   will not group results by organization.

## Do I need to build anything first?

**No compile step.** It is Python, so nothing is compiled. But you do need one of two
things depending on which install route you pick.

- **Route A, manual config (recommended for the development version).** Nothing to
  build. Claude Desktop runs `uv` against your checkout, so your edits take effect on
  the next restart. This is the route to use while the branch is uncommitted.
- **Route B, packaged bundle.** You build a `.mcpb` file with `make package-mcpb`, which
  needs the packer installed: `npm install -g @anthropic-ai/mcpb`. The bundle is a
  snapshot, so you must rebuild it after every code change. Better for handing to
  someone else than for your own iteration.

You need `uv` either way. Check with `uv --version`. If it is missing, install it from
the official instructions at astral.sh and confirm the URL before running anything.

Prove the code works before involving Claude Desktop at all:

```sh
cd ~/rapid7/rapid7-bulk-export-mcp
uv run pytest -q
```

177 passing and 1 skipped is the expected result. If that fails, fix it before going
further, because a broken server inside Claude Desktop is much harder to diagnose.

---

## Removing an older install

Do this before anything else. An older build offering the same tool names will make the
demo unpredictable, and worse, it can share the same database.

Check what is there:

```sh
# Installed as a Claude Desktop extension
ls "$HOME/Library/Application Support/Claude/Claude Extensions" | grep -i rapid7

# Its version and configured settings
python3 -c "import json;d=json.load(open('$HOME/Library/Application Support/Claude/Claude Extensions/ant.dir.gh.rapid7.rapid7-bulk-export-mcp/manifest.json'));print(d['name'],d['version'],len(d['tools']),'tools')"

# Installed as a Python package on PATH
which -a rapid7-mcp-server
python3 -m pip list 2>/dev/null | grep -i rapid7

# Configured in other clients
grep -o 'rapid7[a-z-]*' ~/.kiro/settings/mcp.json ~/.gemini/settings.json 2>/dev/null
grep -c rapid7 "$HOME/Library/Application Support/Claude/claude_desktop_config.json" 2>/dev/null
```

To remove the extension: Claude Desktop, Settings, Extensions, find Rapid7 Bulk Export,
then disable or uninstall. Disabling is enough and is reversible, which is what you want
if the release is your fallback. Fully quit Claude Desktop with Cmd Q afterwards.

If a `pip` or `uv pip` install put `rapid7-mcp-server` on your PATH, remove it so nothing
can launch the wrong copy:

```sh
uv pip uninstall rapid7-vulnerability-export   # from inside that virtualenv
```

**The database matters more than the duplicate tools.** An older build has no org-scoped
load mode, so if it ever loads while pointed at the same directory, it drops and recreates
the `vulnerabilities` table and every org but the last one disappears. Two defences, use
both: disable the old extension, and set `DATA_DIR` on the development server as shown
below.

## Route A: point Claude Desktop at your working copy

### A1. Find the config file

```sh
open ~/Library/Application\ Support/Claude/
```

The file is `claude_desktop_config.json`. If it does not exist, create it. If it does,
back it up first:

```sh
cp ~/Library/Application\ Support/Claude/claude_desktop_config.json \
   ~/Library/Application\ Support/Claude/claude_desktop_config.json.bak
```

### A2. Get the absolute path to `uv`

Claude Desktop does not inherit your shell's `PATH`, so a bare `uv` will fail with
"command not found" and the server will show as disconnected. Get the real path:

```sh
which uv
```

Typically `/Users/<you>/.local/bin/uv` or `/opt/homebrew/bin/uv`. Use whatever that
prints, in full, below.

### A3. Write the config

Replace `<uv-path>` with the output of `which uv`, and `jearls` with your username.

```json
{
  "mcpServers": {
    "rapid7-bulk-export-dev": {
      "command": "<uv-path>",
      "args": [
        "run",
        "--directory",
        "/Users/jearls/rapid7/rapid7-bulk-export-mcp",
        "run_server.py"
      ],
      "env": {
        "RAPID7_API_KEY": "",
        "RAPID7_REGION": "us",
        "RAPID7_ORGS_FILE": "/Users/jearls/.rapid7_mcp/orgs.json",
        "DATA_DIR": "/Users/jearls/.rapid7_mcp_dev"
      }
    }
  }
}
```

Notes on that block:

- `--directory` is what makes this the development version. `uv` resolves the project
  and its dependencies from that folder, so your uncommitted branch is what runs.
- Give it a distinct name (`rapid7-bulk-export-dev`) so it cannot be confused with a
  released copy installed from the connectors directory. See "Removing an older install"
  below, and do that first: two servers offering the same tool names is the fastest way
  to have a confusing demo.
- `DATA_DIR` is not optional here, it is a safety measure. The released extension sets no
  `DATA_DIR`, so it falls back to `~/.rapid7_mcp`. If the development server used the same
  directory, the older build could load in snapshot mode over the top of the multi-org
  data and drop every org but the last. A separate directory makes that impossible.
- `RAPID7_API_KEY` is optional when `RAPID7_ORGS_FILE` is set, because each
  organization's key is resolved from the org list. To keep that key as one of the
  organizations, give its entry the `key_ref` `RAPID7_API_KEY`.
- Keys are not in this file. `RAPID7_ORGS_FILE` points at a list of *names* of Keychain
  entries. Run `uv run scripts/add_rapid7_org.py` to store the keys and write that file.

### A4. Restart Claude Desktop

Fully quit it (Cmd Q), do not just close the window, then reopen.

### A5. Confirm it connected

In Claude Desktop, open Settings, then Developer. The server should be listed as
running. If it is not, open its logs:

```sh
tail -50 ~/Library/Logs/Claude/mcp-server-rapid7-bulk-export-dev.log
```

Then ask Claude in a new chat:

> Which organizations are configured for Rapid7 reporting?

If it calls `list_rapid7_orgs` and lists your organizations, the server is wired up
correctly.

---

## Route B: build and install the bundle

Only if you want a single file to hand to someone else.

```sh
cd ~/rapid7/rapid7-bulk-export-mcp
npm install -g @anthropic-ai/mcpb   # one time
make package-mcpb
```

That produces `rapid7-bulk-export-<version>.mcpb` in the repo root. Open Claude Desktop,
go to Settings, then Extensions, and drag the `.mcpb` file in. It will prompt for the
values declared in `manifest.json`: the API key, the region, and the organizations file
path.

Remember the bundle is a snapshot of the code at build time. After any edit you must
`make package-mcpb` again and reinstall. That is why Route A is better while iterating.

---

## Installing the agent skill

The server gives Claude the tools. The skill tells it how to use them. Build the zip:

```sh
cd ~/rapid7/rapid7-bulk-export-mcp
make package-skill
```

That writes `rapid7-bulk-export-skill-<version>.zip` in the repo root. In Claude Desktop:
Settings, then Capabilities or Skills depending on your version, then add a skill and
upload that zip.

The skill in this branch contains the multi-organization section. If you upload an older
zip, or skip this step, Claude will still have the new tools but will not know the
correct order to call them in, and will not know to gate on `get_rapid7_org_coverage`
before quoting totals. Do not skip it.

## Adding a second organization to a single-org setup

If you already use one API key, you do not start again:

1. Run `uv run scripts/add_rapid7_org.py`.
2. Answer yes when it asks to keep the key you already use (`RAPID7_API_KEY`). Give that
   organization a label and a region.
3. Add the new organization at the next prompt. The script stores its key in the
   Keychain and writes the orgs file with both organizations. It does not store the
   existing key again. That organization's entry references `RAPID7_API_KEY`.
4. Set the organizations file in Claude Desktop to the path the script prints. Keep the
   Rapid7 API Key setting as it is, because the first organization uses it.
5. Quit and reopen Claude Desktop, then export every organization.

The first multi-org export of your original organization replaces its single-org data,
so its totals are not counted twice.

To go back to one organization, clear the organizations file setting in Claude Desktop
and restart it. The server then uses the Rapid7 API Key alone, as before. The next
export replaces the data with that organization only.

## Verifying the whole thing end to end

In a new Claude Desktop chat, in this order:

1. "Which organizations are configured for Rapid7 reporting?" expects `list_rapid7_orgs`.
2. "Start a vulnerability export for every configured organization." expects
   `start_rapid7_multi_org_export`.
3. "Check the multi-org export progress." expects `check_rapid7_multi_org_export`.
4. "Show the org coverage." expects `get_rapid7_org_coverage` and must say counts match.

If step 1 works but Claude improvises at step 2, the skill is not installed or not
active. Ask it directly to use `start_rapid7_multi_org_export` and fix the skill after
the demo.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Server shows as disconnected | Bare `uv` in `command` | Use the absolute path from `which uv` |
| "command not found" in the log | Same as above | Same as above |
| Server starts, tools missing | Stale Claude Desktop process | Fully quit with Cmd Q and reopen |
| `RAPID7_API_KEY not found` | A single-org code path was reached | Put one organization key in `RAPID7_API_KEY` |
| `list_rapid7_orgs` says not usable | A key is missing from the Keychain, or two organizations share one key | Fix the Keychain entry, then restart Claude Desktop |
| Duplicate or odd tool behaviour | The released extension is installed as well | Disable the released one for the demo |
| Claude ignores the multi-org flow | Skill not installed or outdated | `make package-skill` and upload the new zip |
| Edits do not take effect | Running Route B, or a stale process | Rebuild the bundle, or switch to Route A and restart |

## Where the local data lives

```text
~/.rapid7_mcp/rapid7_bulk_export.db           the queryable data
~/.rapid7_mcp/rapid7_bulk_export_tracking.db  which export belongs to which org
```

To start clean before a demo, ask Claude to purge the local Rapid7 data, or delete those
two files while Claude Desktop is closed.

Worth saying plainly: this database holds several customers' vulnerability data side by
side on your laptop. Keep it on the local stdio setup above. The Docker HTTP mode shares
one database between everyone who can reach the endpoint, which is not appropriate for
multi-tenant data without access control in front of it.
