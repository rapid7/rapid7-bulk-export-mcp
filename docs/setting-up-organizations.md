# Setting up your organizations

Use this guide to report across more than one Rapid7 organization in Claude Desktop. It
takes about one minute per organization.

## Before you start

- Install the Rapid7 Bulk Export extension in Claude Desktop, and set its Rapid7 API Key
  if you already use one organization.
- Get one **Organization API key** for each organization. A Platform Admin creates it in
  the Insight Platform, under Administration, then API Key Management. Each organization
  needs its own key. One key cannot export two organizations.
- Keep the keys open in your password manager. You paste them one at a time.

You do not need to know each organization's region. The setup finds it.

## 1. Ask Claude to open the setup

In Claude Desktop, ask: "Set up my Rapid7 organizations."

A Terminal window opens on macOS, or a console window on Windows, with a short menu.
Never type or paste a key into the Claude chat. Keys go into that window only.

If no window opens, Claude opens a folder instead. Double-click **Set up Rapid7
organizations** in it.

## 2. Add your organizations

Choose **1. Add organizations**. The first time, the setup asks about the organization
you already use:

```
Keep that organization in the list too? (Y/n):        press Return
Name for that organization [primary]:                 press Return, or type a name
Its region (us, us2, ...) [us]:                       press Return, or type its region
```

Then it asks for each new organization:

```
Organization name (press Return to finish): fogcutter
Paste its Organization API key (it will not show):     paste the key, then press Return
  Checking which region accepts this key…
  The key works in region us.
  Add 'fogcutter' in us? (Y/n):                       press Return
  Added 'fogcutter'. Its key is in the macOS Keychain.
```

- The key does not show on the screen when you paste it. That is expected.
- If a key does not work, the setup says why and adds nothing. Paste the correct key at
  the next prompt.
- To finish, press Return at an empty organization name, then choose **5. Quit**.

## 3. Export in Claude

Ask Claude: "Start vulnerability exports for every org in the orgs file."

You do not need to change any setting or restart Claude Desktop. The extension finds the
organizations by itself.

## Changing the list later

Ask Claude to open the setup again, and choose from the menu:

| Menu choice | What it does |
| --- | --- |
| 1. Add organizations | Add more organizations |
| 2. Remove an organization | Remove one organization and delete its stored key, after you confirm |
| 3. Remove every organization | Remove all of them and their stored keys, after you type `clear` |
| 4. Check every key | Test every key again and confirm its region |
| 5. Quit | Close the setup |

When no organizations are left, Claude Desktop goes back to the one organization in the
Rapid7 API Key setting. That setting is never changed by the setup.

## Where your keys are

| Computer | Keys are stored in |
| --- | --- |
| macOS | Keychain |
| Windows | Windows Credential Manager |
| Linux desktop | Secret Service (GNOME Keyring or KWallet) |

The organizations file, `orgs.json` in the extension's data folder (normally
`~/.rapid7_mcp`), holds names and regions only. It never holds a key.

## If Claude cannot open the setup

Run the setup command yourself. Copy the line for your computer into Terminal on macOS or
PowerShell on Windows:

**macOS**

```
"$HOME/Library/Application Support/Claude/Claude Extensions/local.mcpb.rapid7.rapid7-bulk-export/.venv/bin/rapid7-orgs" menu
```

**Windows (PowerShell)**

```
& "$env:APPDATA\Claude\Claude Extensions\local.mcpb.rapid7.rapid7-bulk-export\.venv\Scripts\rapid7-orgs.exe" menu
```

The same command also takes `list`, `check`, `remove NAME`, and `clear` in place of
`menu`.

## Servers and Docker

A server has no key store and no desktop. Put each key in an environment variable,
list the organizations in an orgs file that names those variables, and set
`RAPID7_ORGS_FILE` to that file.
