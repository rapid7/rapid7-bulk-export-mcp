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

You do not need to know each organization's region. The setup command finds it.

## 1. Open a terminal

- **macOS:** open Terminal (Applications, then Utilities).
- **Windows:** open PowerShell (Start, then type PowerShell).

## 2. Start the setup command

The command installs with the extension. Copy the line for your computer:

**macOS**

```
"$HOME/Library/Application Support/Claude/Claude Extensions/local.mcpb.rapid7.rapid7-bulk-export/.venv/bin/rapid7-orgs"
```

**Windows (PowerShell)**

```
& "$env:APPDATA\Claude\Claude Extensions\local.mcpb.rapid7.rapid7-bulk-export\.venv\Scripts\rapid7-orgs.exe"
```

If the command is not found, open Claude Desktop once after you install the extension,
then try again. Claude Desktop finishes the install when it first starts the extension.

## 3. Answer the questions

The first time, the command asks about the organization you already use:

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
- If a key does not work, the command says why and adds nothing. Paste the correct key
  at the next prompt.
- To finish, press Return at an empty organization name.

The command then lists every organization and prints the path of your organizations
file.

## 4. Point Claude Desktop at the organizations file

1. In Claude Desktop, open Settings, then Extensions, then Rapid7 Bulk Export, then
   Configure.
2. Set **Organizations file** to the path the command printed.
3. Keep the **Rapid7 API Key** setting as it is, if you kept that organization in step 3.
4. Quit Claude Desktop completely, then open it again.

## 5. Check it in Claude

Ask Claude: "Which organizations are configured for Rapid7 reporting?"

Claude lists every organization and its region. It never shows a key.

Then ask: "Start vulnerability exports for every org in the orgs file."

## Changing the list later

Run the same command with one of these words after it:

| Command | What it does |
| --- | --- |
| `rapid7-orgs` | Add more organizations |
| `rapid7-orgs list` | Show each organization, its region, and whether its key is stored |
| `rapid7-orgs remove fogcutter` | Remove one organization and delete its stored key |
| `rapid7-orgs check` | Test every key again and confirm its region |

After any change, quit and reopen Claude Desktop.

To go back to one organization, clear the **Organizations file** setting in Claude
Desktop, then restart it.

## Where your keys are

| Computer | Keys are stored in |
| --- | --- |
| macOS | Keychain |
| Windows | Windows Credential Manager |
| Linux desktop | Secret Service (GNOME Keyring or KWallet) |

The organizations file holds names and regions only. It never holds a key. On a server
or in Docker, with no key store, put each key in an environment variable instead and
name that variable in the organizations file.
