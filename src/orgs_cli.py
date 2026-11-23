"""rapid7-orgs: set up the organizations for multi-org reporting, on macOS, Windows, or Linux.

Run it with no arguments for a guided walkthrough. Each organization needs a name and its
own Organization API key. The command finds the key's region, checks the key works, stores
it in this computer's key store, and lists the organization in the orgs file. The orgs file
holds names and regions only, never a key.

    rapid7-orgs            add organizations, one at a time
    rapid7-orgs list       show the organizations, never their keys
    rapid7-orgs remove X   remove organization X and its stored key
    rapid7-orgs check      re-check every key and its region
    rapid7-orgs menu       choose a task from a menu (the window Claude opens)
"""

import argparse
import getpass
import json
import os
import re
import sys
import tempfile
from typing import Dict, List, Optional

from . import keystore
from .config import REGION_ENDPOINTS
from .region_detect import detect_region

DEFAULT_ORGS_FILE = os.path.join("~", ".rapid7_mcp", "orgs.json")
SINGLE_ORG_KEY_REF = "RAPID7_API_KEY"
KEY_REF_PREFIX = "R7_KEY_"
LABEL = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")


def orgs_path(raw: Optional[str] = None) -> str:
    return os.path.expanduser(raw or os.environ.get("RAPID7_ORGS_FILE") or DEFAULT_ORGS_FILE)


def key_ref_for(label: str) -> str:
    return KEY_REF_PREFIX + re.sub(r"[^A-Z0-9]", "_", label.upper())


def normalize_label(raw: str) -> str:
    label = raw.strip().lower().replace(" ", "-")
    if not LABEL.match(label):
        raise ValueError("A name may use letters, digits, dashes and underscores, up to 48 characters.")
    return label


def load_orgs(path: str) -> List[Dict[str, str]]:
    try:
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError as e:
        raise ValueError(f"{path} is not valid JSON: {e}") from e
    orgs = document.get("orgs") if isinstance(document, dict) else None
    if not isinstance(orgs, list):
        raise ValueError(f"{path} must contain an 'orgs' list")
    return orgs


def write_orgs(path: str, orgs: List[Dict[str, str]]) -> None:
    """Write the orgs file atomically, readable only by this user."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    handle = tempfile.NamedTemporaryFile("w", dir=directory, delete=False, encoding="utf-8")
    try:
        with handle:
            json.dump({"orgs": orgs}, handle, indent=2)
            handle.write("\n")
        os.chmod(handle.name, 0o600)
        os.replace(handle.name, path)
    except BaseException:
        os.unlink(handle.name)
        raise


def resolve(key_ref: str) -> Optional[str]:
    """Find a key the same way the server does: the environment first, then the key store."""
    return os.environ.get(key_ref) or keystore.read(key_ref)


def key_status(org: Dict[str, str]) -> str:
    if resolve(org.get("key_ref", "")):
        return "key stored"
    if org.get("key_ref") == SINGLE_ORG_KEY_REF:
        return "key set in Claude Desktop"
    return "KEY MISSING"


def ask(prompt: str, default: str = "") -> str:
    answer = input(f"{prompt} [{default}]: " if default else f"{prompt}: ").strip()
    return answer or default


def confirm(prompt: str) -> bool:
    return ask(f"{prompt} (Y/n)").lower() in ("", "y", "yes")


def _carry_over(path: str) -> Optional[Dict[str, str]]:
    """Offer to keep the key the customer already uses as the first organization."""
    print("You may already use one Rapid7 API key in Claude Desktop (the Rapid7 API Key setting).")
    if not confirm("Keep that organization in the list too?"):
        return None
    label = normalize_label(ask("Name for that organization", "primary"))
    default_region = os.environ.get("RAPID7_REGION", "us")
    region = ask(f"Its region ({', '.join(REGION_ENDPOINTS)})", default_region).lower()
    if region not in REGION_ENDPOINTS:
        raise ValueError(f"'{region}' is not a Rapid7 region.")
    print(f"  '{label}' will use the Rapid7 API Key from Claude Desktop. Keep that setting as it is.\n")
    return {"label": label, "key_ref": SINGLE_ORG_KEY_REF, "region": region}


def _add_one(orgs: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Ask for one organization. Return its entry, None when the customer is done."""
    raw = input("Organization name (press Return to finish): ").strip()
    if not raw:
        return None
    label = normalize_label(raw)
    if any(org.get("label") == label for org in orgs):
        raise ValueError(f"'{label}' is already in the list. Choose another name, or remove it first.")

    key = getpass.getpass("Paste its Organization API key (it will not show): ").strip()
    if not key:
        raise ValueError("No key was entered, so nothing was added.")
    for org in orgs:
        if resolve(org.get("key_ref", "")) == key:
            raise ValueError(f"That key is already used by '{org['label']}'. Each organization needs its own key.")

    print("  Checking which region accepts this key…")
    found = detect_region(key)
    if not found.region:
        raise ValueError(f"{found.problem} Nothing was added.")
    print(f"  The key works in region {found.region}.")
    if not confirm(f"  Add '{label}' in {found.region}?"):
        raise ValueError("Skipped. Nothing was added.")

    key_ref = key_ref_for(label)
    keystore.store(key_ref, key)
    print(f"  Added '{label}'. Its key is in the {keystore.name()}.\n")
    return {"label": label, "key_ref": key_ref, "region": found.region}


def command_add(path: str) -> int:
    if not keystore.available():
        print(
            "This computer has no key store that the command can use, so keys cannot be stored here safely.\n"
            "On a server, set one environment variable per organization instead, and list them in the orgs file.",
            file=sys.stderr,
        )
        return 1
    orgs = load_orgs(path)
    print(f"Organizations file: {path}")
    print(f"Keys are stored in: {keystore.name()}\n")
    pending = _carry_over(path) if not os.path.exists(path) else None

    added = 0
    while True:
        try:
            entry = _add_one(orgs + ([pending] if pending else []))
        except ValueError as e:
            print(f"  {e}\n")
            continue
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if entry is None:
            break
        if pending:
            orgs, pending = [pending], None
        orgs.append(entry)
        write_orgs(path, orgs)
        added += 1

    if not added:
        print("Nothing was added.")
        return 0
    print(f"{len(orgs)} organization(s) in {path}:")
    command_list(path)
    print("\nNext:")
    print("  1. In Claude Desktop, open Settings, then Extensions, then Rapid7 Bulk Export, then Configure.")
    print(f"  2. Set Organizations file to: {path}")
    print("  3. Quit and reopen Claude Desktop.")
    print('  4. Ask Claude: "Start vulnerability exports for every org in the orgs file."')
    return 0


def command_list(path: str) -> int:
    orgs = load_orgs(path)
    if not orgs:
        print(f"No organizations in {path}. Run rapid7-orgs to add some.")
        return 0
    for org in orgs:
        print(f"  {org.get('label', '?'):<24} {org.get('region', '?'):<5} {key_status(org)}")
    return 0


def command_remove(path: str, raw_label: str) -> int:
    label = normalize_label(raw_label)
    orgs = load_orgs(path)
    match = next((org for org in orgs if org.get("label") == label), None)
    if match is None:
        print(f"'{label}' is not in {path}.", file=sys.stderr)
        return 1
    write_orgs(path, [org for org in orgs if org is not match])
    if match.get("key_ref", "").startswith(KEY_REF_PREFIX):
        keystore.remove(match["key_ref"])
    print(f"Removed '{label}'. Restart Claude Desktop to stop using it.")
    if len(orgs) <= 2:
        print("To go back to one organization, clear the Organizations file setting in Claude Desktop.")
    return 0


def command_check(path: str) -> int:
    orgs = load_orgs(path)
    if not orgs:
        print(f"No organizations in {path}.")
        return 1
    problems = 0
    seen: Dict[str, str] = {}
    for org in orgs:
        label, region = org.get("label", "?"), org.get("region", "?")
        key = resolve(org.get("key_ref", ""))
        if not key:
            status = key_status(org)
            problems += status == "KEY MISSING"
        elif key in seen:
            status, problems = f"SAME KEY AS {seen[key]}", problems + 1
        else:
            seen[key] = label
            found = detect_region(key)
            if found.region == region:
                status = "key works"
            elif found.region:
                status, problems = f"WRONG REGION: the key works in {found.region}", problems + 1
            else:
                status, problems = f"NOT WORKING: {found.problem}", problems + 1
        print(f"  {label:<24} {region:<5} {status}")
    print("All organizations are ready." if not problems else f"{problems} problem(s) to fix.")
    return 0 if not problems else 1


MENU = ("Add organizations", "Remove an organization", "Check every key", "Quit")


def command_menu(path: str) -> int:
    """The window Claude opens: the organizations, then a choice of task, until Quit."""
    while True:
        print(f"\nRapid7 organizations in {path}:")
        command_list(path)
        print()
        for number, item in enumerate(MENU, 1):
            print(f"  {number}. {item}")
        try:
            choice = input(f"Choose 1 to {len(MENU)}: ").strip()
            if choice == "1":
                command_add(path)
            elif choice == "2":
                name = input("Name of the organization to remove: ").strip()
                if name and ask(f"Remove '{name}' and its stored key? (y/N)").lower() in ("y", "yes"):
                    command_remove(path, name)
                else:
                    print(f"Kept '{name}'.")
            elif choice == "3":
                command_check(path)
            elif choice in ("4", "q", "quit"):
                break
        except ValueError as e:
            print(f"  {e}")
        except (EOFError, KeyboardInterrupt):
            break
    try:
        input("\nPress Return to close this window.")
    except (EOFError, KeyboardInterrupt):
        pass
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rapid7-orgs", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--orgs-file", help=f"The orgs file (default: $RAPID7_ORGS_FILE, else {DEFAULT_ORGS_FILE})")
    parser.add_argument("command", nargs="?", default="add", choices=["add", "list", "remove", "check", "menu"])
    parser.add_argument("name", nargs="?", help="The organization to remove")
    args = parser.parse_args(argv)
    path = orgs_path(args.orgs_file)
    try:
        if args.command == "list":
            return command_list(path)
        if args.command == "remove":
            if not args.name:
                parser.error("remove needs the name of the organization")
            return command_remove(path, args.name)
        if args.command == "check":
            return command_check(path)
        if args.command == "menu":
            return command_menu(path)
        return command_add(path)
    except (ValueError, keystore.KeyStoreUnavailable) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
