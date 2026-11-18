#!/usr/bin/env python3
"""Add Rapid7 organization API keys to the macOS Keychain and the orgs file.

Prompts for one organization at a time, so you paste a key instead of typing a
long `security add-generic-password` line. The orgs file never holds a key, only
the name of the Keychain entry that does.

By default `security` prompts for the key itself, so the key never enters this
process, never appears in the process list, and never reaches shell history.
Use --key-from-stdin only to pipe a key from a password manager.

Run from the repo root:

    uv run scripts/add_rapid7_org.py
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

try:
    from src.config import REGION_ENDPOINTS
except ImportError:  # pragma: no cover
    sys.exit("Run this from the repo root, for example: uv run scripts/add_rapid7_org.py")

DEFAULT_ORGS_FILE = "~/.rapid7_mcp/orgs.json"
KEYCHAIN_ACCOUNT = "rapid7"
KEY_REF_PREFIX = "R7_KEY_"
LABEL_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def normalize_label(raw):
    """Lowercase and validate an organization label.

    The label is the name shown in reports, and load_org_configs() rejects a
    duplicate, so it is normalized here rather than taken verbatim.
    """
    label = raw.strip().lower()
    if not label:
        raise ValueError("Label must not be empty")
    if not LABEL_PATTERN.match(label):
        raise ValueError(
            f"Label '{label}' is not usable. Use lowercase letters, digits, dashes and underscores, "
            "and start with a letter or digit."
        )
    return label


def key_ref_for(label):
    """Derive the Keychain entry name for a label."""
    return KEY_REF_PREFIX + re.sub(r"[^A-Z0-9]", "_", label.upper())


def validate_region(raw, default):
    """Resolve and validate a region, falling back to the default when empty."""
    region = (raw or "").strip().lower() or default
    if region not in REGION_ENDPOINTS:
        valid = ", ".join(sorted(REGION_ENDPOINTS))
        raise ValueError(f"Invalid region '{region}'. Valid regions are: {valid}")
    return region


def merge_org(orgs, entry):
    """Return the org list with entry added, replacing any entry of the same label."""
    merged = [org for org in orgs if org.get("label") != entry["label"]]
    merged.append(entry)
    return merged


def load_orgs(path):
    """Read the orgs array from path, returning an empty list when the file is absent."""
    try:
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError as e:
        raise ValueError(f"{path} is not valid JSON: {e}") from e

    orgs = document.get("orgs") if isinstance(document, dict) else None
    if orgs is None:
        raise ValueError(f"{path} must contain an 'orgs' array")
    if not isinstance(orgs, list):
        raise ValueError(f"{path} has an 'orgs' key that is not an array")
    return orgs


def write_orgs(path, orgs):
    """Write the orgs array to path atomically, readable only by this user."""
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


def mask(secret):
    """Render a secret as its last four characters only."""
    return f"...{secret[-4:]}" if secret and len(secret) >= 4 else "...(short)"


def keychain_get(key_ref):
    """Return the stored key for key_ref, or None when there is no such entry."""
    result = subprocess.run(
        ["security", "find-generic-password", "-s", key_ref, "-a", KEYCHAIN_ACCOUNT, "-w"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def keychain_set_prompted(key_ref):
    """Store a key under key_ref, letting `security` read it from the terminal.

    Passing -w last makes security prompt for the key itself, which keeps the key
    out of this process and out of the argument list.
    """
    result = subprocess.run(
        ["security", "add-generic-password", "-U", "-a", KEYCHAIN_ACCOUNT, "-s", key_ref, "-w"],
        check=False,
    )
    return result.returncode == 0


def keychain_set_value(key_ref, key):
    """Store a key under key_ref by passing it as an argument.

    Less safe than keychain_set_prompted, because the key is briefly visible to
    this user's other processes. Only used for --key-from-stdin.
    """
    result = subprocess.run(
        ["security", "add-generic-password", "-U", "-a", KEYCHAIN_ACCOUNT, "-s", key_ref, "-w", key],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def resolve_key(key_ref):
    """Resolve a key the same way the server does: environment first, then Keychain."""
    return os.environ.get(key_ref) or keychain_get(key_ref)


def verify(orgs):
    """Report how every org resolves and return True when the set is usable."""
    if not orgs:
        print("No organizations configured yet.")
        return False

    print("\nVerifying:")
    seen_keys = {}
    unresolved = []
    duplicates = []

    for org in orgs:
        label = org.get("label", "?")
        key_ref = org.get("key_ref", "?")
        region = org.get("region", "(default)")
        key = resolve_key(key_ref)
        if not key:
            unresolved.append(f"{label} (key_ref: {key_ref})")
            print(f"  {label:<20} {key_ref:<28} {region:<5} MISSING")
            continue
        if key in seen_keys:
            duplicates.append(f"{label} shares a key with {seen_keys[key]}")
            print(f"  {label:<20} {key_ref:<28} {region:<5} DUPLICATE of {seen_keys[key]}")
            continue
        seen_keys[key] = label
        print(f"  {label:<20} {key_ref:<28} {region:<5} ok  {mask(key)}")

    org_word = "organization" if len(orgs) == 1 else "organizations"
    key_word = "key" if len(seen_keys) == 1 else "keys"
    print(f"\n{len(orgs)} {org_word}, {len(seen_keys)} distinct {key_word}.")
    for problem in unresolved:
        print(f"  MISSING: {problem}")
    for problem in duplicates:
        print(f"  DUPLICATE: {problem}")
    if unresolved or duplicates:
        print("\nFix the problems above. list_rapid7_orgs will fail on them.")
        return False
    print("All keys resolve and each organization has its own key.")
    return True


def prompt_one(default_region, read_key_from_stdin):
    """Prompt for one organization. Return the entry, or None when the user is done."""
    raw_label = input("\nLabel (empty to finish): ")
    if not raw_label.strip():
        return None
    label = normalize_label(raw_label)
    region = validate_region(input(f"Region [{default_region}]: "), default_region)
    key_ref = key_ref_for(label)

    if keychain_get(key_ref):
        print(f"Keychain entry {key_ref} already exists and will be replaced.")

    if read_key_from_stdin:
        key = sys.stdin.readline().strip()
        if not key:
            raise ValueError(f"No key arrived on stdin for '{label}'")
        stored = keychain_set_value(key_ref, key)
    else:
        print(f"Keychain entry: {key_ref}")
        print("Paste the API key at the prompt below, then press Return.")
        stored = keychain_set_prompted(key_ref)

    if not stored:
        raise ValueError(f"security could not store the key for '{label}'")

    written = keychain_get(key_ref)
    if not written:
        raise ValueError(f"Stored the key for '{label}' but could not read it back")
    print(f"Stored {key_ref} ({mask(written)}) for '{label}' in region {region}.")

    return {"label": label, "key_ref": key_ref, "region": region}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--orgs-file",
        default=os.environ.get("RAPID7_ORGS_FILE", DEFAULT_ORGS_FILE),
        help=f"Path to the orgs file (default: $RAPID7_ORGS_FILE, else {DEFAULT_ORGS_FILE})",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("RAPID7_REGION", "us"),
        help="Default region offered at each prompt (default: $RAPID7_REGION, else us)",
    )
    parser.add_argument(
        "--key-from-stdin",
        action="store_true",
        help=(
            "Read each key from stdin instead of letting security prompt. "
            "Less safe, for piping from a password manager."
        ),
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Report how the existing orgs file resolves and change nothing",
    )
    args = parser.parse_args(argv)

    path = os.path.expanduser(args.orgs_file)
    default_region = validate_region(args.region, "us")

    try:
        orgs = load_orgs(path)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"Orgs file: {path}")
    print(f"Configured: {len(orgs)} organization(s)")
    for org in orgs:
        print(f"  {org.get('label')} -> {org.get('key_ref')} ({org.get('region', 'default')})")

    if args.verify_only:
        return 0 if verify(orgs) else 1

    added = 0
    while True:
        try:
            entry = prompt_one(default_region, args.key_from_stdin)
        except (EOFError, KeyboardInterrupt):
            print()
            break
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            continue
        if entry is None:
            break
        orgs = merge_org(orgs, entry)
        write_orgs(path, orgs)
        added += 1
        print(f"Wrote {path}")

    if added == 0:
        print("\nNothing added.")
        return 0 if verify(orgs) else 1

    usable = verify(orgs)
    print("\nNext steps:")
    print(f'  1. Set "RAPID7_ORGS_FILE": "{path}" in the Claude Desktop config env block.')
    print("  2. Quit Claude Desktop with Cmd Q, then reopen it.")
    print("  3. Ask Claude: Which organizations are configured for Rapid7 reporting?")
    return 0 if usable else 1


if __name__ == "__main__":
    sys.exit(main())
