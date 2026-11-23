"""Read and write org API keys in the operating system's key store.

Each key is stored under its key_ref as the service and "rapid7" as the account, which is
how the earlier macOS setup script stored them too.

On macOS, every access goes through Apple's `security` tool, as the server's own lookup
does. macOS ties each Keychain item to the program that created it, so items written by
Python through keyring would make macOS ask the user before the server could read them.
Windows Credential Manager and the Linux Secret Service are used through keyring.
"""

import platform
import re
import subprocess  # nosec B404
from typing import Optional

import keyring

ACCOUNT = "rapid7"

_STORE_NAMES = {
    "keyring.backends.macOS": "macOS Keychain",
    "keyring.backends.Windows": "Windows Credential Manager",
    "keyring.backends.SecretService": "Secret Service",
    "keyring.backends.libsecret": "Secret Service",
    "keyring.backends.kwallet": "KWallet",
}


class KeyStoreUnavailable(RuntimeError):
    """This machine has no usable key store, so a key cannot be stored safely."""


def _backends():
    try:
        backend = keyring.get_keyring()
        return list(getattr(backend, "backends", [backend]))
    except Exception:
        # keyring raises while choosing or listing backends on a machine it cannot
        # serve, so that machine has no usable store.
        return []


def _safe(backend) -> bool:
    """A store that encrypts or protects keys. Failing, null, and plain-text stores do not."""
    module = type(backend).__module__.lower()
    return not any(word in module for word in ("fail", "null", "keyrings.alt", "plaintext"))


_KEY_REF = re.compile(r"^[A-Za-z0-9_#-]{1,128}$")
_SAFE_KEY = re.compile(r"^[A-Za-z0-9._~+/=-]{1,512}$")


def _macos() -> bool:
    return platform.system() == "Darwin"


def _security(args, stdin=None) -> subprocess.CompletedProcess:
    return subprocess.run(  # nosec B603 B607
        ["security", *args], input=stdin, capture_output=True, text=True, timeout=10
    )


def _check(key_ref: str, key: Optional[str] = None) -> None:
    if not _KEY_REF.match(key_ref):
        raise ValueError("A key name may contain only letters, digits, underscores and dashes.")
    if key is not None and not _SAFE_KEY.match(key):
        raise ValueError("The key contains characters an API key never has. Paste the key on its own.")


def is_macos_keychain() -> bool:
    """Report whether the store is the macOS Keychain, which the server reads with `security`."""
    return any(type(backend).__module__ == "keyring.backends.macOS" for backend in _backends())


def available() -> bool:
    """Report whether a protected key store is in use."""
    return _macos() or any(_safe(backend) for backend in _backends())


def name() -> str:
    """The key store's name, as the customer knows it."""
    if _macos():
        return "macOS Keychain"
    for backend in _backends():
        if _safe(backend):
            return _STORE_NAMES.get(type(backend).__module__, type(backend).__name__)
    return "no key store"


def read(key_ref: str) -> Optional[str]:
    """Return the key stored under key_ref, or None when there is none or no usable store."""
    if _macos():
        _check(key_ref)
        result = _security(["find-generic-password", "-s", key_ref, "-w"])
        return (result.stdout.strip() or None) if result.returncode == 0 else None
    if not available():
        return None
    try:
        return keyring.get_password(key_ref, ACCOUNT) or None
    except Exception:
        # A locked or broken store means no key here. The caller then reports the org
        # as missing, which names it, rather than failing with the store's own error.
        return None


def store(key_ref: str, key: str) -> None:
    """Store key under key_ref, refusing any store that would keep it in plain text."""
    if _macos():
        _check(key_ref, key)
        # security -i reads the command on stdin, so the key never appears in a process's arguments.
        command = f"add-generic-password -U -s {key_ref} -a {ACCOUNT} -w {key}\n"
        result = _security(["-i"], stdin=command)
        if result.returncode != 0 or read(key_ref) != key:
            raise KeyStoreUnavailable("The macOS Keychain did not store the key. Unlock the Keychain and try again.")
        return
    if not available():
        raise KeyStoreUnavailable(
            "This machine has no protected key store, so the key was not stored. Set an environment "
            f"variable named {key_ref} to the key instead, for example from your secrets manager."
        )
    keyring.set_password(key_ref, ACCOUNT, key)


def remove(key_ref: str) -> bool:
    """Delete the key stored under key_ref. Return False when there was none."""
    if read(key_ref) is None:
        return False
    if _macos():
        return _security(["delete-generic-password", "-s", key_ref, "-a", ACCOUNT]).returncode == 0
    keyring.delete_password(key_ref, ACCOUNT)
    return True
