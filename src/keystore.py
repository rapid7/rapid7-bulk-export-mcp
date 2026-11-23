"""Read and write org API keys in the operating system's key store.

macOS Keychain, Windows Credential Manager, and the Linux Secret Service all work through
keyring. Each key is stored under its key_ref as the service and "rapid7" as the account,
which is how the earlier macOS setup script stored them too.
"""

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


def is_macos_keychain() -> bool:
    """Report whether the store is the macOS Keychain, which the server reads with `security`."""
    return any(type(backend).__module__ == "keyring.backends.macOS" for backend in _backends())


def available() -> bool:
    """Report whether a protected key store is in use."""
    return any(_safe(backend) for backend in _backends())


def name() -> str:
    """The key store's name, as the customer knows it."""
    for backend in _backends():
        if _safe(backend):
            return _STORE_NAMES.get(type(backend).__module__, type(backend).__name__)
    return "no key store"


def read(key_ref: str) -> Optional[str]:
    """Return the key stored under key_ref, or None when there is none or no usable store."""
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
    keyring.delete_password(key_ref, ACCOUNT)
    return True
