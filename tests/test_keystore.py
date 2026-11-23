"""Org keys live in the operating system's key store on macOS, Windows, and Linux, never in a plain file."""

from unittest.mock import patch

import keyring
import pytest
from keyring.backend import KeyringBackend
from keyring.backends import fail

from src import config, keystore


class MemoryStore(KeyringBackend):
    """Stands in for an OS key store, so no test reaches the real one."""

    priority = 1

    def __init__(self):
        super().__init__()
        self.items = {}

    def get_password(self, service, username):
        return self.items.get((service, username))

    def set_password(self, service, username, password):
        self.items[(service, username)] = password

    def delete_password(self, service, username):
        self.items.pop((service, username))


class WindowsCredentialManager(MemoryStore):
    """A store whose module name matches keyring's real Windows backend."""


WindowsCredentialManager.__module__ = "keyring.backends.Windows"


class PlaintextStore(MemoryStore):
    """Matches the plain-text backends that keyrings.alt provides."""


PlaintextStore.__module__ = "keyrings.alt.file"


@pytest.fixture(autouse=True)
def not_macos(monkeypatch):
    """keyring paths run as Linux, so no test reaches the real macOS Keychain."""
    monkeypatch.setattr(keystore.platform, "system", lambda: "Linux")


@pytest.fixture
def store():
    previous = keyring.get_keyring()
    backend = MemoryStore()
    keyring.set_keyring(backend)
    yield backend
    keyring.set_keyring(previous)


@pytest.fixture
def windows():
    previous = keyring.get_keyring()
    backend = WindowsCredentialManager()
    keyring.set_keyring(backend)
    yield backend
    keyring.set_keyring(previous)


def test_a_key_is_stored_and_read_under_its_key_ref_and_the_rapid7_account(store):
    keystore.store("R7_KEY_FOGCUTTER", "key-value-0001")

    assert store.items == {("R7_KEY_FOGCUTTER", "rapid7"): "key-value-0001"}
    assert keystore.read("R7_KEY_FOGCUTTER") == "key-value-0001"


def test_removing_a_key_deletes_it_and_a_missing_key_is_not_an_error(store):
    keystore.store("R7_KEY_A", "key-a")

    assert keystore.remove("R7_KEY_A") is True
    assert keystore.read("R7_KEY_A") is None
    assert keystore.remove("R7_KEY_A") is False


@pytest.mark.parametrize("backend", [fail.Keyring(), PlaintextStore()])
def test_no_key_is_ever_written_to_a_failing_or_plain_text_store(backend):
    previous = keyring.get_keyring()
    keyring.set_keyring(backend)
    try:
        assert keystore.available() is False
        with pytest.raises(keystore.KeyStoreUnavailable, match="environment variable"):
            keystore.store("R7_KEY_A", "key-a")
        assert keystore.read("R7_KEY_A") is None
    finally:
        keyring.set_keyring(previous)


def test_the_store_is_named_for_the_customer(windows):
    assert keystore.name() == "Windows Credential Manager"


def test_the_server_reads_org_keys_from_the_os_store_on_windows(windows, monkeypatch):
    keystore.store("R7_KEY_EU", "key-eu")
    monkeypatch.delenv("R7_KEY_EU", raising=False)

    with patch("src.config.platform.system", return_value="Windows"):
        assert config._get_key_from_keychain("R7_KEY_EU") == "key-eu"


def test_on_macos_the_server_keeps_its_existing_keychain_lookup(store):
    keystore.store("R7_KEY_EU", "key-from-keyring")

    with (
        patch("src.config.platform.system", return_value="Darwin"),
        patch("src.config.subprocess.run") as run,
    ):
        run.return_value.stdout = "key-from-security\n"
        assert config._get_key_from_keychain("R7_KEY_EU") == "key-from-security"
    run.assert_called_once()


def test_an_error_from_the_os_store_reads_as_no_key(windows):
    with (
        patch("src.config.platform.system", return_value="Linux"),
        patch.object(WindowsCredentialManager, "get_password", side_effect=RuntimeError("locked")),
    ):
        assert config._get_key_from_keychain("R7_KEY_EU") is None


class FakeSecurity:
    """Stands in for Apple's security tool, recording every call."""

    def __init__(self):
        self.items, self.calls = {}, []

    def __call__(self, args, input=None, capture_output=True, text=True, timeout=None):
        self.calls.append((list(args), input))
        result = type("Result", (), {"returncode": 0, "stdout": ""})()
        verb = args[1] if args[0] == "security" else None
        if verb == "-i":
            words = input.split()
            self.items[words[words.index("-s") + 1]] = words[words.index("-w") + 1]
        elif verb == "find-generic-password":
            ref = args[args.index("-s") + 1]
            result.returncode, result.stdout = (0, self.items[ref] + "\n") if ref in self.items else (44, "")
        elif verb == "delete-generic-password":
            self.items.pop(args[args.index("-s") + 1], None)
        return result


@pytest.fixture
def mac(monkeypatch):
    fake = FakeSecurity()
    monkeypatch.setattr(keystore.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(keystore.subprocess, "run", fake)
    return fake


def test_on_macos_keys_go_through_security_and_never_appear_in_its_arguments(mac):
    keystore.store("R7_KEY_FOGCUTTER", "aaaa-1111-key")

    assert mac.items == {"R7_KEY_FOGCUTTER": "aaaa-1111-key"}
    assert all("aaaa-1111-key" not in " ".join(args) for args, _ in mac.calls), "the key is sent on stdin only"
    assert any(args == ["security", "-i"] and "aaaa-1111-key" in stdin for args, stdin in mac.calls if stdin)
    assert keystore.read("R7_KEY_FOGCUTTER") == "aaaa-1111-key"
    assert keystore.name() == "macOS Keychain"


def test_on_macos_a_key_stored_by_the_old_script_is_read_unchanged(mac):
    mac.items["R7_KEY_SYNTH_VM_EU_CENTRAL_1"] = "old-script-key"

    assert keystore.read("R7_KEY_SYNTH_VM_EU_CENTRAL_1") == "old-script-key"


def test_on_macos_remove_deletes_the_item(mac):
    keystore.store("R7_KEY_A", "key-a")

    assert keystore.remove("R7_KEY_A") is True
    assert keystore.read("R7_KEY_A") is None


@pytest.mark.parametrize(
    ("ref", "key"), [("R7 KEY", "k"), ("R7_KEY_A", "has space"), ("R7_KEY_A", "x\nadd-generic-password")]
)
def test_on_macos_a_name_or_key_that_could_change_the_command_is_refused(mac, ref, key):
    with pytest.raises(ValueError):
        keystore.store(ref, key)

    assert mac.calls == []
