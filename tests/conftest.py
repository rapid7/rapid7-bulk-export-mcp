import logging
import socket

import pytest


@pytest.fixture(autouse=True)
def _restore_refresh_logger():
    """Undo global logging mutation so tests cannot leak state into each other.

    ``src.cli._configure_logging`` clears handlers and sets ``propagate = False``
    on the module-level ``rapid7.refresh`` logger. That is correct for the CLI —
    exactly one handler, no duplicate output — but it mutates process-global state.
    Once any CLI test has run, records from child loggers such as
    ``rapid7.refresh.export`` no longer reach the root logger, so pytest's
    ``caplog`` captures NOTHING in every later test. The failure is silent and
    order-dependent: the affected tests pass in isolation and fail in the full
    suite, which is the worst shape for a test defect to take.

    Snapshot the logger's handlers, level and propagate flag, and restore them
    after each test.
    """
    logger = logging.getLogger("rapid7.refresh")
    handlers = logger.handlers[:]
    level = logger.level
    propagate = logger.propagate
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)
    logger.propagate = propagate


@pytest.fixture(autouse=True)
def _no_outside_network(monkeypatch):
    """No test may open a connection off this machine, however it reaches the network.

    A test that needs a KEV catalogue passes a stub session to kev_catalogue instead.
    """
    real_connect = socket.socket.connect

    def _local_only(sock, address):
        host = address[0] if isinstance(address, tuple) else address
        if isinstance(host, str) and host not in ("127.0.0.1", "::1", "localhost") and sock.family != socket.AF_UNIX:
            raise ConnectionRefusedError(f"tests may not reach {host}")
        return real_connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", _local_only)
