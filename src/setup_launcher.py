"""Open the rapid7-orgs menu in a terminal window on the customer's own computer.

The server writes a small file that runs the menu, then opens it: in Terminal on macOS,
in a console window on Windows. Linux has no standard terminal command, so there the
folder opens and the customer double-clicks the file. The file holds paths and the
default region only, never a key.
"""

import os
import platform
import shlex
import subprocess  # nosec B404
import sys
from pathlib import Path

from .config import REGION_ENDPOINTS

FILE_NAME = "Set up Rapid7 organizations"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _suffix() -> str:
    return {"Darwin": ".command", "Windows": ".cmd"}.get(platform.system(), ".sh")


def _menu_command() -> list:
    windows = platform.system() == "Windows"
    launcher = Path(sys.executable).parent / ("rapid7-orgs.exe" if windows else "rapid7-orgs")
    command = [str(launcher)] if launcher.exists() else [sys.executable, "-m", "src.orgs_cli"]
    command.append("menu")
    orgs_file = os.environ.get("RAPID7_ORGS_FILE", "").strip()
    if orgs_file and not orgs_file.startswith("${"):
        command += ["--orgs-file", orgs_file]
    return command


def write_setup_file(folder: Path) -> Path:
    """Owner-only, because double-clicking it runs a command as the customer."""
    folder.mkdir(parents=True, exist_ok=True)
    command = _menu_command()
    region = os.environ.get("RAPID7_REGION", "us")
    region = region if region in REGION_ENDPOINTS else "us"
    path = folder / f"{FILE_NAME}{_suffix()}"
    if platform.system() == "Windows":
        line = " ".join(f'"{part}"' if " " in part or "\\" in part else part for part in command)
        text = f'@echo off\r\nset RAPID7_REGION={region}\r\ncd /d "{_PROJECT_ROOT}"\r\n{line}\r\n'
    else:
        line = " ".join(shlex.quote(part) for part in command)
        root = shlex.quote(str(_PROJECT_ROOT))
        text = f"#!/bin/sh\ncd {root} || exit 1\nRAPID7_REGION={region} exec {line}\n"
    path.write_text(text, encoding="utf-8")
    path.chmod(0o700)
    return path


def _start(args: list) -> bool:
    try:
        subprocess.Popen(  # nosec B603 B607
            args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True
        )
        return True
    except OSError:
        return False


def open_window(path: Path) -> bool:
    system = platform.system()
    if system == "Darwin":
        return _start(["open", "-a", "Terminal", str(path)])
    if system == "Windows":
        return _start(["cmd", "/c", "start", "", str(path)])
    return False


def open_folder(folder: Path) -> bool:
    system = platform.system()
    opener = {"Darwin": "open", "Windows": "explorer"}.get(system, "xdg-open")
    return _start([opener, str(folder)])
