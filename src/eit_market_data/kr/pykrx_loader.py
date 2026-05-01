"""Helpers for importing pykrx without noisy stdout side effects."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from importlib import import_module
from io import StringIO
from types import ModuleType


def _quiet_import(module_name: str) -> ModuleType:
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        return import_module(module_name)


def load_pykrx_stock() -> ModuleType:
    """Import ``pykrx.stock`` while suppressing import-time login chatter."""
    return _quiet_import("pykrx.stock")


def load_pykrx_webio() -> ModuleType:
    """Import ``pykrx.website.comm.webio`` while suppressing import-time chatter."""
    return _quiet_import("pykrx.website.comm.webio")
