from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


def get_version() -> str:
    try:
        return version("resilix-python")
    except PackageNotFoundError:
        return Path(__file__).resolve().parents[2].joinpath("VERSION").read_text(encoding="utf-8").strip()
