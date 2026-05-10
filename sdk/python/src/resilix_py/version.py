from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


def get_version() -> str:
    try:
        return version("resilix-python")
    except PackageNotFoundError:
        version_file = Path(__file__).resolve().parents[2].joinpath("VERSION")
        return version_file.read_text(encoding="utf-8").strip()
