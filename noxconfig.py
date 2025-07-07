from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Config:
    root: Path = Path(__file__).parent
    doc: Path = Path(__file__).parent / "doc"
    source: Path = Path("exasol/ai/mcp/server")
    version_file: Path = (
        Path(__file__).parent
        / "exasol"
        / "ai"
        / "mcp"
        / "server"
        / "version.py"
    )
    path_filters: Iterable[str] = ()
    pyupgrade_args = ("--py310-plus",)
    plugins = []


PROJECT_CONFIG = Config()
