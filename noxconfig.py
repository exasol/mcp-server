from __future__ import annotations

from pathlib import Path

from exasol.toolbox.config import BaseConfig

PROJECT_CONFIG = BaseConfig(
    root_path=Path(__file__).parent,
    project_name="ai/mcp/server",
    python_versions=("3.10", "3.11", "3.12", "3.13"),
)
