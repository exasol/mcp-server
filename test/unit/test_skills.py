from unittest import mock

import pytest

from exasol.ai.mcp.server.main import (
    _SKILLS_DIR,
    register_skills,
)
from exasol.ai.mcp.server.tools.mcp_server import ExasolMCPServer

EXPECTED_SKILLS = [
    "exasol-sql-dialect",
    "exasol-udfs",
    "exasol-mcp-server",
    "exasol-system-tables",
]


def test_skills_directory_exists():
    assert _SKILLS_DIR.is_dir(), f"Skills directory not found: {_SKILLS_DIR}"


@pytest.mark.parametrize("skill_name", EXPECTED_SKILLS)
def test_skill_directory_exists(skill_name):
    skill_dir = _SKILLS_DIR / skill_name
    assert skill_dir.is_dir(), f"Missing skill directory: {skill_dir}"


@pytest.mark.parametrize("skill_name", EXPECTED_SKILLS)
def test_skill_file_exists(skill_name):
    skill_file = _SKILLS_DIR / skill_name / "SKILL.md"
    assert skill_file.is_file(), f"Missing SKILL.md in {skill_name}"


@pytest.mark.parametrize("skill_name", EXPECTED_SKILLS)
def test_skill_file_not_empty(skill_name):
    skill_file = _SKILLS_DIR / skill_name / "SKILL.md"
    assert skill_file.stat().st_size > 0, f"SKILL.md is empty in {skill_name}"


def test_register_skills_calls_add_provider():
    mcp_server = mock.create_autospec(ExasolMCPServer)
    register_skills(mcp_server)
    mcp_server.add_provider.assert_called_once()
