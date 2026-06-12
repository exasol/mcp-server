import asyncio
from pathlib import Path
from unittest import mock

from click.testing import CliRunner

from exasol.ai.mcp.server.main import (
    _install_skills_async,
    install_skills_cli,
)

EXPECTED_SKILLS = [
    "exasol-sql-dialect",
    "exasol-udfs",
    "exasol-mcp-server",
    "exasol-system-tables",
    "exasol-table-design",
    "exasol-import-export",
    "exasol-virtual-schemas",
]


def test_install_skills_local_creates_skill_dirs(tmp_path):
    paths = asyncio.run(_install_skills_async(tmp_path, None))
    assert len(paths) == len(EXPECTED_SKILLS)
    for skill_name in EXPECTED_SKILLS:
        assert (tmp_path / skill_name / "SKILL.md").is_file()


def test_install_skills_local_overwrites_existing(tmp_path):
    skill_dir = tmp_path / "exasol-sql-dialect"
    skill_dir.mkdir()
    stale_file = skill_dir / "SKILL.md"
    stale_file.write_text("stale content")

    asyncio.run(_install_skills_async(tmp_path, None))

    content = stale_file.read_text()
    assert content != "stale content"
    assert len(content) > 0


def test_install_skills_cli_local(tmp_path):
    runner = CliRunner()
    result = runner.invoke(install_skills_cli, ["--target-dir", str(tmp_path)])
    assert result.exit_code == 0
    for skill_name in EXPECTED_SKILLS:
        assert skill_name in result.output
    for skill_name in EXPECTED_SKILLS:
        assert (tmp_path / skill_name / "SKILL.md").is_file()


def test_install_skills_cli_remote(tmp_path):
    remote_url = "http://example.com/mcp"
    installed = [tmp_path / "exasol-sql-dialect"]

    with mock.patch(
        "exasol.ai.mcp.server.main._install_skills_async",
        return_value=installed,
    ) as mock_helper:
        runner = CliRunner()
        result = runner.invoke(
            install_skills_cli,
            ["--target-dir", str(tmp_path), "--server-url", remote_url],
        )

    assert result.exit_code == 0
    mock_helper.assert_called_once_with(Path(str(tmp_path)), remote_url)


def test_install_skills_cli_no_skills_installed(tmp_path):
    with mock.patch(
        "exasol.ai.mcp.server.main._install_skills_async",
        return_value=[],
    ):
        runner = CliRunner()
        result = runner.invoke(install_skills_cli, ["--target-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "No skills installed." in result.output
