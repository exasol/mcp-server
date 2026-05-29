import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from unittest import mock

import pytest
from fastmcp import Client
from pyexasol import ExaConnection

from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.setup.server_settings import McpServerSettings

EXPECTED_SKILL_URIS = {
    "skill://exasol-sql-dialect/SKILL.md",
    "skill://exasol-udfs/SKILL.md",
    "skill://exasol-mcp-server/SKILL.md",
    "skill://exasol-system-tables/SKILL.md",
}


@pytest.fixture
def mcp_server():
    @contextmanager
    def connection_factory(
        no_auth: bool = False,
    ) -> Generator[ExaConnection, None, None]:
        yield mock.create_autospec(ExaConnection)

    db_connection = DbConnection(connection_factory, num_retries=1)
    return create_mcp_server(db_connection, McpServerSettings())


async def _list_resource_uris(server) -> set[str]:
    async with Client(server) as client:
        resources = await client.list_resources()
        return {str(r.uri) for r in resources}


async def _read_resource(server, uri: str) -> str:
    async with Client(server) as client:
        contents = await client.read_resource(uri)
        return contents[0].text


def test_skills_are_listed(mcp_server):
    uris = asyncio.run(_list_resource_uris(mcp_server))
    assert EXPECTED_SKILL_URIS.issubset(uris)


@pytest.mark.parametrize("uri", sorted(EXPECTED_SKILL_URIS))
def test_skill_content_is_readable(mcp_server, uri):
    content = asyncio.run(_read_resource(mcp_server, uri))
    assert len(content) > 0


def test_sql_dialect_skill_mentions_fetch_first(mcp_server):
    content = asyncio.run(
        _read_resource(mcp_server, "skill://exasol-sql-dialect/SKILL.md")
    )
    assert "FETCH FIRST" in content


def test_udfs_skill_mentions_exaiterator(mcp_server):
    content = asyncio.run(_read_resource(mcp_server, "skill://exasol-udfs/SKILL.md"))
    assert "ExaIterator" in content


def test_system_tables_skill_mentions_exa_all(mcp_server):
    content = asyncio.run(
        _read_resource(mcp_server, "skill://exasol-system-tables/SKILL.md")
    )
    assert "EXA_ALL_" in content


def test_mcp_server_skill_mentions_workflows(mcp_server):
    content = asyncio.run(
        _read_resource(mcp_server, "skill://exasol-mcp-server/SKILL.md")
    )
    assert "Workflows" in content
