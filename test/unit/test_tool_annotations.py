import asyncio
from unittest.mock import (
    MagicMock,
    create_autospec,
)

import pytest
from fastmcp import Client

from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
    MetaListSettings,
)


async def _list_tools_async():
    config = McpServerSettings(
        schemas=MetaListSettings(enable=True),
        enable_read_query=True,
        enable_write_query=True,
        enable_read_bucketfs=True,
        enable_write_bucketfs=True,
    )
    server = create_mcp_server(create_autospec(DbConnection), config, MagicMock())
    async with Client(server) as client:
        return await client.list_tools()


@pytest.fixture(scope="module")
def tools():
    return {tool.name: tool for tool in asyncio.run(_list_tools_async())}


@pytest.mark.parametrize(
    ["tool_name", "read_only", "destructive", "idempotent"],
    [
        ("list_exasol_schemas", True, None, None),
        ("execute_exasol_query", True, None, None),
        ("execute_exasol_write_query", None, True, None),
        ("list_exasol_keywords", True, None, True),
        ("describe_exasol_built_in_function", True, None, True),
        ("read_bucketfs_text_file", True, None, None),
        ("delete_bucketfs_file", None, True, None),
    ],
)
def test_tool_annotations(tools, tool_name, read_only, destructive, idempotent):
    """
    Guards against the tool hints being silently dropped, e.g. by a change in the
    ToolAnnotations field names.
    """
    annotations = tools[tool_name].annotations
    assert annotations.read_only_hint is read_only
    assert annotations.destructive_hint is destructive
    assert annotations.idempotent_hint is idempotent
