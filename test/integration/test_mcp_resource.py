import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from test.utils.result_utils import get_list_result_json

from fastmcp import Client
from pyexasol import ExaConnection

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.server_settings import McpServerSettings


async def _read_resource_async(connection: ExaConnection, resource_uri: str):
    @contextmanager
    def connection_factory() -> Generator[ExaConnection, None, None]:
        yield connection

    db_connection = DbConnection(connection_factory, num_retries=1)

    exa_server = create_mcp_server(db_connection, McpServerSettings())
    async with Client(exa_server) as client:
        return await client.read_resource(resource_uri)


def _read_resource(connection: ExaConnection, resource_uri: str):
    return asyncio.run(_read_resource_async(connection, resource_uri))


def _get_resource_content(result) -> str:
    return result[0].text


def test_list_sql_types(pyexasol_connection):
    result = _read_resource(pyexasol_connection, "dialect://sql-types")
    result_json = get_list_result_json(result, _get_resource_content)
    type_names = [row["TYPE_NAME"] for row in result_json.result]
    assert all(type_name in type_names for type_name in ["CHAR", "VARCHAR", "DECIMAL"])
