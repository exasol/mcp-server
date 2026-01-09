import asyncio
import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import (
    Any,
    cast,
)

from fastmcp import Client
from pyexasol import ExaConnection

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)


async def _run_tool_async(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    @contextmanager
    def connection_factory() -> Generator[ExaConnection, None, None]:
        yield connection

    db_connection = DbConnection(connection_factory, num_retries=1)

    exa_server = create_mcp_server(db_connection, config)
    async with Client(exa_server) as client:
        return await client.call_tool(tool_name, kwargs)


async def _list_tools_async(connection: ExaConnection, config: McpServerSettings):
    exa_server = create_mcp_server(connection, config)
    async with Client(exa_server) as client:
        return await client.list_tools()


def run_tool(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    return asyncio.run(_run_tool_async(connection, config, tool_name, **kwargs))


def list_tools(connection: ExaConnection, config: McpServerSettings):
    return asyncio.run(_list_tools_async(connection, config))


def result_sort_func(d: Any) -> str:
    if isinstance(d, dict):
        return ",".join(str(d[key]) for key in sorted(d.keys()))
    return str(d)


def get_result_json(result) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(result.content[0].text))


def get_sort_result_json(result) -> dict[str, Any]:
    result_json = get_result_json(result)
    return {
        key: sorted(val, key=result_sort_func) if isinstance(val, list) else val
        for key, val in result_json.items()
    }


def get_list_result_json(result) -> ExaDbResult:
    result_json = get_result_json(result)
    unsorted = ExaDbResult(**result_json)
    if isinstance(unsorted.result, list):
        return ExaDbResult(sorted(unsorted.result, key=result_sort_func))
    return unsorted
