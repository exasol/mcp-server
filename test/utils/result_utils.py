import asyncio
import json
from dataclasses import dataclass
from typing import (
    Any,
    cast,
)

import exasol.bucketfs as bfs
import pytest
from fastmcp import Client
from mcp.types import Tool
from pyexasol import ExaConnection

from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)


@dataclass
class ToolHints:
    tool_name: str
    read_only: bool | None = None
    destructive: bool | None = None

    def __hash__(self):
        return hash(self.tool_name)


def result_sort_func(d: Any) -> str:
    if isinstance(d, dict):
        return ",".join(str(d[key]) for key in sorted(d.keys()))
    return str(d)


def get_result_content(result) -> str:
    return result.content[0].text


def get_result_json(result, content_extractor=get_result_content) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(content_extractor(result)))


def get_sort_result_json(
    result, content_extractor=get_result_content
) -> dict[str, Any]:
    result_json = get_result_json(result, content_extractor)
    return {
        key: sorted(val, key=result_sort_func) if isinstance(val, list) else val
        for key, val in result_json.items()
    }


def get_list_result_json(result, content_extractor=get_result_content) -> ExaDbResult:
    result_json = get_result_json(result, content_extractor)
    unsorted = ExaDbResult(**result_json)
    if isinstance(unsorted.result, list):
        return ExaDbResult(sorted(unsorted.result, key=result_sort_func))
    return unsorted


async def _list_tools_async(
    connection: ExaConnection,
    config: McpServerSettings,
    bucketfs_location: bfs.path.PathLike | None,
):
    exa_server = create_mcp_server(connection, config, bucketfs_location)
    async with Client(exa_server) as client:
        return await client.list_tools()


def list_tools(
    connection: ExaConnection,
    config: McpServerSettings,
    bucketfs_location: bfs.path.PathLike | None = None,
):
    return asyncio.run(_list_tools_async(connection, config, bucketfs_location))


def get_tool_hints(tool: Tool) -> ToolHints:
    if tool.annotations is None:
        return ToolHints(tool.name)
    return ToolHints(
        tool_name=tool.name,
        read_only=tool.annotations.readOnlyHint,
        destructive=tool.annotations.destructiveHint,
    )


def verify_result_table(
    result: ExaDbResult,
    key_column: str,
    other_columns: list[str],
    expected_keys: list[str],
) -> None:
    test_data = list(
        filter(lambda row: row[key_column] in expected_keys, result.result)
    )
    # Verify that all expected keys are present in the output.
    keys_found = {row[key_column] for row in test_data}
    if keys_found != set(expected_keys):
        pytest.fail(
            f"The expected rows {set(expected_keys).difference(keys_found)} "
            "not found in the output"
        )
    if other_columns:
        # Verify that there are values in all other expected columns.
        for col_name in other_columns:
            for row in test_data:
                if not row[col_name]:
                    pytest.fail(f"{col_name} is empty for {row[col_name]}")
