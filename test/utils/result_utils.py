import asyncio
import json
from dataclasses import dataclass
from typing import (
    Any,
)

import exasol.bucketfs as bfs
import pytest
from fastmcp import Client
from mcp.types import Tool
from pyexasol import ExaConnection

from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
)


@dataclass
class ToolHints:
    tool_name: str
    read_only: bool | None = None
    destructive: bool | None = None
    idempotent: bool | None = None

    def __hash__(self):
        return hash(self.tool_name)


def result_sort_func(d: Any) -> str:
    if isinstance(d, dict):
        return ",".join(str(d[key]) for key in sorted(d.keys()))
    return str(d)


def to_dicts(result_json: Any) -> list[dict[str, Any]]:
    """
    Normalizes a query-result payload to a list of dicts, regardless of the
    configured `query_result_format`: passes a list of dicts through unchanged, and
    converts the tabular `{"columns": [...], "rows": [[...], ...]}` shape.
    """
    if (
        isinstance(result_json, dict)
        and "columns" in result_json
        and "rows" in result_json
    ):
        columns = result_json["columns"]
        return [dict(zip(columns, row)) for row in result_json["rows"]]
    return result_json


def get_result_content(result) -> str:
    return result.content[0].text


def get_result_json(result, content_extractor=get_result_content):
    return json.loads(content_extractor(result))


def sort_dict_lists(d: dict[str, Any]) -> dict[str, Any]:
    return {
        key: sorted(val, key=result_sort_func) if isinstance(val, list) else val
        for key, val in d.items()
    }


def get_sort_result_json_list(
    result, content_extractor=get_result_content
) -> list[dict[str, Any]]:
    """
    For tools that return a list of items (e.g. the batched `describe_*` tools) -
    sorts the list-valued fields of every item, preserving the order of the items
    themselves.
    """
    return [
        sort_dict_lists(item) for item in get_result_json(result, content_extractor)
    ]


def get_list_result_json(result, content_extractor=get_result_content):
    result_json = get_result_json(result, content_extractor)
    if isinstance(result_json, list):
        return sorted(result_json, key=result_sort_func)
    return result_json


def get_query_result_json(result, content_extractor=get_result_content):
    """
    Like `get_list_result_json`, but first normalizes the tabular
    `{"columns", "rows"}` shape (used by `execute_exasol_query`,
    `profile_exasol_query` and the `summarize_exasol_table` sample) to a list of
    dicts, so tests can compare the result regardless of `query_result_format`.
    """
    result_json = to_dicts(get_result_json(result, content_extractor))
    if isinstance(result_json, list):
        return sorted(result_json, key=result_sort_func)
    return result_json


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
        idempotent=tool.annotations.idempotentHint,
    )


def verify_result_table(
    result: list[dict[str, Any]],
    key_column: str,
    other_columns: list[str],
    expected_keys: list[str],
) -> None:
    test_data = list(filter(lambda row: row[key_column] in expected_keys, result))
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
