import asyncio
from collections.abc import Generator
from contextlib import contextmanager

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError
from mcp.shared.exceptions import MCPError
from pyexasol import ExaConnection

from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.setup.server_settings import McpServerSettings

# Client modes for testing the elicitation. The "legacy" client negotiates a
# handshake-era protocol, where the elicitation is a server-initiated request.
# The "auto" client negotiates the 2026-07-28 protocol, where the elicitation
# follows the guard pattern.
CLIENT_MODES = ["legacy", "auto"]


def expect_not_accepted(mode: str, action: str | None):
    """
    Returns a context manager that expects a write tool call to fail because the
    user didn't accept the elicitation. The error message is checked too, so that
    any other failure, such as an assertion in the elicitation handler, is not
    taken for the rejection.

    The action None means the client doesn't support elicitation. In the "auto"
    mode, the client then fails to fulfil the elicitation request returned by the
    tool, before the tool is called again.
    """
    if action is None:
        error_type = MCPError if mode == "auto" else ToolError
        return pytest.raises(error_type, match="Elicitation not supported")
    past_tense = {"decline": "declined", "cancel": "cancelled"}[action]
    return pytest.raises(ToolError, match=f"is {past_tense} by the user")


async def _run_tool_async(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    @contextmanager
    def connection_factory(
        no_auth: bool = False,
    ) -> Generator[ExaConnection, None, None]:
        yield connection

    db_connection = DbConnection(connection_factory, num_retries=1)

    exa_server = create_mcp_server(db_connection, config)
    async with Client(exa_server) as client:
        return await client.call_tool(tool_name, kwargs)


def run_tool(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    return asyncio.run(_run_tool_async(connection, config, tool_name, **kwargs))
