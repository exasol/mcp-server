"""
Integration tests verifying that a pooled connection survives a failed query, so that
the session state is not lost. See GitHub issue #309.
"""

import asyncio
import ssl
from textwrap import dedent

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from exasol.ai.mcp.server.connection.connection_factory import (
    ENV_DSN,
    ENV_PASSWORD,
    ENV_USER,
    get_connection_factory,
)
from exasol.ai.mcp.server.connection.db_connection import (
    DbConnection,
    fetchval,
)
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.setup.server_settings import McpServerSettings
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    CURRENT_PREPROCESSOR_FIELD,
)
from test.utils.result_utils import get_result_json


@pytest.fixture
def db_connection(run_on_itde, backend_aware_onprem_database_params) -> DbConnection:
    env = {
        ENV_DSN: backend_aware_onprem_database_params["dsn"],
        ENV_USER: backend_aware_onprem_database_params["user"],
        ENV_PASSWORD: backend_aware_onprem_database_params["password"],
    }
    connection_factory = get_connection_factory(
        env, websocket_sslopt={"cert_reqs": ssl.CERT_NONE}
    )
    return DbConnection(connection_factory=connection_factory)


@pytest.fixture
def bad_preprocessor(pyexasol_connection, db_schema_name) -> str:
    name = "failing_preprocessor"
    pyexasol_connection.execute(dedent(f"""\
            CREATE OR REPLACE LUA PREPROCESSOR SCRIPT "{db_schema_name}"."{name}" AS
                error("This preprocessor always fails")
            /
        """))
    yield name
    pyexasol_connection.execute(f'DROP SCRIPT IF EXISTS "{db_schema_name}"."{name}"')


def _current_session(db_connection: DbConnection) -> int:
    return int(
        db_connection.execute_query(
            "SELECT CURRENT_SESSION", snapshot=False, fetch=fetchval
        )
    )


def test_session_survives_query_error(db_connection) -> None:
    session_before = _current_session(db_connection)
    with pytest.raises(RuntimeError):
        db_connection.execute_query("SELECT * FROM NO_SUCH_TABLE", snapshot=False)
    assert _current_session(db_connection) == session_before


def test_preprocessor_survives_query_error(
    db_connection, setup_database, db_schema_name, db_preprocessor
) -> None:
    config = McpServerSettings(enable_read_query=True)
    exa_server = create_mcp_server(db_connection, config)

    async def run_tools():
        async with Client(exa_server) as client:
            await client.call_tool(
                "set_exasol_preprocessor",
                {"schema_name": db_schema_name, "script_name": db_preprocessor.name},
            )
            with pytest.raises(ToolError):
                await client.call_tool(
                    "execute_exasol_query", {"query": "SELECT * FROM NO_SUCH_TABLE"}
                )
            return await client.call_tool("list_exasol_preprocessors", {})

    data = get_result_json(asyncio.run(run_tools()))
    assert data[CURRENT_PREPROCESSOR_FIELD] is not None
    assert db_preprocessor.name.upper() in data[CURRENT_PREPROCESSOR_FIELD].upper()


def test_recovery_from_failing_preprocessor(
    db_connection, setup_database, db_schema_name, db_preprocessor, bad_preprocessor
) -> None:
    """
    Since the connection is kept after a query error, a preprocessor that fails every
    query also stays active. Check that it can still be replaced with a working one.
    """
    config = McpServerSettings(enable_read_query=True)
    exa_server = create_mcp_server(db_connection, config)

    async def run_tools():
        async with Client(exa_server) as client:
            await client.call_tool(
                "set_exasol_preprocessor",
                {"schema_name": db_schema_name, "script_name": bad_preprocessor},
            )
            with pytest.raises(ToolError):
                await client.call_tool("execute_exasol_query", {"query": "SELECT 1"})
            await client.call_tool(
                "set_exasol_preprocessor",
                {"schema_name": db_schema_name, "script_name": db_preprocessor.name},
            )
            return await client.call_tool("execute_exasol_query", {"query": "SELECT 1"})

    result = asyncio.run(run_tools())
    assert not result.is_error
