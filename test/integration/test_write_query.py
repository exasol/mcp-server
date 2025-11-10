import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from test.utils.db_objects import (
    ExaColumn,
    ExaSchema,
    ExaTable,
)
from test.utils.sql_utils import format_table_rows

import pytest
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult
from fastmcp.exceptions import ToolError
from pyexasol import ExaConnection

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.server_settings import McpServerSettings


async def _run_tool_async(
    connection: ExaConnection, config: McpServerSettings, action: str | None, query: str
):
    """
    Runs the `execute_write_query`, returning the specified action from what would be
    a user elicitation input. If the action is None, the elicitation handler is not
    registered. This can be used to test the case when a client application does not
    support elicitation.
    """

    @contextmanager
    def connection_factory() -> Generator[ExaConnection, None, None]:
        yield connection

    async def elicitation_handler(message: str, response_type: type, params, context):
        return ElicitResult(action=action)

    db_connection = DbConnection(connection_factory, num_retries=1)
    exa_server = create_mcp_server(db_connection, config)

    el_handler = elicitation_handler if action else None
    async with Client(exa_server, elicitation_handler=el_handler) as client:
        await client.call_tool("execute_write_query", {"query": query})


def _run_tool(
    connection: ExaConnection, config: McpServerSettings, action: str | None, query: str
) -> None:
    asyncio.run(_run_tool_async(connection, config, action, query))


def _validate_table_creation(
    connection: ExaConnection,
    config: McpServerSettings,
    action: str | None,
    schema: ExaSchema,
    table: ExaTable,
) -> None:
    """
    Validates that it is possible to create a table and insert some rows into
    it, using the `execute_write_query` tool.
    """
    try:
        create_query = f"CREATE OR REPLACE TABLE {table.decl(schema.name)}"
        _run_tool(connection, config, action, query=create_query)
        insert_query = (
            f'INSERT INTO "{schema.name}"."{table.name}" '
            f"VALUES {format_table_rows(table.rows)}"
        )
        _run_tool(connection, config, action, query=insert_query)

        select_query = f'SELECT * FROM "{schema.name}"."{table.name}"'
        rows = connection.execute(select_query).fetchall()
        assert len(rows) == len(table.rows)
    finally:
        drop_query = f'DROP TABLE IF EXISTS "{schema.name}"."{table.name}"'
        connection.execute(query=drop_query)


@pytest.fixture
def new_table() -> ExaTable:
    return ExaTable(
        name="job_centre",
        comment=None,
        columns=[
            ExaColumn(name="address", type="VARCHAR(500) UTF8", comment=None),
            ExaColumn(name="job_seekers", type="DECIMAL(18,0)", comment=None),
        ],
        constraints=[],
        keywords=[],
        rows=[
            ("45 Rue de la République, Batiment B, 3ème étage, 69002 LYON", 28765),
            ("18 Impasse des Vignes, 33610 CESTAS", 9765),
            ("Zone Artisanale Les Estroublans, 13014 MARSEILLE", 34987),
        ],
    )


def test_execute_write_query(
    pyexasol_connection, setup_database, db_schemas, new_table
) -> None:
    config = McpServerSettings(enable_write_query=True)
    for schema in db_schemas:
        _validate_table_creation(
            connection=pyexasol_connection,
            config=config,
            action="accept",
            schema=schema,
            table=new_table,
        )


@pytest.mark.parametrize("action", ["decline", "cancel", None])
def test_execute_write_query_not_accepted(
    pyexasol_connection, setup_database, db_schemas, new_table, action
) -> None:
    config = McpServerSettings(enable_write_query=True)
    for schema in db_schemas:
        with pytest.raises(ToolError):
            _validate_table_creation(
                connection=pyexasol_connection,
                config=config,
                action=action,
                schema=schema,
                table=new_table,
            )
