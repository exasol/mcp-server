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


def _verify_resource_table(
    pyexasol_connection: ExaConnection,
    resource_uri: str,
    key_column: str,
    other_columns: list[str],
    expected_keys: list[str],
) -> None:
    result = _read_resource(pyexasol_connection, resource_uri)
    result_json = get_list_result_json(result, _get_resource_content)
    test_data = list(
        filter(lambda row: row[key_column] in expected_keys, result_json.result)
    )
    # Verify that all expected keys are present in the output.
    assert {row[key_column] for row in test_data} == set(expected_keys)
    if other_columns:
        # Verify that there are values in all other expected columns.
        for col_name in other_columns:
            for row in test_data:
                assert row[col_name], f"{col_name} is empty for {row[col_name]}"


def test_list_sql_types(pyexasol_connection):
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="dialect://sql-types",
        key_column="TYPE_NAME",
        other_columns=["CREATE_PARAMS", "PRECISION"],
        expected_keys=["CHAR", "VARCHAR", "DECIMAL"],
    )


def test_list_system_tables(pyexasol_connection):
    conf = McpServerSettings().tables
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="system://system-tables",
        key_column=conf.name_field,
        other_columns=[conf.schema_field, conf.comment_field],
        expected_keys=["EXA_ALL_COLUMNS", "EXA_CLUSTERS"],
    )


def test_list_statistics_tables(pyexasol_connection):
    conf = McpServerSettings().tables
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="system://statistics-tables",
        key_column=conf.name_field,
        other_columns=[conf.schema_field, conf.comment_field],
        expected_keys=["EXA_SQL_DAILY", "EXA_DBA_AUDIT_SESSIONS"],
    )


def test_list_reserved_keywords(pyexasol_connection):
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="dialect://reserved-keywords",
        key_column="KEYWORD",
        other_columns=[],
        expected_keys=["ALL", "BEFORE", "CONDITION", "FINAL"],
    )
