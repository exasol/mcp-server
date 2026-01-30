import asyncio
import json
from collections.abc import Generator
from contextlib import contextmanager
from test.utils.result_utils import (
    get_list_result_json,
    verify_result_table,
)

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
    verify_result_table(result_json, key_column, other_columns, expected_keys)


def test_list_sql_types(pyexasol_connection):
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="dialect://sql-types",
        key_column="TYPE_NAME",
        other_columns=["CREATE_PARAMS", "PRECISION"],
        expected_keys=["CHAR", "VARCHAR", "DECIMAL"],
    )


def test_list_system_tables(pyexasol_connection):
    result = _read_resource(pyexasol_connection, "system://system-table/list")
    result_json = json.loads(result[0].text)
    assert all(
        table_name in result_json for table_name in ["EXA_ALL_COLUMNS", "EXA_CLUSTERS"]
    )


def test_describe_system_table(pyexasol_connection):
    conf = McpServerSettings().tables
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="system://system-table/details/exa_all_columns",
        key_column=conf.name_field,
        other_columns=[conf.schema_field, conf.comment_field],
        expected_keys=["EXA_ALL_COLUMNS"],
    )


def test_list_statistics_tables(pyexasol_connection):
    result = _read_resource(pyexasol_connection, "system://statistics-table/list")
    result_json = json.loads(result[0].text)
    assert all(
        table_name in result_json
        for table_name in ["EXA_SQL_DAILY", "EXA_DBA_AUDIT_SESSIONS"]
    )


def test_describe_statistics_tables(pyexasol_connection):
    conf = McpServerSettings().tables
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="system://statistics-table/details/exa_sql_daily",
        key_column=conf.name_field,
        other_columns=[conf.schema_field, conf.comment_field],
        expected_keys=["EXA_SQL_DAILY"],
    )


def test_list_reserved_keywords(pyexasol_connection):
    result = _read_resource(pyexasol_connection, "dialect://keyword/reserved/a")
    result_json = json.loads(result[0].text)
    assert all(keyword.startswith("A") for keyword in result_json)
    assert all(keyword in result_json for keyword in ["ALL", "ANY", "ARE"])
    assert all(keyword not in result_json for keyword in ["ABS", "ADD_YEARS", "ALWAYS"])


def test_list_non_reserved_keywords(pyexasol_connection):
    result = _read_resource(pyexasol_connection, "dialect://keyword/non-reserved/a")
    result_json = json.loads(result[0].text)
    assert all(keyword.startswith("A") for keyword in result_json)
    assert all(keyword in result_json for keyword in ["ABS", "ADD_YEARS", "ALWAYS"])
    assert all(keyword not in result_json for keyword in ["ALL", "ANY", "ARE"])


def test_builtin_function_categories(pyexasol_connection):
    result = _read_resource(
        pyexasol_connection, "dialect://built-in-function/categories"
    )
    result_json = json.loads(result[0].text)
    assert all(
        expected_name in result_json
        for expected_name in ["numeric", "string", "analytic"]
    )


def test_list_builtin_functions(pyexasol_connection):
    result = _read_resource(
        pyexasol_connection, "dialect://built-in-function/list/numeric"
    )
    result_json = json.loads(result[0].text)
    assert all(expected_name in result_json for expected_name in ["CEILING", "DEGREES"])


def test_describe_builtin_function(pyexasol_connection):
    _verify_resource_table(
        pyexasol_connection,
        resource_uri="dialect://built-in-function/details/to_date",
        key_column="name",
        other_columns=["description", "types", "usage-notes", "example"],
        expected_keys=["TO_DATE"],
    )
