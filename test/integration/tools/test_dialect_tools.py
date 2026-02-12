from test.utils.result_utils import (
    get_list_result_json,
    get_result_json,
    verify_result_table,
)
from test.utils.tool_utils import run_tool

from pyexasol import ExaConnection

from exasol.ai.mcp.server.setup.server_settings import McpServerSettings


def _run_tool(connection: ExaConnection, tool_name: str, **kwargs):
    """Runs a tool that is always enabled and doesn't require configuration"""
    config = McpServerSettings()
    return run_tool(connection, config, tool_name, **kwargs)


def _verify_result_table(
    pyexasol_connection: ExaConnection,
    tool_name: str,
    key_column: str,
    other_columns: list[str],
    expected_keys: list[str],
    **kwargs,
) -> None:
    result = _run_tool(pyexasol_connection, tool_name, **kwargs)
    result_json = get_list_result_json(result)
    verify_result_table(result_json, key_column, other_columns, expected_keys)


def test_list_sql_types(pyexasol_connection):
    _verify_result_table(
        pyexasol_connection,
        "list_sql_types",
        key_column="TYPE_NAME",
        other_columns=["CREATE_PARAMS", "PRECISION"],
        expected_keys=["CHAR", "VARCHAR", "DECIMAL"],
    )


def test_list_system_tables(pyexasol_connection):
    result = _run_tool(pyexasol_connection, "list_system_tables")
    result_json = get_result_json(result)
    assert all(
        table_name in result_json for table_name in ["EXA_ALL_COLUMNS", "EXA_CLUSTERS"]
    )


def test_describe_system_table(pyexasol_connection):
    conf = McpServerSettings().tables
    _verify_result_table(
        pyexasol_connection,
        "describe_system_table",
        key_column=conf.name_field,
        other_columns=[conf.schema_field, conf.comment_field],
        expected_keys=["EXA_ALL_COLUMNS"],
        table_name="exa_all_columns",
    )


def test_list_statistics_tables(pyexasol_connection):
    result = _run_tool(pyexasol_connection, "list_statistics_tables")
    result_json = get_result_json(result)
    assert all(
        table_name in result_json
        for table_name in ["EXA_SQL_DAILY", "EXA_DBA_AUDIT_SESSIONS"]
    )


def test_describe_statistics_tables(pyexasol_connection):
    conf = McpServerSettings().tables
    _verify_result_table(
        pyexasol_connection,
        "describe_statistics_table",
        key_column=conf.name_field,
        other_columns=[conf.schema_field, conf.comment_field],
        expected_keys=["EXA_SQL_DAILY"],
        table_name="exa_sql_daily",
    )


def test_list_reserved_keywords(pyexasol_connection):
    result = _run_tool(pyexasol_connection, "list_keywords", reserved=True, letter="a")
    result_json = get_result_json(result)
    assert all(keyword.startswith("A") for keyword in result_json)
    assert all(keyword in result_json for keyword in ["ALL", "ANY", "ARE"])
    assert all(keyword not in result_json for keyword in ["ABS", "ADD_YEARS", "ALWAYS"])


def test_list_non_reserved_keywords(pyexasol_connection):
    result = _run_tool(pyexasol_connection, "list_keywords", reserved=False, letter="a")
    result_json = get_result_json(result)
    assert all(keyword.startswith("A") for keyword in result_json)
    assert all(keyword in result_json for keyword in ["ABS", "ADD_YEARS", "ALWAYS"])
    assert all(keyword not in result_json for keyword in ["ALL", "ANY", "ARE"])


def test_builtin_function_categories(pyexasol_connection):
    result = _run_tool(pyexasol_connection, "builtin_function_categories")
    result_json = get_result_json(result)
    assert all(
        expected_name in result_json
        for expected_name in ["numeric", "string", "analytic"]
    )


def test_list_builtin_functions(pyexasol_connection):
    result = _run_tool(
        pyexasol_connection, "list_builtin_functions", category="numeric"
    )
    result_json = get_result_json(result)
    assert all(expected_name in result_json for expected_name in ["CEILING", "DEGREES"])


def test_describe_builtin_function(pyexasol_connection):
    _verify_result_table(
        pyexasol_connection,
        "describe_builtin_function",
        key_column="name",
        other_columns=["description", "types", "usage-notes", "example"],
        expected_keys=["TO_DATE"],
        name="to_date",
    )
