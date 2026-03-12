from test.utils.result_utils import (
    ToolHints,
    get_tool_hints,
    list_tools,
)

from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
    MetaColumnSettings,
    MetaListSettings,
    MetaParameterSettings,
)


def test_tool_hints(pyexasol_connection) -> None:
    """
    This test validates hints the tool annotations.
    """
    enable_meta_list = MetaListSettings(enable=True)
    config = McpServerSettings(
        schemas=enable_meta_list,
        tables=enable_meta_list,
        views=enable_meta_list,
        functions=enable_meta_list,
        scripts=enable_meta_list,
        columns=MetaColumnSettings(enable=True),
        parameters=MetaParameterSettings(enable=True),
        enable_read_query=True,
        enable_write_query=True,
    )
    result = list_tools(pyexasol_connection, config)

    tool_list = {get_tool_hints(tool) for tool in result}
    expected_tool_list = {
        ToolHints(tool_name="list_exasol_schemas", read_only=True),
        ToolHints(tool_name="find_exasol_schemas", read_only=True),
        ToolHints(tool_name="list_exasol_tables_and_views", read_only=True),
        ToolHints(tool_name="find_exasol_tables_and_views", read_only=True),
        ToolHints(tool_name="list_exasol_custom_functions", read_only=True),
        ToolHints(tool_name="find_exasol_custom_functions", read_only=True),
        ToolHints(tool_name="list_exasol_user_defined_functions", read_only=True),
        ToolHints(tool_name="find_exasol_user_defined_functions", read_only=True),
        ToolHints(tool_name="describe_exasol_table_or_view", read_only=True),
        ToolHints(tool_name="describe_exasol_custom_function", read_only=True),
        ToolHints(tool_name="describe_exasol_user_defined_function", read_only=True),
        ToolHints(tool_name="validate_exasol_query", read_only=True),
        ToolHints(tool_name="execute_exasol_query", read_only=True),
        ToolHints(tool_name="execute_exasol_write_query", destructive=True),
        ToolHints(tool_name="list_exasol_sql_types", read_only=True, idempotent=True),
        ToolHints(
            tool_name="list_exasol_system_tables", read_only=True, idempotent=True
        ),
        ToolHints(
            tool_name="describe_exasol_system_table", read_only=True, idempotent=True
        ),
        ToolHints(
            tool_name="list_exasol_statistics_tables", read_only=True, idempotent=True
        ),
        ToolHints(
            tool_name="describe_exasol_statistics_table",
            read_only=True,
            idempotent=True,
        ),
        ToolHints(tool_name="list_exasol_keywords", read_only=True, idempotent=True),
        ToolHints(
            tool_name="list_exasol_built_in_function_categories",
            read_only=True,
            idempotent=True,
        ),
        ToolHints(
            tool_name="list_exasol_built_in_functions", read_only=True, idempotent=True
        ),
        ToolHints(
            tool_name="describe_exasol_built_in_function",
            read_only=True,
            idempotent=True,
        ),
    }
    assert tool_list == expected_tool_list
