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
        ToolHints(tool_name="list_schemas", read_only=True),
        ToolHints(tool_name="find_schemas", read_only=True),
        ToolHints(tool_name="list_tables", read_only=True),
        ToolHints(tool_name="find_tables", read_only=True),
        ToolHints(tool_name="list_functions", read_only=True),
        ToolHints(tool_name="find_functions", read_only=True),
        ToolHints(tool_name="list_scripts", read_only=True),
        ToolHints(tool_name="find_scripts", read_only=True),
        ToolHints(tool_name="describe_table", read_only=True),
        ToolHints(tool_name="describe_function", read_only=True),
        ToolHints(tool_name="describe_script", read_only=True),
        ToolHints(tool_name="execute_query", read_only=True),
        ToolHints(tool_name="execute_write_query", destructive=True),
        ToolHints(tool_name="list_sql_types", read_only=True, idempotent=True),
        ToolHints(tool_name="list_system_tables", read_only=True, idempotent=True),
        ToolHints(tool_name="describe_system_table", read_only=True, idempotent=True),
        ToolHints(tool_name="list_statistics_tables", read_only=True, idempotent=True),
        ToolHints(
            tool_name="describe_statistics_table", read_only=True, idempotent=True
        ),
        ToolHints(tool_name="list_keywords", read_only=True, idempotent=True),
        ToolHints(
            tool_name="builtin_function_categories", read_only=True, idempotent=True
        ),
        ToolHints(tool_name="list_builtin_functions", read_only=True, idempotent=True),
        ToolHints(
            tool_name="describe_builtin_function", read_only=True, idempotent=True
        ),
    }
    assert tool_list == expected_tool_list
