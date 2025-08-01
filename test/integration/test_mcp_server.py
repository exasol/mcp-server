import asyncio
import json
from test.utils.db_objects import (
    ExaColumn,
    ExaConstraint,
    ExaDbObject,
    ExaFunction,
    ExaParameter,
    ExaTable,
)
from typing import (
    Any,
    cast,
)

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError
from pyexasol import ExaConnection

from exasol.ai.mcp.server.mcp_server import ExasolMCPServer
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
    MetaColumnSettings,
    MetaListSettings,
    MetaParameterSettings,
)


async def _run_tool_async(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    exa_server = ExasolMCPServer(connection, config)
    async with Client(exa_server) as client:
        return await client.call_tool(tool_name, kwargs)


async def _list_tools_async(connection: ExaConnection, config: McpServerSettings):
    exa_server = ExasolMCPServer(connection, config)
    async with Client(exa_server) as client:
        return await client.list_tools()


def _run_tool(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    return asyncio.run(_run_tool_async(connection, config, tool_name, **kwargs))


def _list_tools(connection: ExaConnection, config: McpServerSettings):
    return asyncio.run(_list_tools_async(connection, config))


def _result_sort_func(d: Any) -> str:
    if isinstance(d, dict):
        return ",".join(str(d[key]) for key in sorted(d.keys()))
    return str(d)


def _get_result_json(result) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(result.content[0].text))


def _get_sort_result_json(result) -> dict[str, Any]:
    result_json = _get_result_json(result)
    return {
        key: sorted(val, key=_result_sort_func) if isinstance(val, list) else val
        for key, val in result_json.items()
    }


def _get_list_result_json(result) -> ExaDbResult:
    result_json = _get_result_json(result)
    unsorted = ExaDbResult(**result_json)
    if isinstance(unsorted.result, list):
        return ExaDbResult(sorted(unsorted.result, key=_result_sort_func))
    return unsorted


def _get_expected_list_json(
    db_objects: list[ExaDbObject], name_part: str, conf: MetaListSettings
) -> ExaDbResult:
    no_pattern = not (conf.like_pattern or conf.regexp_pattern)
    expected_json = [
        {conf.name_field: db_obj.name, conf.comment_field: db_obj.comment}
        for db_obj in db_objects
        if no_pattern or (name_part in db_obj.name)
    ]
    return ExaDbResult(sorted(expected_json, key=_result_sort_func))


def _get_expected_column_list_json(
    column_list: list[ExaColumn], conf: MetaColumnSettings
) -> list[dict[str, Any]]:
    expected_json = [
        {
            conf.name_field: col.name,
            conf.type_field: col.type,
            conf.comment_field: col.comment,
        }
        for col in column_list
    ]
    return sorted(expected_json, key=_result_sort_func)


def _get_expected_constraint_list_json(
    constraint_list: list[ExaConstraint], conf: MetaColumnSettings, schema_name: str
) -> list[dict[str, Any]]:
    expected_json = [
        {
            conf.constraint_name_field: cons.name,
            conf.constraint_type_field: cons.type,
            conf.constraint_columns_field: ",".join(cons.columns),
            conf.referenced_schema_field: (
                schema_name if cons.type == "FOREIGN KEY" else None
            ),
            conf.referenced_table_field: cons.ref_table,
            conf.referenced_columns_field: (
                None if cons.ref_columns is None else ",".join(cons.ref_columns)
            ),
        }
        for cons in constraint_list
    ]
    return sorted(expected_json, key=_result_sort_func)


def _get_expected_table_json(
    table: ExaTable, conf: MetaColumnSettings, schema_name: str
) -> dict[str, Any]:
    return {
        conf.columns_field: _get_expected_column_list_json(table.columns, conf),
        conf.constraints_field: _get_expected_constraint_list_json(
            table.constraints, conf, schema_name
        ),
    }


def _get_expected_param_list_json(
    param_list: list[ExaParameter], conf: MetaParameterSettings
) -> list[dict[str, str]]:
    return [
        {conf.name_field: param.name, conf.type_field: param.type}
        for param in param_list
    ]


def _get_expected_param_json(
    func: ExaFunction, conf: MetaParameterSettings
) -> dict[str, Any]:
    expected_json = {conf.input_field: _get_expected_param_list_json(func.inputs, conf)}
    if func.emits:
        expected_json[conf.emit_field] = _get_expected_param_list_json(func.emits, conf)
    if func.returns:
        expected_json[conf.return_field] = {conf.type_field: func.returns}
    return expected_json


@pytest.mark.parametrize(
    ["tool_name", "meta_types"],
    [
        ("list_schemas", ["schemas"]),
        ("list_tables", ["tables", "views"]),
        ("list_functions", ["functions"]),
        ("list_scripts", ["scripts"]),
        ("describe_table", ["columns"]),
        ("describe_function", ["parameters"]),
        ("describe_script", ["parameters"]),
    ],
)
def test_tool_disabled(
    pyexasol_connection,
    tool_name,
    meta_types,
) -> None:
    """
    This test validates disabling a tool via the configuration.
    """
    config_dict = {meta_type: {"enable": False} for meta_type in meta_types}
    config = McpServerSettings.model_validate(config_dict)
    result = _list_tools(pyexasol_connection, config)
    tool_list = [tool.name for tool in result]
    assert tool_name not in tool_list


@pytest.mark.parametrize(
    ["use_like", "use_regexp"],
    [(False, False), (True, False), (False, True)],
    ids=["all", "like", "regexp"],
)
def test_list_schemas(
    pyexasol_connection, setup_database, db_schemas, use_like, use_regexp
) -> None:
    """
    Test the `list_schemas` tool with various combinations of configuration parameters.
    If the output is restricted to one schema the test validates the exact result.
    Otherwise, it checks that the expected schema is present in the output.
    """
    for schema in db_schemas:
        config = McpServerSettings(
            schemas=MetaListSettings(
                enable=True,
                like_pattern=schema.name if use_like else "",
                regexp_pattern=schema.name if use_regexp else "",
            )
        )
        result = _run_tool(pyexasol_connection, config, "list_schemas")
        result_json = _get_list_result_json(result)
        expected_json = _get_expected_list_json([schema], schema.name, config.schemas)
        if use_like or use_regexp:
            assert result_json == expected_json
        else:
            assert expected_json.result[0] in result_json.result


@pytest.mark.parametrize(
    ["enable_tables", "enable_views", "use_like", "use_regexp"],
    [
        (True, True, False, False),
        (True, True, True, False),
        (True, True, False, True),
        (True, False, False, False),
        (True, False, True, False),
        (True, False, False, True),
        (False, True, False, False),
        (False, True, True, False),
        (False, True, False, True),
    ],
    ids=[
        "both-all",
        "both-like",
        "both-regexp",
        "tables-all",
        "tables-like",
        "tables-regexp",
        "views-all",
        "views-like",
        "views-regexp",
    ],
)
def test_list_tables(
    pyexasol_connection,
    setup_database,
    db_schemas,
    db_tables,
    db_views,
    enable_tables,
    enable_views,
    use_like,
    use_regexp,
) -> None:
    """
    Test the `list_tables` tool with various combinations of configuration parameters.

    The fixture tables and views are created in pre-existing schema and a newly created
    one. The pre-existing schema must be opened. When listing tables in this schema we
    will not specify the schema name in the call to test that picking the current schema
    works. For this schema we will not test the listing of all tables because there
    could be other left over tables we are not aware of here. For the newly created one
    we will test all cases in regard to the selection patterns.

    The table selection cases comprise combinations of tables/views: on/off, and the
    selection pattern: like/regexp/none.
    """
    config = McpServerSettings(
        tables=MetaListSettings(
            enable=enable_tables,
            like_pattern="%resort%" if use_like else "",
            regexp_pattern=".*resort.*" if use_regexp else "",
        ),
        views=MetaListSettings(
            enable=enable_views,
            like_pattern="%run%" if use_like else "",
            regexp_pattern=".*run.*" if use_regexp else "",
        ),
    )
    for schema in db_schemas:
        if (not schema.is_new) and (not use_like) and (not use_regexp):
            continue
        result = _run_tool(
            pyexasol_connection,
            config,
            "list_tables",
            schema_name=schema.schema_name_arg,
        )
        result_json = _get_list_result_json(result)
        expected_result: list[dict[str, Any]] = []
        if enable_tables:
            expected_json = _get_expected_list_json(db_tables, "resort", config.tables)
            expected_result.extend(expected_json.result)
        if enable_views:
            expected_json = _get_expected_list_json(db_views, "run", config.views)
            expected_result.extend(expected_json.result)
        expected_json = ExaDbResult(sorted(expected_result, key=_result_sort_func))
        assert result_json == expected_json


@pytest.mark.parametrize(
    ["use_like", "use_regexp"],
    [(False, False), (True, False), (False, True)],
    ids=["all", "like", "regexp"],
)
def test_list_functions(
    pyexasol_connection, setup_database, db_schemas, db_functions, use_like, use_regexp
) -> None:
    """
    Test the `list_functions` tool with various combinations of configuration parameters.
    There are same considerations as in the test for `list_tables`.
    """
    config = McpServerSettings(
        functions=MetaListSettings(
            enable=True,
            like_pattern="cut%" if use_like else "",
            regexp_pattern="cut.*" if use_regexp else "",
        )
    )
    for schema in db_schemas:
        if (not schema.is_new) and (not use_like) and (not use_regexp):
            continue
        result = _run_tool(
            pyexasol_connection,
            config,
            "list_functions",
            schema_name=schema.schema_name_arg,
        )
        result_json = _get_list_result_json(result)
        expected_json = _get_expected_list_json(db_functions, "cut", config.functions)
        assert result_json == expected_json


@pytest.mark.parametrize(
    ["use_like", "use_regexp"],
    [(False, False), (True, False), (False, True)],
    ids=["all", "like", "regexp"],
)
def test_list_scripts(
    pyexasol_connection, setup_database, db_schemas, db_scripts, use_like, use_regexp
) -> None:
    """
    Test the `list_scripts` tool with various combinations of configuration parameters.
    There are same considerations as in the test for `list_tables`.
    """
    config = McpServerSettings(
        scripts=MetaListSettings(
            enable=True,
            like_pattern="fibo%" if use_like else "",
            regexp_pattern="fibo.*" if use_regexp else "",
        )
    )
    for schema in db_schemas:
        if (not schema.is_new) and (not use_like) and (not use_regexp):
            continue
        result = _run_tool(
            pyexasol_connection,
            config,
            "list_scripts",
            schema_name=schema.schema_name_arg,
        )
        result_json = _get_list_result_json(result)
        expected_json = _get_expected_list_json(db_scripts, "fibo", config.scripts)
        assert result_json == expected_json


def test_describe_table(
    pyexasol_connection, setup_database, db_schemas, db_tables
) -> None:
    """
    Test the `describe_table` tool. The tool is tested on each table of every schema.
    """
    config = McpServerSettings(
        columns=MetaColumnSettings(
            enable=True,
        )
    )
    for schema in db_schemas:
        for table in db_tables:
            result = _run_tool(
                pyexasol_connection,
                config,
                "describe_table",
                schema_name=schema.schema_name_arg,
                table_name=table.name,
            )
            result_json = _get_sort_result_json(result)
            expected_json = _get_expected_table_json(table, config.columns, schema.name)
            assert result_json == expected_json


@pytest.mark.parametrize(
    ["tool_name", "other_kwargs"],
    [
        ("describe_table", {"table_name": "ski_resort"}),
        ("describe_function", {"func_name": "factorial"}),
        ("describe_script", {"script_name": "fibonacci"}),
    ],
    ids=["describe_table", "describe_function", "describe_script"],
)
def test_describe_no_schema_name(
    pyexasol_connection, setup_database, tool_name, other_kwargs
) -> None:
    """
    The test validates that the `describe_xxx` tool returns an error if the schema
    is not provided and no current schema opened.
    """
    config = McpServerSettings(
        columns=MetaColumnSettings(enable=True),
        parameters=MetaParameterSettings(enable=True),
    )
    current_schema = pyexasol_connection.current_schema()
    try:
        if current_schema:
            pyexasol_connection.execute("CLOSE SCHEMA")
        with pytest.raises(ToolError, match="Schema name"):
            _run_tool(pyexasol_connection, config, tool_name=tool_name, **other_kwargs)
    finally:
        if current_schema:
            pyexasol_connection.execute(f'OPEN SCHEMA "{current_schema}"')


@pytest.mark.parametrize(
    ["tool_name", "error_match"],
    [
        ("describe_table", "Table name"),
        ("describe_function", "Function or script name"),
        ("describe_script", "Function or script name"),
    ],
    ids=["describe_table", "describe_function", "describe_script"],
)
def test_describe_no_db_object_name(
    pyexasol_connection, setup_database, db_schemas, tool_name, error_match
) -> None:
    """
    The test validates that the `describe_xxx` returns an error if the name of the
    db object to be described is not provided.
    """
    config = McpServerSettings(
        columns=MetaColumnSettings(enable=True),
        parameters=MetaParameterSettings(enable=True),
    )
    for schema in db_schemas:
        with pytest.raises(ToolError, match=error_match):
            _run_tool(
                pyexasol_connection,
                config,
                tool_name=tool_name,
                schema_name=schema.schema_name_arg,
            )


def test_describe_function(
    pyexasol_connection, setup_database, db_schemas, db_functions
) -> None:
    """
    Test the `describe_function` tool. The tool is tested on each function
    of every schema.
    """
    config = McpServerSettings(
        parameters=MetaParameterSettings(
            enable=True,
        )
    )
    for schema in db_schemas:
        for func in db_functions:
            result = _run_tool(
                pyexasol_connection,
                config,
                "describe_function",
                schema_name=schema.schema_name_arg,
                func_name=func.name,
            )
            result_json = _get_result_json(result)
            expected_json = _get_expected_param_json(func, config.parameters)
            assert result_json == expected_json


def test_describe_script(
    pyexasol_connection, setup_database, db_schemas, db_scripts
) -> None:
    """
    Test the `describe_script` tool. The tool is tested on each script
    of every schema.
    """
    config = McpServerSettings(
        parameters=MetaParameterSettings(
            enable=True,
        )
    )
    for schema in db_schemas:
        for script in db_scripts:
            result = _run_tool(
                pyexasol_connection,
                config,
                "describe_script",
                schema_name=schema.schema_name_arg,
                script_name=script.name,
            )
            result_json = _get_result_json(result)
            expected_json = _get_expected_param_json(script, config.parameters)
            assert result_json == expected_json


def test_execute_query(pyexasol_connection, setup_database, db_schemas, db_tables):
    """
    Test the `execute_query` tool. Runs the simplest SELECT query that grabs the entire
    content of a table and validates this content. The tool is tested on each table
    of every schema.
    """
    config = McpServerSettings(enable_read_query=True)
    for schema in db_schemas:
        for table in db_tables:
            query = f'SELECT * FROM "{schema.name}"."{table.name}"'
            result = _run_tool(
                pyexasol_connection, config, tool_name="execute_query", query=query
            )
            result_json = _get_list_result_json(result)
            expected_json = [
                {col.name: col_value for col, col_value in zip(table.columns, row)}
                for row in table.rows
            ]
            expected_json.sort(key=_result_sort_func)
            assert result_json == ExaDbResult(expected_json)


def test_execute_query_error(
    pyexasol_connection, setup_database, db_schemas, db_tables
):
    """
    The test validates that the `execute_query` tool fails if asked to execute a
    disallowed query.
    """
    config = McpServerSettings(enable_read_query=True)
    for schema in db_schemas:
        for table in db_tables:
            query = (
                f'SELECT * INTO TABLE "{schema.name}"."ANOTHER_TABLE" '
                f'FROM "{schema.name}"."{table.name}"'
            )
            with pytest.raises(ToolError):
                _run_tool(
                    pyexasol_connection, config, tool_name="execute_query", query=query
                )
