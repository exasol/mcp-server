import asyncio
import json
from test.utils.db_objects import (
    ExaColumn,
    ExaDbObject,
    ExaFunction,
    ExaParameter,
)
from typing import Any

import pytest
from fastmcp import Client
from pyexasol import ExaConnection

from exasol.ai.mcp.server.mcp_server import ExasolMCPServer
from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaColumnSettings,
    MetaParameterSettings,
    MetaSettings,
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


def _result_sort_func(d: dict[str, Any]) -> str:
    return ",".join(str(v) for v in d.values())


def _get_list_result_json(result) -> list[dict[str, Any]]:
    result_json = json.loads(result.content[0].text)
    if isinstance(result_json, list):
        return sorted(result_json, key=_result_sort_func)
    return [result_json]


def _get_expected_db_obj_json(
    db_obj: ExaDbObject, conf: MetaSettings
) -> dict[str, Any]:
    obj_json = {conf.name_field: db_obj.name, conf.comment_field: db_obj.comment}
    if isinstance(db_obj, ExaColumn) and isinstance(conf, MetaColumnSettings):
        obj_json[conf.type_field] = db_obj.type
        obj_json[conf.primary_key_field] = db_obj.primary_key
        obj_json[conf.foreign_key_field] = db_obj.foreign_key
    return obj_json


def _get_expected_list_json(
    db_objects: list[ExaDbObject], name_part: str, conf: MetaSettings
) -> list[dict[str, Any]]:
    no_pattern = not (conf.like_pattern or conf.regexp_pattern)
    expected_json = [
        _get_expected_db_obj_json(db_obj, conf)
        for db_obj in db_objects
        if no_pattern or (name_part in db_obj.name)
    ]
    return sorted(expected_json, key=_result_sort_func)


def _get_expected_param_list_json(
    param_list: list[ExaParameter], conf: MetaParameterSettings
) -> dict[str, str]:
    return [
        {conf.name_field: param.name, conf.type_field: param.type}
        for param in param_list
    ]


def _get_expected_param_json(
    func: ExaFunction, conf: MetaParameterSettings
) -> list[dict[str, Any]]:
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
            schemas=MetaSettings(
                enable=True,
                name_field="the_name",
                comment_field="the_comment",
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
            assert expected_json[0] in result_json


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
        tables=MetaSettings(
            enable=enable_tables,
            name_field="the_name",
            comment_field="the_comment",
            like_pattern="%resort%" if use_like else "",
            regexp_pattern=".*resort.*" if use_regexp else "",
        ),
        views=MetaSettings(
            enable=enable_views,
            name_field="the_name",
            comment_field="the_comment",
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
        expected_json: list[dict[str, Any]] = []
        if enable_tables:
            expected_json.extend(
                _get_expected_list_json(db_tables, "resort", config.tables)
            )
        if enable_views:
            expected_json.extend(_get_expected_list_json(db_views, "run", config.views))
        expected_json = sorted(expected_json, key=_result_sort_func)
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
        functions=MetaSettings(
            enable=True,
            name_field="the_name",
            comment_field="the_comment",
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
        scripts=MetaSettings(
            enable=True,
            name_field="the_name",
            comment_field="the_comment",
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


@pytest.mark.parametrize(
    ["use_like", "use_regexp"],
    [(False, False), (True, False), (False, True)],
    ids=["all", "like", "regexp"],
)
def test_describe_table(
    pyexasol_connection, setup_database, db_schemas, db_tables, use_like, use_regexp
) -> None:
    """
    Test the `describe_table` tool with various combinations of configuration
    parameters. The tool is tested on each table of every schema.
    """
    config = McpServerSettings(
        columns=MetaColumnSettings(
            enable=True,
            name_field="the_name",
            comment_field="the_comment",
            type_field="the_type",
            primary_key_field="the_primary_key",
            foreign_key_field="the_foreign_key",
            like_pattern="%id%" if use_like else "",
            regexp_pattern=".*id.*" if use_regexp else "",
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
            result_json = _get_list_result_json(result)
            expected_json = _get_expected_list_json(table.columns, "id", config.columns)
            assert result_json == expected_json


def test_describe_table_schema_error(
    pyexasol_connection, setup_database, db_tables
) -> None:
    """
    The test validates that the `describe_table` returns an error if the schema
    is not provided and no current schema opened.
    """
    config = McpServerSettings(columns=MetaColumnSettings(enable=True))
    current_schema = pyexasol_connection.current_schema()
    try:
        if current_schema:
            pyexasol_connection.execute("CLOSE SCHEMA")
        result = _run_tool(
            pyexasol_connection,
            config,
            "describe_table",
            table_name=db_tables[0].name,
        )
        result_json = _get_list_result_json(result)
        assert all(key == "error" for di in result_json for key in di.keys())
    finally:
        if current_schema:
            pyexasol_connection.execute(f'OPEN SCHEMA "{current_schema}"')


def test_describe_table_table_error(
    pyexasol_connection, setup_database, db_schemas
) -> None:
    """
    The test validates that the `describe_table` returns an error if the table
    name is not provided.
    """
    config = McpServerSettings(columns=MetaColumnSettings(enable=True))
    for schema in db_schemas:
        result = _run_tool(
            pyexasol_connection,
            config,
            "describe_table",
            schema_name=schema.schema_name_arg,
        )
        result_json = _get_list_result_json(result)
        assert all(key == "error" for di in result_json for key in di.keys())


def test_describe_scripts(
    pyexasol_connection, setup_database, db_schemas, db_scripts
) -> None:
    config = McpServerSettings(
        parameters=MetaParameterSettings(
            enable=True,
            name_field="the_name",
            type_field="the_type",
            input_field="inputs_params",
            emit_field="emit_params",
            return_field="return_type",
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
            result_json = json.loads(result.content[0].text)
            expected_json = _get_expected_param_json(script, config.parameters)
            assert result_json == expected_json
