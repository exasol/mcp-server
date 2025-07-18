import asyncio
import json
from test.utils.db_objects import (
    ExaColumn,
    ExaDbObject,
)
from typing import Any

import pytest
from fastmcp import Client
from pyexasol import ExaConnection

from exasol.ai.mcp.server.mcp_server import (
    ExasolMCPServer,
    McpServerSettings,
    MetaColumnSettings,
    MetaSettings,
)


async def _run_tool_async(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    exa_server = ExasolMCPServer(connection, config)
    async with Client(exa_server) as client:
        return await client.call_tool(tool_name, kwargs)


def _run_tool(
    connection: ExaConnection, config: McpServerSettings, tool_name: str, **kwargs
):
    return asyncio.run(_run_tool_async(connection, config, tool_name, **kwargs))


def _result_sort_func(d: dict[str, Any]) -> str:
    return ",".join(str(v) for v in d.values())


def _get_list_result_json(result) -> list[dict[str, Any]]:
    result_json = json.loads(result.content[0].text)
    return sorted(result_json, key=_result_sort_func)


def _get_expected_db_obj_json(
    db_obj: ExaDbObject, conf: MetaSettings
) -> dict[str, Any]:
    obj_json = {conf.name_column: db_obj.name, conf.comment_column: db_obj.comment}
    if isinstance(db_obj, ExaColumn) and isinstance(conf, MetaColumnSettings):
        obj_json[conf.type_column] = db_obj.type
        obj_json[conf.primary_key_column] = db_obj.primary_key
        obj_json[conf.foreign_key_column] = db_obj.foreign_key
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
                name_column="the_name",
                comment_column="the_comment",
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
            name_column="the_name",
            comment_column="the_comment",
            like_pattern="%resort%" if use_like else "",
            regexp_pattern=".*resort.*" if use_regexp else "",
        ),
        views=MetaSettings(
            enable=enable_views,
            name_column="the_name",
            comment_column="the_comment",
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
            name_column="the_name",
            comment_column="the_comment",
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
            name_column="the_name",
            comment_column="the_comment",
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
            name_column="the_name",
            comment_column="the_comment",
            type_column="the_type",
            primary_key_column="the_primary_key",
            foreign_key_column="the_foreign_key",
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
