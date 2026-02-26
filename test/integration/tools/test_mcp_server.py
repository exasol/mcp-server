from itertools import chain
from test.utils.db_objects import (
    ExaColumn,
    ExaConstraint,
    ExaDbObject,
    ExaFunction,
    ExaParameter,
    ExaSchema,
    ExaTable,
)
from test.utils.result_utils import (
    get_list_result_json,
    get_result_json,
    get_sort_result_json,
    list_tools,
    result_sort_func,
)
from test.utils.tool_utils import run_tool
from typing import Any

import pytest
from fastmcp.exceptions import ToolError

from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
    MetaColumnSettings,
    MetaListSettings,
    MetaParameterSettings,
)
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    COLUMNS_FIELD,
    COMMENT_FIELD,
    CONSTRAINT_COLUMNS_FIELD,
    CONSTRAINT_NAME_FIELD,
    CONSTRAINT_TYPE_FIELD,
    CONSTRAINTS_FIELD,
    DYNAMIC_INPUT_FIELD,
    DYNAMIC_OUTPUT_FIELD,
    EMITS_FIELD,
    INPUT_FIELD,
    NAME_FIELD,
    REFERENCED_COLUMNS_FIELD,
    REFERENCED_SCHEMA_FIELD,
    REFERENCED_TABLE_FIELD,
    RETURNS_FIELD,
    SCHEMA_FIELD,
    SQL_TYPE_FIELD,
    USAGE_FIELD,
)


def _get_expected_json(db_obj: ExaDbObject, schema_name: str | None) -> dict[str, Any]:
    expected_json = {NAME_FIELD: db_obj.name, COMMENT_FIELD: db_obj.comment}
    if schema_name:
        expected_json[SCHEMA_FIELD] = schema_name
    return expected_json


def _get_expected_list_json(
    db_objects: list[ExaDbObject],
    name_part: str,
    conf: MetaListSettings,
    schema_name: str | None = None,
) -> list[dict[str, Any]]:
    no_pattern = not (conf.like_pattern or conf.regexp_pattern)
    expected_json = [
        _get_expected_json(db_obj, schema_name)
        for db_obj in db_objects
        if no_pattern or (name_part in db_obj.name)
    ]
    return sorted(expected_json, key=result_sort_func)


def _get_expected_column_list_json(
    column_list: list[ExaColumn],
) -> list[dict[str, Any]]:
    expected_json = [
        {
            NAME_FIELD: col.name,
            SQL_TYPE_FIELD: col.type,
            COMMENT_FIELD: col.comment,
        }
        for col in column_list
    ]
    return sorted(expected_json, key=result_sort_func)


def _get_expected_constraint_list_json(
    constraint_list: list[ExaConstraint], schema_name: str
) -> list[dict[str, Any]]:
    expected_json = [
        {
            CONSTRAINT_NAME_FIELD: cons.name,
            CONSTRAINT_TYPE_FIELD: cons.type,
            CONSTRAINT_COLUMNS_FIELD: ",".join(cons.columns),
            REFERENCED_SCHEMA_FIELD: (
                schema_name if cons.type == "FOREIGN KEY" else None
            ),
            REFERENCED_TABLE_FIELD: cons.ref_table,
            REFERENCED_COLUMNS_FIELD: (
                None if cons.ref_columns is None else ",".join(cons.ref_columns)
            ),
        }
        for cons in constraint_list
    ]
    return sorted(expected_json, key=result_sort_func)


def _get_expected_table_json(table: ExaTable, schema_name: str) -> dict[str, Any]:
    return {
        SCHEMA_FIELD: schema_name,
        NAME_FIELD: table.name,
        COLUMNS_FIELD: _get_expected_column_list_json(table.columns),
        CONSTRAINTS_FIELD: _get_expected_constraint_list_json(
            table.constraints, schema_name
        ),
        COMMENT_FIELD: table.comment,
    }


def _get_expected_param_list_json(
    param_list: list[ExaParameter],
) -> list[dict[str, str]]:
    return [
        {NAME_FIELD: param.name, SQL_TYPE_FIELD: param.type, COMMENT_FIELD: None}
        for param in param_list
    ]


def _get_expected_param_json(func: ExaFunction, schema_name: str) -> dict[str, Any]:
    expected_json = {
        SCHEMA_FIELD: schema_name,
        NAME_FIELD: func.name,
        INPUT_FIELD: _get_expected_param_list_json(func.inputs),
        DYNAMIC_INPUT_FIELD: False,
        COMMENT_FIELD: func.comment,
    }
    if func.emits:
        expected_json[EMITS_FIELD] = _get_expected_param_list_json(func.emits)
        expected_json[DYNAMIC_OUTPUT_FIELD] = False
    if func.returns:
        expected_json[RETURNS_FIELD] = func.returns
    return expected_json


def _get_db_name_param(db_obj: ExaDbObject, case_sensitive: bool) -> str:
    """
    Returns the object name as a parameter for a tool call.
    If an object selection is case-insensitive, the object name case is swapped
    in order to test this feature.
    """
    if case_sensitive:
        return db_obj.name
    else:
        return db_obj.name.swapcase()


def _get_schema_param(schema: ExaSchema, restricted: bool, case_sensitive: bool) -> str:
    """
    Returns the schema name as a parameter for a tool call.
    If the schema visibility is restricted to one schema the schema_name parameter
    should not be provided in a call to a tool.
    """
    if restricted:
        return ""
    else:
        return _get_db_name_param(schema, case_sensitive)


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
    result = list_tools(pyexasol_connection, config)
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
        result = run_tool(pyexasol_connection, config, "list_schemas")
        result_json = get_list_result_json(result)
        expected_json = _get_expected_list_json([schema], schema.name, config.schemas)
        if use_like or use_regexp:
            assert result_json == expected_json
        else:
            assert expected_json[0] in result_json


@pytest.mark.parametrize("language", ["", "english"])
def test_find_schemas(
    pyexasol_connection, setup_database, db_schemas, language
) -> None:
    config = McpServerSettings(
        schemas=MetaListSettings(enable=True),
        language=language,
    )
    for schema in db_schemas:
        # Will test on new schemas only, where the result is more reliable.
        if not schema.is_new:
            continue
        result = run_tool(
            pyexasol_connection, config, "find_schemas", keywords=schema.keywords
        )
        result_json = get_result_json(result)[0]
        expected_json = {NAME_FIELD: schema.name, COMMENT_FIELD: schema.comment}
        assert result_json == expected_json


@pytest.mark.parametrize(
    ["enable_tables", "enable_views", "use_like", "use_regexp", "case_sensitive"],
    [
        (True, True, False, False, False),  # both-all
        (True, True, False, False, True),  # both-all-case-sensitive
        (True, True, True, False, False),  # both-like
        (True, True, False, True, False),  # both-regexp
        (True, False, False, False, False),  # tables-all
        (True, False, True, False, False),  # tables-like
        (True, False, True, False, True),  # tables-like-case-sensitive
        (True, False, False, True, False),  # tables-regexp
        (False, True, False, False, False),  # views-all
        (False, True, True, False, False),  # views-like
        (False, True, False, True, False),  # views-regexp
        (False, True, False, True, True),  # views-regexp-case-sensitive
    ],
    ids=[
        "both-all",
        "both-all-case-sensitive",
        "both-like",
        "both-regexp",
        "tables-all",
        "tables-like",
        "tables-like-case-sensitive",
        "tables-regexp",
        "views-all",
        "views-like",
        "views-regexp",
        "views-regexp-case-sensitive",
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
    case_sensitive,
) -> None:
    """
    Test the `list_tables` tool with various combinations of configuration parameters.

    For the pre-existing schema we will not test the listing of all tables because there
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
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        if (not schema.is_new) and (not use_like) and (not use_regexp):
            continue
        result = run_tool(
            pyexasol_connection,
            config,
            "list_tables",
            schema_name=_get_schema_param(schema, False, case_sensitive),
        )
        result_json = get_list_result_json(result)
        expected_result: list[dict[str, Any]] = []
        if enable_tables:
            expected_json = _get_expected_list_json(
                db_tables, "resort", config.tables, schema.name
            )
            expected_result.extend(expected_json)
        if enable_views:
            expected_json = _get_expected_list_json(
                db_views, "run", config.views, schema.name
            )
            expected_result.extend(expected_json)
        expected_json = sorted(expected_result, key=result_sort_func)
        assert result_json == expected_json


@pytest.mark.parametrize(
    ["language", "use_like", "use_regexp", "case_sensitive"],
    [
        ("", False, False, False),  # all
        ("", False, False, True),  # all-case-sensitive
        ("", True, False, False),  # like
        ("", False, True, False),  # regexp
        ("english", False, False, False),  # eng-all
        ("english", True, False, False),  # eng-like
        ("english", False, True, False),  # eng-regexp
    ],
    ids=[
        "all",
        "all-case-sensitive",
        "like",
        "regexp",
        "eng-all",
        "eng-like",
        "eng-regexp",
    ],
)
def test_find_tables(
    pyexasol_connection,
    setup_database,
    db_schemas,
    db_tables,
    db_views,
    language,
    use_like,
    use_regexp,
    case_sensitive,
) -> None:
    for schema in db_schemas:
        #  Will test on new schemas only, where the result can be guaranteed.
        if not schema.is_new:
            continue
        config = McpServerSettings(
            schemas=MetaListSettings(
                like_pattern=schema.name if use_like else "",
                regexp_pattern=schema.name if use_regexp else "",
            ),
            tables=MetaListSettings(enable=True),
            views=MetaListSettings(enable=True),
            language=language,
            case_sensitive=case_sensitive,
        )
        for table in chain(db_tables, db_views):
            result = run_tool(
                pyexasol_connection,
                config,
                "find_tables",
                keywords=table.keywords,
                schema_name=_get_schema_param(
                    schema, use_like or use_regexp, case_sensitive
                ),
            )

            result_json = get_result_json(result)[0]
            expected_json = {
                NAME_FIELD: table.name,
                COMMENT_FIELD: table.comment,
                SCHEMA_FIELD: schema.name,
            }
            assert result_json == expected_json


@pytest.mark.parametrize(
    ["use_like", "use_regexp", "case_sensitive"],
    [
        (False, False, False),
        (False, False, True),
        (True, False, False),
        (False, True, False),
    ],
    ids=["all", "all-case-sensitive", "like", "regexp"],
)
def test_list_functions(
    pyexasol_connection,
    setup_database,
    db_schemas,
    db_functions,
    use_like,
    use_regexp,
    case_sensitive,
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
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        if (not schema.is_new) and (not use_like) and (not use_regexp):
            continue
        result = run_tool(
            pyexasol_connection,
            config,
            "list_functions",
            schema_name=_get_schema_param(schema, False, case_sensitive),
        )
        result_json = get_list_result_json(result)
        expected_json = _get_expected_list_json(
            db_functions, "cut", config.functions, schema.name
        )
        assert result_json == expected_json


@pytest.mark.parametrize(
    ["language", "use_like", "use_regexp", "case_sensitive"],
    [
        ("", False, False, False),  # all
        ("", False, False, True),  # all-case-sensitive
        ("", True, False, False),  # like
        ("", False, True, False),  # regexp
        ("english", False, False, False),  # eng-all
        ("english", True, False, False),  # eng-like
        ("english", False, True, False),  # eng-regexp
    ],
    ids=[
        "all",
        "all-case-sensitive",
        "like",
        "regexp",
        "eng-all",
        "eng-like",
        "eng-regexp",
    ],
)
def test_find_functions(
    pyexasol_connection,
    setup_database,
    db_schemas,
    db_functions,
    language,
    use_like,
    use_regexp,
    case_sensitive,
) -> None:
    for schema in db_schemas:
        # Will test on new schemas only, where the result can be guaranteed.
        if not schema.is_new:
            continue
        config = McpServerSettings(
            schemas=MetaListSettings(
                like_pattern=schema.name if use_like else "",
                regexp_pattern=schema.name if use_regexp else "",
            ),
            functions=MetaListSettings(enable=True),
            language=language,
            case_sensitive=case_sensitive,
        )
        for func in db_functions:
            result = run_tool(
                pyexasol_connection,
                config,
                "find_functions",
                keywords=func.keywords,
                schema_name=_get_schema_param(
                    schema, use_like or use_regexp, case_sensitive
                ),
            )
            result_json = get_result_json(result)[0]
            expected_json = {
                NAME_FIELD: func.name,
                COMMENT_FIELD: func.comment,
                SCHEMA_FIELD: schema.name,
            }
            assert result_json == expected_json


@pytest.mark.parametrize(
    ["use_like", "use_regexp", "case_sensitive"],
    [
        (False, False, False),
        (False, False, True),
        (True, False, False),
        (False, True, False),
    ],
    ids=["all", "all-case-sensitive", "like", "regexp"],
)
def test_list_scripts(
    pyexasol_connection,
    setup_database,
    db_schemas,
    db_scripts,
    use_like,
    use_regexp,
    case_sensitive,
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
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        if (not schema.is_new) and (not use_like) and (not use_regexp):
            continue
        result = run_tool(
            pyexasol_connection,
            config,
            "list_scripts",
            schema_name=_get_schema_param(schema, False, case_sensitive),
        )
        result_json = get_list_result_json(result)
        expected_json = _get_expected_list_json(
            db_scripts, "fibo", config.scripts, schema.name
        )
        assert result_json == expected_json


@pytest.mark.parametrize(
    ["language", "use_like", "use_regexp", "case_sensitive"],
    [
        ("", False, False, False),  # all
        ("", False, False, True),  # all-case-sensitive
        ("", True, False, False),  # like
        ("", False, True, False),  # regexp
        ("english", False, False, False),  # eng-all
        ("english", True, False, False),  # eng-like
        ("english", False, True, False),  # eng-regexp
    ],
    ids=[
        "all",
        "all-case-sensitive",
        "like",
        "regexp",
        "eng-all",
        "eng-like",
        "eng-regexp",
    ],
)
def test_find_scripts(
    pyexasol_connection,
    setup_database,
    db_schemas,
    db_scripts,
    language,
    use_like,
    use_regexp,
    case_sensitive,
) -> None:
    for schema in db_schemas:
        # Will test on new schemas only, where the result can be guaranteed.
        if not schema.is_new:
            continue
        config = McpServerSettings(
            schemas=MetaListSettings(
                like_pattern=schema.name if use_like else "",
                regexp_pattern=schema.name if use_regexp else "",
            ),
            scripts=MetaListSettings(enable=True),
            language=language,
        )
        for script in db_scripts:
            result = run_tool(
                pyexasol_connection,
                config,
                "find_scripts",
                keywords=script.keywords,
                schema_name=_get_schema_param(
                    schema, use_like or use_regexp, case_sensitive
                ),
            )
            result_json = get_result_json(result)[0]
            expected_json = {
                NAME_FIELD: script.name,
                COMMENT_FIELD: script.comment,
                SCHEMA_FIELD: schema.name,
            }
            assert result_json == expected_json


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_table(
    pyexasol_connection, setup_database, db_schemas, db_tables, case_sensitive
) -> None:
    """
    Test the `describe_table` tool. The tool is tested on each table of every schema.
    """
    config = McpServerSettings(
        columns=MetaColumnSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        for table in db_tables:
            result = run_tool(
                pyexasol_connection,
                config,
                "describe_table",
                schema_name=_get_db_name_param(schema, case_sensitive),
                table_name=_get_db_name_param(table, case_sensitive),
            )
            result_json = get_sort_result_json(result)
            expected_json = _get_expected_table_json(table, schema.name)
            assert result_json == expected_json


def test_describe_sys_table(pyexasol_connection) -> None:
    """
    Test the `describe_table` tool, passing the name of a system table to it.
    """
    config = McpServerSettings(columns=MetaColumnSettings(enable=True))
    result = run_tool(
        pyexasol_connection,
        config,
        "describe_table",
        schema_name="SYS",
        table_name="EXA_ALL_COLUMNS",
    )
    result_json = get_result_json(result)
    result_columns = result_json["columns"]
    column_names = [col[NAME_FIELD] for col in result_columns]
    assert all(
        expected_column in column_names
        for expected_column in ["COLUMN_NAME", "COLUMN_TYPE", "COLUMN_COMMENT"]
    )


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_view_comment(
    pyexasol_connection, setup_database, db_schemas, db_views, case_sensitive
) -> None:
    config = McpServerSettings(
        columns=MetaColumnSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        for view in db_views:
            result = run_tool(
                pyexasol_connection,
                config,
                "describe_table",
                schema_name=_get_db_name_param(schema, case_sensitive),
                table_name=_get_db_name_param(view, case_sensitive),
            )
            result_json = get_sort_result_json(result)
            assert result_json[COMMENT_FIELD] == view.comment


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
    is not provided.
    """
    config = McpServerSettings(
        columns=MetaColumnSettings(enable=True),
        parameters=MetaParameterSettings(enable=True),
    )
    with pytest.raises(ToolError):
        run_tool(pyexasol_connection, config, tool_name=tool_name, **other_kwargs)


@pytest.mark.parametrize(
    "tool_name", ["describe_table", "describe_function", "describe_script"]
)
def test_describe_no_db_object_name(
    pyexasol_connection, setup_database, db_schemas, tool_name
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
        with pytest.raises(ToolError):
            run_tool(
                pyexasol_connection,
                config,
                tool_name=tool_name,
                schema_name=schema.name,
            )


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_function(
    pyexasol_connection, setup_database, db_schemas, db_functions, case_sensitive
) -> None:
    """
    Test the `describe_function` tool. The tool is tested on each function
    of every schema.
    """
    config = McpServerSettings(
        parameters=MetaParameterSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        for func in db_functions:
            result = run_tool(
                pyexasol_connection,
                config,
                "describe_function",
                schema_name=_get_db_name_param(schema, case_sensitive),
                func_name=_get_db_name_param(func, case_sensitive),
            )
            result_json = get_result_json(result)
            expected_json = _get_expected_param_json(func, schema.name)
            expected_json[USAGE_FIELD] = ""
            assert result_json == expected_json


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_script(
    pyexasol_connection, setup_database, db_schemas, db_scripts, case_sensitive
) -> None:
    """
    Test the `describe_script` tool. The tool is tested on each script
    of every schema.
    """
    config = McpServerSettings(
        parameters=MetaParameterSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        for script in db_scripts:
            result = run_tool(
                pyexasol_connection,
                config,
                "describe_script",
                schema_name=_get_db_name_param(schema, case_sensitive),
                script_name=_get_db_name_param(script, case_sensitive),
            )
            result_json = get_result_json(result)
            # The call example message is properly tested in the unit tests.
            # Here we just verify that it exists.
            assert USAGE_FIELD in result_json
            result_json.pop(USAGE_FIELD)
            expected_json = _get_expected_param_json(script, schema.name)
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
            result = run_tool(
                pyexasol_connection, config, tool_name="execute_query", query=query
            )
            if result.content:
                result_json = get_list_result_json(result)
            else:
                result_json = []
            expected_json = [
                {col.name: col_value for col, col_value in zip(table.columns, row)}
                for row in table.rows
            ]
            expected_json.sort(key=result_sort_func)
            assert result_json == expected_json


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
                run_tool(
                    pyexasol_connection, config, tool_name="execute_query", query=query
                )
