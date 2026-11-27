from collections.abc import Generator
from itertools import chain
from typing import Any

import pyexasol
import pytest
from fastmcp.exceptions import ToolError

from exasol.ai.mcp.server.connection.db_connection import generic_db_error_message
from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
    MetaListSettings,
    MetaSettings,
)
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    COLUMNS_FIELD,
    COMMENT_FIELD,
    CONSTRAINT_COLUMNS_FIELD,
    CONSTRAINT_NAME_FIELD,
    CONSTRAINT_TYPE_FIELD,
    CONSTRAINTS_FIELD,
    CURRENT_PREPROCESSOR_FIELD,
    DYNAMIC_INPUT_FIELD,
    DYNAMIC_OUTPUT_FIELD,
    EMITS_FIELD,
    INPUT_FIELD,
    NAME_FIELD,
    PREPROCESSORS_FIELD,
    REFERENCED_COLUMNS_FIELD,
    REFERENCED_SCHEMA_FIELD,
    REFERENCED_TABLE_FIELD,
    RETURNS_FIELD,
    SCHEMA_FIELD,
    SQL_TYPE_FIELD,
    USAGE_FIELD,
)
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
    get_query_result_json,
    get_result_json,
    get_sort_result_json_list,
    list_tools,
    result_sort_func,
    to_dicts,
)
from test.utils.tool_utils import run_tool


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
        ("list_exasol_schemas", ["schemas"]),
        ("list_exasol_tables_and_views", ["tables", "views"]),
        ("list_exasol_custom_functions", ["functions"]),
        ("list_exasol_user_defined_functions", ["scripts"]),
        ("describe_exasol_tables_and_views", ["columns"]),
        ("describe_exasol_custom_functions", ["parameters"]),
        ("describe_exasol_user_defined_functions", ["parameters"]),
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
        result = run_tool(pyexasol_connection, config, "list_exasol_schemas")
        result_json = get_list_result_json(result)
        expected_json = _get_expected_list_json([schema], schema.name, config.schemas)
        if use_like or use_regexp:
            assert result_json == expected_json
        else:
            assert expected_json[0] in result_json


_META_TYPES = ["tables", "views", "functions", "scripts"]


def _enabled_meta_types_config(enabled: list[str], **kwargs) -> McpServerSettings:
    """
    Builds the settings where only the listed object types are enabled.
    """
    config_dict = {
        meta_type: {"enable": meta_type in enabled} for meta_type in _META_TYPES
    }
    return McpServerSettings.model_validate({**config_dict, **kwargs})


@pytest.mark.parametrize(
    "enabled",
    [_META_TYPES, ["tables"], ["views"], ["functions"], ["scripts"], []],
    ids=["all", "tables", "views", "functions", "scripts", "none"],
)
@pytest.mark.parametrize("language", ["", "english"])
def test_find_schemas(
    pyexasol_connection, setup_database, db_schemas, language, enabled
) -> None:
    """
    Tests the `find_schemas` tool with any number of enabled object types,
    including just one or none (see issue #337).
    """
    config = _enabled_meta_types_config(enabled, language=language)
    for schema in db_schemas:
        # Will test on new schemas only, where the result is more reliable.
        if not schema.is_new:
            continue
        result = run_tool(
            pyexasol_connection, config, "find_exasol_schemas", keywords=schema.keywords
        )
        result_json = get_result_json(result)[0]
        expected_json = {NAME_FIELD: schema.name, COMMENT_FIELD: schema.comment}
        assert result_json == expected_json


@pytest.fixture
def empty_schema(pyexasol_connection) -> Generator[ExaSchema, None, None]:
    """
    Creates a schema without any objects in it.
    """
    schema = ExaSchema(
        name="mcp_empty_schema",
        comment="vacant schema with no database objects",
        is_new=True,
        keywords=["mcp_empty_schema", "vacant schema"],
    )
    drop_query = f'DROP SCHEMA IF EXISTS "{schema.name}" CASCADE'
    pyexasol_connection.execute(query=drop_query)
    current_schema = pyexasol_connection.current_schema()
    try:
        try:
            pyexasol_connection.execute(query=f'CREATE SCHEMA "{schema.name}"')
            query = f"COMMENT ON SCHEMA \"{schema.name}\" IS '{schema.comment}'"
            pyexasol_connection.execute(query=query)
        finally:
            # Creating a schema opens it, restore the previously opened one.
            if current_schema:
                pyexasol_connection.execute(f'OPEN SCHEMA "{current_schema}"')
            else:
                pyexasol_connection.execute("CLOSE SCHEMA")
        pyexasol_connection.execute(query="COMMIT")
        yield schema
    finally:
        pyexasol_connection.execute(query=drop_query)
        pyexasol_connection.execute(query="COMMIT")


@pytest.mark.parametrize(
    "enabled",
    [_META_TYPES, ["tables"], []],
    ids=["all", "tables", "none"],
)
def test_find_schemas_empty_schema(
    pyexasol_connection, setup_database, empty_schema, enabled
) -> None:
    """
    Checks that a schema without visible objects of an enabled type can still be
    found by its name and comment.
    """
    config = _enabled_meta_types_config(enabled)
    result = run_tool(
        pyexasol_connection,
        config,
        "find_exasol_schemas",
        keywords=empty_schema.keywords,
    )
    result_json = get_result_json(result)[0]
    expected_json = {
        NAME_FIELD: empty_schema.name,
        COMMENT_FIELD: empty_schema.comment,
    }
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
            "list_exasol_tables_and_views",
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
                "find_exasol_tables_and_views",
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


def test_find_tables_no_match(
    pyexasol_connection,
    setup_database,
    db_schemas,
) -> None:
    """
    Regression test for https://github.com/exasol/mcp-server/issues/281.
    `find_exasol_tables_and_views` used to return every table/view when none of
    the provided keywords matched anything. It should return an empty result
    instead.
    """
    schema = next(schema for schema in db_schemas if schema.is_new)
    config = McpServerSettings(
        tables=MetaListSettings(enable=True),
        views=MetaListSettings(enable=True),
    )
    result = run_tool(
        pyexasol_connection,
        config,
        "find_exasol_tables_and_views",
        keywords=["zzz_no_such_keyword_xyz"],
        schema_name=schema.name,
    )
    # An empty list result has no content blocks (a FastMCP quirk), so read the
    # already-parsed value instead of going through get_result_json().
    assert result.data == []


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
            "list_exasol_custom_functions",
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
                "find_exasol_custom_functions",
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
            "list_exasol_user_defined_functions",
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
                "find_exasol_user_defined_functions",
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
def test_describe_tables(
    pyexasol_connection, setup_database, db_schemas, db_tables, case_sensitive
) -> None:
    """
    Test the `describe_tables` tool, describing every table of a schema in one call.
    """
    config = McpServerSettings(
        columns=MetaSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        result = run_tool(
            pyexasol_connection,
            config,
            "describe_exasol_tables_and_views",
            schema_name=schema.name,
            table_names=[table.name for table in db_tables],
        )
        result_json = get_sort_result_json_list(result)
        expected_json = [
            _get_expected_table_json(table, schema.name) for table in db_tables
        ]
        assert result_json == expected_json


def test_describe_sys_table(pyexasol_connection) -> None:
    """
    Test the `describe_tables` tool, passing the name of a system table to it.
    """
    config = McpServerSettings(columns=MetaSettings(enable=True))
    result = run_tool(
        pyexasol_connection,
        config,
        "describe_exasol_tables_and_views",
        schema_name="SYS",
        table_names=["EXA_ALL_COLUMNS"],
    )
    result_json = get_result_json(result)[0]
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
        columns=MetaSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        result = run_tool(
            pyexasol_connection,
            config,
            "describe_exasol_tables_and_views",
            schema_name=schema.name,
            table_names=[view.name for view in db_views],
        )
        result_json = get_sort_result_json_list(result)
        assert [r[COMMENT_FIELD] for r in result_json] == [v.comment for v in db_views]


@pytest.mark.parametrize(
    ["tool_name", "other_kwargs"],
    [
        ("describe_exasol_tables_and_views", {"table_names": ["ski_resort"]}),
        ("describe_exasol_custom_functions", {"func_names": ["factorial"]}),
        ("describe_exasol_user_defined_functions", {"func_names": ["fibonacci"]}),
    ],
    ids=["describe_tables", "describe_functions", "describe_scripts"],
)
def test_describe_no_schema_name(
    pyexasol_connection, setup_database, tool_name, other_kwargs
) -> None:
    """
    The test validates that the `describe_xxx` tool returns an error if the schema
    is not provided.
    """
    config = McpServerSettings(
        columns=MetaSettings(enable=True),
        parameters=MetaSettings(enable=True),
    )
    with pytest.raises(ToolError):
        run_tool(pyexasol_connection, config, tool_name=tool_name, **other_kwargs)


@pytest.mark.parametrize(
    "tool_name",
    [
        "describe_exasol_tables_and_views",
        "describe_exasol_custom_functions",
        "describe_exasol_user_defined_functions",
    ],
)
def test_describe_no_db_object_name(
    pyexasol_connection, setup_database, db_schemas, tool_name
) -> None:
    """
    The test validates that the `describe_xxx` returns an error if the name of the
    db object to be described is not provided.
    """
    config = McpServerSettings(
        columns=MetaSettings(enable=True),
        parameters=MetaSettings(enable=True),
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
def test_describe_functions(
    pyexasol_connection, setup_database, db_schemas, db_functions, case_sensitive
) -> None:
    """
    Test the `describe_functions` tool, describing every function of a schema
    in one call.
    """
    config = McpServerSettings(
        parameters=MetaSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        result = run_tool(
            pyexasol_connection,
            config,
            "describe_exasol_custom_functions",
            schema_name=schema.name,
            func_names=[func.name for func in db_functions],
        )
        result_json = get_result_json(result)
        expected_json = []
        for func in db_functions:
            func_json = _get_expected_param_json(func, schema.name)
            func_json[USAGE_FIELD] = ""
            expected_json.append(func_json)
        assert result_json == expected_json


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_scripts(
    pyexasol_connection, setup_database, db_schemas, db_scripts, case_sensitive
) -> None:
    """
    Test the `describe_scripts` tool, describing every script of a schema in
    one call.
    """
    config = McpServerSettings(
        parameters=MetaSettings(
            enable=True,
        ),
        case_sensitive=case_sensitive,
    )
    for schema in db_schemas:
        result = run_tool(
            pyexasol_connection,
            config,
            "describe_exasol_user_defined_functions",
            schema_name=schema.name,
            func_names=[script.name for script in db_scripts],
        )
        result_json = get_result_json(result)
        for script, script_json in zip(db_scripts, result_json):
            # The call example message is properly tested in the unit tests.
            # Here we just verify that it exists.
            assert USAGE_FIELD in script_json
            script_json.pop(USAGE_FIELD)
            expected_json = _get_expected_param_json(script, schema.name)
            assert script_json == expected_json


@pytest.mark.parametrize(
    ["tool_name", "name_arg", "name"],
    [
        ("describe_exasol_tables_and_views", "table_names", ["ski_resort"]),
        ("describe_exasol_custom_functions", "func_names", ["factorial"]),
        ("describe_exasol_user_defined_functions", "func_names", ["fibonacci"]),
        ("summarize_exasol_table", "table_name", "ski_resort"),
    ],
    ids=["describe_tables", "describe_functions", "describe_scripts", "summarize"],
)
@pytest.mark.parametrize("case_sensitive", [True, False])
@pytest.mark.parametrize("swap", ["schema", "object"])
def test_retrieval_requires_exact_name(
    pyexasol_connection,
    setup_database,
    db_schema_name,
    tool_name,
    name_arg,
    name,
    case_sensitive,
    swap,
) -> None:
    """
    The tools retrieving a specific object require its exact name, regardless of
    the `case_sensitive` setting. The call with the exact names succeeds, the call
    with the case of one name swapped fails.
    """
    config = McpServerSettings(
        columns=MetaSettings(enable=True),
        parameters=MetaSettings(enable=True),
        enable_summarize_table=True,
        case_sensitive=case_sensitive,
    )
    # The exact names work.
    run_tool(
        pyexasol_connection,
        config,
        tool_name,
        schema_name=db_schema_name,
        **{name_arg: name},
    )
    schema_name = db_schema_name.swapcase() if swap == "schema" else db_schema_name
    if swap == "object":
        name = (
            [n.swapcase() for n in name] if isinstance(name, list) else name.swapcase()
        )
    with pytest.raises(ToolError, match="not found"):
        run_tool(
            pyexasol_connection,
            config,
            tool_name,
            schema_name=schema_name,
            **{name_arg: name},
        )


@pytest.fixture
def case_variant_objects(pyexasol_connection, db_schema_name) -> Generator[str]:
    """
    Creates pairs of tables, functions and scripts whose names differ only in case.
    Each object of a pair has a distinct signature, so the test can tell them apart.
    Returns the schema name.
    """
    schema = db_schema_name
    objects = [
        ("TABLE", "orders", '("lower_id" DECIMAL(9,0) PRIMARY KEY)'),
        ("TABLE", "ORDERS", '("UPPER_ID" DECIMAL(9,0))'),
        (
            "FUNCTION",
            "twice",
            '("lower_x" DECIMAL(9,0)) RETURN DECIMAL(9,0) IS '
            'BEGIN RETURN "lower_x" * 2; END;',
        ),
        (
            "FUNCTION",
            "TWICE",
            '("UPPER_X" DOUBLE) RETURN DOUBLE IS BEGIN RETURN "UPPER_X" * 2; END;',
        ),
        (
            "LUA SCALAR SCRIPT",
            "echo",
            '("lower_s" VARCHAR(10)) RETURNS VARCHAR(10) AS\n'
            "function run(ctx) return ctx.lower_s end",
        ),
        (
            "LUA SCALAR SCRIPT",
            "ECHO",
            '("UPPER_S" DOUBLE) RETURNS DOUBLE AS\n'
            "function run(ctx) return ctx.UPPER_S end",
        ),
    ]
    try:
        for obj_type, name, definition in objects:
            # Functions and scripts must end with a slash on its own line.
            terminator = "" if obj_type == "TABLE" else "\n/\n"
            pyexasol_connection.execute(
                f'CREATE OR REPLACE {obj_type} "{schema}"."{name}" '
                f"{definition}{terminator}"
            )
        yield schema
    finally:
        for obj_type, name, _ in objects:
            drop_type = obj_type.split()[-1]
            pyexasol_connection.execute(
                f'DROP {drop_type} IF EXISTS "{schema}"."{name}"'
            )


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_tables_case_variants(
    pyexasol_connection, case_variant_objects, case_sensitive
) -> None:
    """
    Tables whose names differ only in case are described separately, each with its
    own columns and constraints, regardless of the `case_sensitive` setting.
    """
    schema = case_variant_objects
    config = McpServerSettings(
        columns=MetaSettings(enable=True), case_sensitive=case_sensitive
    )
    result = run_tool(
        pyexasol_connection,
        config,
        "describe_exasol_tables_and_views",
        schema_name=schema,
        table_names=["orders", "ORDERS"],
    )
    result_json = get_result_json(result)
    summary = [
        (
            t[NAME_FIELD],
            [c[NAME_FIELD] for c in t[COLUMNS_FIELD]],
            [c[CONSTRAINT_TYPE_FIELD] for c in t[CONSTRAINTS_FIELD]],
        )
        for t in result_json
    ]
    assert summary == [
        ("orders", ["lower_id"], ["PRIMARY KEY"]),
        ("ORDERS", ["UPPER_ID"], []),
    ]


@pytest.mark.parametrize(
    ["tool_name", "name", "expected_input"],
    [
        ("describe_exasol_custom_functions", "twice", "lower_x"),
        ("describe_exasol_custom_functions", "TWICE", "UPPER_X"),
        ("describe_exasol_user_defined_functions", "echo", "lower_s"),
        ("describe_exasol_user_defined_functions", "ECHO", "UPPER_S"),
    ],
)
@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_functions_case_variants(
    pyexasol_connection,
    case_variant_objects,
    tool_name,
    name,
    expected_input,
    case_sensitive,
) -> None:
    """
    A function or script is described by its exact name, even if another one exists
    whose name differs only in case, regardless of the `case_sensitive` setting.
    """
    config = McpServerSettings(
        parameters=MetaSettings(enable=True), case_sensitive=case_sensitive
    )
    result = run_tool(
        pyexasol_connection,
        config,
        tool_name,
        schema_name=case_variant_objects,
        func_names=[name],
    )
    result_json = get_result_json(result)
    assert [
        (f[NAME_FIELD], [p[NAME_FIELD] for p in f[INPUT_FIELD]]) for f in result_json
    ] == [(name, [expected_input])]


def test_list_preprocessors(
    pyexasol_connection, setup_database, db_preprocessor
) -> None:
    config = McpServerSettings()
    result = run_tool(pyexasol_connection, config, "list_exasol_preprocessors")
    data = get_result_json(result)
    names = [p[NAME_FIELD].upper() for p in data[PREPROCESSORS_FIELD]]
    assert db_preprocessor.name.upper() in names
    assert CURRENT_PREPROCESSOR_FIELD in data


def test_set_preprocessor(
    pyexasol_connection, setup_database, db_schema_name, db_preprocessor
) -> None:
    config = McpServerSettings()
    run_tool(
        pyexasol_connection,
        config,
        "set_exasol_preprocessor",
        schema_name=db_schema_name,
        script_name=db_preprocessor.name,
    )
    list_result = run_tool(pyexasol_connection, config, "list_exasol_preprocessors")
    data = get_result_json(list_result)
    assert data[CURRENT_PREPROCESSOR_FIELD] is not None
    assert db_preprocessor.name.upper() in data[CURRENT_PREPROCESSOR_FIELD].upper()


@pytest.mark.parametrize("query_result_format", ["tabular", "dict"])
def test_summarize_table(
    pyexasol_connection, setup_database, db_schemas, db_tables, query_result_format
) -> None:
    """
    Test the `summarize_exasol_table` tool. Verifies column statistics and sample data
    for the `ski_resort` table (which has known rows).
    """
    config = McpServerSettings(
        enable_summarize_table=True,
        columns=MetaSettings(enable=True),
        query_result_format=query_result_format,
    )
    ski_resort = next(t for t in db_tables if t.name == "ski_resort")

    for schema in db_schemas:
        result = run_tool(
            pyexasol_connection,
            config,
            "summarize_exasol_table",
            schema_name=schema.name,
            table_name=ski_resort.name,
        )
        result_json = get_result_json(result)

        assert result_json[SCHEMA_FIELD] == schema.name
        assert result_json[NAME_FIELD] == ski_resort.name
        assert result_json[COMMENT_FIELD] == ski_resort.comment

        columns_json = result_json[COLUMNS_FIELD]
        assert len(columns_json) == len(ski_resort.columns)

        col_by_name = {c["name"]: c for c in columns_json}

        # Total row count
        assert result_json["row_count"] == 3

        # resort_id is DECIMAL(18,0) — numeric
        resort_id = col_by_name["resort_id"]
        assert resort_id["distinct_count"] == 3
        assert resort_id["min"] is not None
        assert resort_id["max"] is not None
        assert len(resort_id["top_values"]) == 3
        assert resort_id["has_nulls"] is False
        assert resort_id["null_percentage"] == 0

        # resort_name is VARCHAR — not numeric
        resort_name = col_by_name["resort_name"]
        assert resort_name["distinct_count"] == 3
        assert resort_name["min"] is None
        assert resort_name["max"] is None
        assert len(resort_name["top_values"]) == 3
        assert resort_name["has_nulls"] is False
        assert resort_name["null_percentage"] == 0

        # country is VARCHAR — not numeric; France appears twice, Austria once
        country = col_by_name["country"]
        assert country["distinct_count"] == 2
        assert country["min"] is None
        assert len(country["top_values"]) == 2
        assert country["top_values"][0] == "France"  # highest frequency first
        assert country["has_nulls"] is False
        assert country["null_percentage"] == 0

        # altitude is DECIMAL(18,0) — numeric
        altitude = col_by_name["altitude"]
        assert altitude["distinct_count"] == 3
        assert altitude["min"] is not None
        assert altitude["max"] is not None
        assert len(altitude["top_values"]) == 3
        assert altitude["has_nulls"] is False
        assert altitude["null_percentage"] == 0

        # Sample
        sample = to_dicts(result_json["sample"])
        assert len(sample) > 0
        assert len(sample) <= 10


def test_summarize_table_disabled(pyexasol_connection) -> None:
    """
    Test that `summarize_exasol_table` is not registered when disabled.
    """
    config = McpServerSettings()  # enable_summarize_table defaults to False
    result = list_tools(pyexasol_connection, config)
    tool_list = [tool.name for tool in result]
    assert "summarize_exasol_table" not in tool_list


@pytest.mark.parametrize("query_result_format", ["tabular", "dict"])
def test_execute_query(
    pyexasol_connection, setup_database, db_schemas, db_tables, query_result_format
):
    """
    Test the `execute_query` tool. Runs the simplest SELECT query that grabs the entire
    content of a table and validates this content. The tool is tested on each table
    of every schema, against both values of `query_result_format`.
    """
    config = McpServerSettings(
        enable_read_query=True, query_result_format=query_result_format
    )
    for schema in db_schemas:
        for table in db_tables:
            query = f'SELECT * FROM "{schema.name}"."{table.name}"'
            result = run_tool(
                pyexasol_connection,
                config,
                tool_name="execute_exasol_query",
                query=query,
            )
            if result.content:
                result_json = get_query_result_json(result)
            else:
                result_json = []
            expected_json = [
                {col.name: col_value for col, col_value in zip(table.columns, row)}
                for row in table.rows
            ]
            expected_json.sort(key=result_sort_func)
            assert result_json == expected_json


def test_execute_query_rejects_duplicate_columns(
    backend_aware_database_params, setup_database, db_schemas, db_tables
):
    """
    pyexasol itself refuses to return a result set with two identically-named
    columns (e.g. the same column selected twice), regardless of fetch format - see
    `ExaStatement._check_duplicate_col_names`, which runs unconditionally as part of
    statement execution. The tool can only surface this as a sanitized error, not
    work around it.

    Triggering this error makes `DbConnection` discard the connection it used (see
    `db_connection.py`'s handling of `ExaRuntimeError`), so this test opens its own,
    disposable connection instead of using the session-scoped `pyexasol_connection`
    fixture, which other fixtures and tests rely on staying open for the rest of the
    session.
    """
    schema = db_schemas[0]
    table = next(t for t in db_tables if t.columns)
    col_name = table.columns[0].name
    query = f'SELECT "{col_name}", "{col_name}" FROM "{schema.name}"."{table.name}"'
    config = McpServerSettings(enable_read_query=True)
    with pyexasol.connect(**backend_aware_database_params) as connection:
        with pytest.raises(ToolError) as exc_info:
            run_tool(connection, config, tool_name="execute_exasol_query", query=query)
    expected = generic_db_error_message(pyexasol.ExaRuntimeError)
    assert (
        str(exc_info.value) == f"Error calling tool 'execute_exasol_query': {expected}"
    )


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
                    pyexasol_connection,
                    config,
                    tool_name="execute_exasol_query",
                    query=query,
                )


@pytest.mark.parametrize(
    "default_row_limit, row_limit, effective_limit",
    [
        (None, 1, 1),  # row_limit caps the result, no default configured
        (1, None, 1),  # default_row_limit is applied when row_limit is omitted
        (1, 3, 3),  # an explicit row_limit overrides a configured default
    ],
)
@pytest.mark.parametrize("query_result_format", ["tabular", "dict"])
def test_execute_query_with_row_limit(
    pyexasol_connection,
    setup_database,
    db_schemas,
    db_tables,
    query_result_format,
    default_row_limit,
    row_limit,
    effective_limit,
):
    """
    Test that row_limit caps the number of returned rows, that a configured
    default_row_limit is applied when row_limit is omitted, and that an
    explicit row_limit always takes precedence over the default.
    """
    config = McpServerSettings(
        enable_read_query=True,
        default_row_limit=default_row_limit,
        query_result_format=query_result_format,
    )
    for schema in db_schemas:
        for table in db_tables:
            query = f'SELECT * FROM "{schema.name}"."{table.name}"'
            result = run_tool(
                pyexasol_connection,
                config,
                tool_name="execute_exasol_query",
                query=query,
                row_limit=row_limit,
            )
            result_json = get_query_result_json(result) if result.content else []
            expected_rows = min(effective_limit, len(table.rows))
            assert len(result_json) == expected_rows


@pytest.mark.parametrize("query_result_format", ["tabular", "dict"])
def test_profile_query(
    pyexasol_connection, setup_database, db_schemas, db_tables, query_result_format
):
    """
    Test that profile_exasol_query returns a non-empty execution plan for a valid query.
    """
    config = McpServerSettings(
        enable_query_profiling=True, query_result_format=query_result_format
    )
    schema = db_schemas[0]
    table = db_tables[0]
    query = f'SELECT * FROM "{schema.name}"."{table.name}"'
    result = run_tool(
        pyexasol_connection,
        config,
        tool_name="profile_exasol_query",
        query=query,
    )
    result_json = get_query_result_json(result)
    assert len(result_json) > 0
    assert "PART_NAME" in result_json[0]
    assert "DURATION" in result_json[0]


def test_profile_query_error(
    pyexasol_connection, setup_database, db_schemas, db_tables
):
    """
    Test that profile_exasol_query rejects non-SELECT queries.
    """
    config = McpServerSettings(enable_query_profiling=True)
    schema = db_schemas[0]
    table = db_tables[0]
    query = (
        f'SELECT * INTO TABLE "{schema.name}"."ANOTHER_TABLE" '
        f'FROM "{schema.name}"."{table.name}"'
    )
    with pytest.raises(ToolError):
        run_tool(
            pyexasol_connection,
            config,
            tool_name="profile_exasol_query",
            query=query,
        )


_ROWS_123 = (
    'SELECT 1 AS "A" FROM DUAL UNION ALL SELECT 2 FROM DUAL '
    "UNION ALL SELECT 3 FROM DUAL"
)
_ROWS_23 = 'SELECT 2 AS "A" FROM DUAL UNION ALL SELECT 3 FROM DUAL'

_TABLE_OPERATOR_QUERIES = [
    (f"{_ROWS_123} UNION ALL {_ROWS_23}", [1, 2, 2, 3, 3]),
    (f"({_ROWS_123}) UNION ({_ROWS_23})", [1, 2, 3]),
    (f"({_ROWS_123}) INTERSECT ({_ROWS_23})", [2, 3]),
    (f"({_ROWS_123}) MINUS ({_ROWS_23})", [1]),
    (f"({_ROWS_123}) EXCEPT ({_ROWS_23})", [1]),
    (f"({_ROWS_23})", [2, 3]),
    (
        f'WITH "C" AS ({_ROWS_23}) (SELECT * FROM "C") UNION ALL '
        f'(SELECT 1 AS "A" FROM DUAL)',
        [1, 2, 3],
    ),
    (f"({_ROWS_123}) MINUS ({_ROWS_23});", [1]),
]
_TABLE_OPERATOR_IDS = [
    "union-all",
    "union",
    "intersect",
    "minus",
    "except",
    "parenthesised-select",
    "cte-union-all",
    "trailing-semicolon",
]


@pytest.mark.parametrize("row_limit", [None, 2])
@pytest.mark.parametrize(
    ["query", "expected_values"], _TABLE_OPERATOR_QUERIES, ids=_TABLE_OPERATOR_IDS
)
def test_execute_query_table_operators(
    pyexasol_connection, query, expected_values, row_limit
):
    """
    Test that `execute_exasol_query` accepts the SELECTs combined with the table
    operators, with and without the row limit.
    """
    config = McpServerSettings(enable_read_query=True)
    result = run_tool(
        pyexasol_connection,
        config,
        tool_name="execute_exasol_query",
        query=query,
        row_limit=row_limit,
    )
    values = sorted(row["A"] for row in get_query_result_json(result))
    if row_limit is None:
        assert values == expected_values
    else:
        assert len(values) == min(row_limit, len(expected_values))
        assert set(values) <= set(expected_values)


@pytest.fixture
def variadic_emits_udf(pyexasol_connection, db_schema_name) -> Generator[str]:
    """
    Creates a UDF with dynamic output columns, which emits 1, 2, ..., n.
    It's written in Lua, which runs in the database itself, without a script
    language container.
    """
    name = "emit_sequence"
    pyexasol_connection.execute(f"""
CREATE OR REPLACE LUA SCALAR SCRIPT "{db_schema_name}"."{name}"(n DOUBLE) EMITS (...)
AS
function run(ctx)
    for i = 1, ctx.n do
        ctx.emit(i)
    end
end
/
""")
    yield f'"{db_schema_name}"."{name}"'
    pyexasol_connection.execute(f'DROP SCRIPT IF EXISTS "{db_schema_name}"."{name}"')


@pytest.mark.parametrize(
    "default_row_limit, row_limit, expected_count",
    [(None, None, 3), (None, 2, 2), (2, None, 2)],
    ids=["no-row-limit", "row-limit", "default-row-limit"],
)
@pytest.mark.parametrize("query_result_format", ["tabular", "dict"])
@pytest.mark.parametrize("terminator", ["", ";", "; -- comment", " -- comment"])
def test_execute_query_variadic_emits_udf(
    pyexasol_connection,
    variadic_emits_udf,
    query_result_format,
    default_row_limit,
    row_limit,
    expected_count,
    terminator,
):
    """
    Test that a variadic UDF with the EMITS clause can be called with and without
    the row limit, also when the query ends with a semicolon or a comment.
    """
    config = McpServerSettings(
        enable_read_query=True,
        default_row_limit=default_row_limit,
        query_result_format=query_result_format,
    )
    query = f'SELECT {variadic_emits_udf}(3) EMITS ("A" DOUBLE) FROM DUAL{terminator}'
    result = run_tool(
        pyexasol_connection,
        config,
        tool_name="execute_exasol_query",
        query=query,
        row_limit=row_limit,
    )
    values = [row["A"] for row in get_query_result_json(result)]
    assert len(values) == expected_count
    assert set(values) <= {1, 2, 3}


def test_execute_query_keeps_column_names_with_row_limit(pyexasol_connection):
    """
    Test that the row limit doesn't change the names of the result columns, which
    Exasol derives from the expression text for an expression without an alias.
    """
    config = McpServerSettings(enable_read_query=True)
    query = "SELECT NVL(NULL, 1) FROM DUAL"
    results = [
        get_query_result_json(
            run_tool(
                pyexasol_connection,
                config,
                tool_name="execute_exasol_query",
                query=query,
                row_limit=row_limit,
            )
        )
        for row_limit in [None, 5]
    ]
    assert results[0] == results[1]
    assert list(results[0][0]) == ["NVL(NULL,1)"]


def test_execute_query_table_operator_with_select_into(pyexasol_connection):
    """
    Test that `execute_exasol_query` rejects a SELECT INTO in an operand of a table
    operator.
    """
    config = McpServerSettings(enable_read_query=True)
    query = (
        'SELECT 1 AS "A" FROM DUAL UNION ALL '
        'SELECT 2 INTO TABLE "ANOTHER_TABLE" FROM DUAL'
    )
    with pytest.raises(ToolError):
        run_tool(
            pyexasol_connection,
            config,
            tool_name="execute_exasol_query",
            query=query,
        )


def test_profile_query_table_operator(pyexasol_connection):
    """
    Test that `profile_exasol_query` accepts the SELECTs combined with a table
    operator.
    """
    config = McpServerSettings(enable_query_profiling=True)
    result = run_tool(
        pyexasol_connection,
        config,
        tool_name="profile_exasol_query",
        query=f"({_ROWS_123}) MINUS ({_ROWS_23})",
    )
    result_json = get_query_result_json(result)
    assert len(result_json) > 0
    assert "PART_NAME" in result_json[0]
