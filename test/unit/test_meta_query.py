from test.utils.text_utils import collapse_spaces

import pytest

from exasol.ai.mcp.server.meta_query import (
    INFO_COLUMN,
    ExasolMetaQuery,
    MetaType,
)
from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaListSettings,
)


@pytest.mark.parametrize(
    [
        "schema_name",
        "schema_pattern",
        "schema_pattern_type",
        "table_pattern",
        "table_pattern_type",
        "expected_where_clause",
    ],
    [
        ("", "", "", "", "", ""),
        ("EXA_TOOLBOX", "", "", "", "", """WHERE "TABLE_SCHEMA" = 'EXA_TOOLBOX'"""),
        ("", "", "", "PUB", "REGEXP_LIKE", """WHERE local."name" REGEXP_LIKE 'PUB'"""),
        (
            "EXA_TOOLBOX",
            "EXA%",
            "LIKE",
            "PUB%",
            "LIKE",
            """WHERE local."name" LIKE 'PUB%' AND "TABLE_SCHEMA" = 'EXA_TOOLBOX'""",
        ),
        (
            "",
            "EXA",
            "REGEXP_LIKE",
            "PUB%",
            "LIKE",
            """WHERE local."name" LIKE 'PUB%' AND "TABLE_SCHEMA" REGEXP_LIKE 'EXA'""",
        ),
    ],
    ids=[
        "all-tables",
        "exact-schema",
        "table-pattern",
        "exact-schema-table-pattern",
        "schema-and-table-patterns",
    ],
)
def test_get_metadata(
    schema_name,
    schema_pattern,
    schema_pattern_type,
    table_pattern,
    table_pattern_type,
    expected_where_clause,
):
    config = McpServerSettings(
        schemas=MetaListSettings(
            like_pattern=schema_pattern if schema_pattern_type == "LIKE" else "",
            regexp_pattern=(
                schema_pattern if schema_pattern_type == "REGEXP_LIKE" else ""
            ),
        ),
        tables=MetaListSettings(
            enable=True,
            like_pattern=table_pattern if table_pattern_type == "LIKE" else "",
            regexp_pattern=table_pattern if table_pattern_type == "REGEXP_LIKE" else "",
            name_field="name",
            comment_field="comment",
            schema_field="schema",
        ),
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.get_metadata(MetaType.TABLE, schema_name))
    expected_query = collapse_spaces(
        f"""
        SELECT
            "TABLE_NAME" AS "name",
            "TABLE_COMMENT" AS "comment",
            "TABLE_SCHEMA" AS "schema"
        FROM SYS."EXA_ALL_TABLES"
        {expected_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    ["schema_name", "schema_pattern", "script_pattern", "expected_where_clause"],
    [
        ("", "", "", """WHERE "SCRIPT_TYPE" = 'UDF'"""),
        (
            "EXA_TOOLBOX",
            "",
            "",
            """WHERE "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" = 'EXA_TOOLBOX'""",
        ),
        (
            "",
            "",
            "BUCKETFS%",
            """WHERE local."name" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF'""",
        ),
        (
            "EXA_TOOLBOX",
            "EXA%",
            "BUCKETFS%",
            """WHERE local."name" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" = 'EXA_TOOLBOX'""",
        ),
        (
            "",
            "EXA%",
            "BUCKETFS%",
            """WHERE local."name" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" LIKE 'EXA%'""",
        ),
    ],
    ids=[
        "all-tables",
        "exact-schema",
        "table-pattern",
        "exact-schema-table-pattern",
        "schema-and-table-patterns",
    ],
)
def test_get_script_metadata(
    schema_name, schema_pattern, script_pattern, expected_where_clause
):
    config = McpServerSettings(
        schemas=MetaListSettings(like_pattern=schema_pattern),
        scripts=MetaListSettings(
            enable=True,
            like_pattern=script_pattern,
            name_field="name",
            comment_field="comment",
            schema_field="schema",
        ),
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.get_metadata(MetaType.SCRIPT, schema_name))
    expected_query = collapse_spaces(
        f"""
        SELECT
            "SCRIPT_NAME" AS "name",
            "SCRIPT_COMMENT" AS "comment",
            "SCRIPT_SCHEMA" AS "schema"
        FROM SYS."EXA_ALL_SCRIPTS"
        {expected_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    ["pattern", "pattern_type"],
    [("", ""), ("EXASOL%", "LIKE"), ("EXASOL", "REGEXP_LIKE")],
    ids=["no-pattern", "like", "regexp"],
)
def test_get_schema_metadata(pattern, pattern_type):
    config = McpServerSettings(
        schemas=MetaListSettings(
            enable=True,
            like_pattern=pattern if pattern_type == "LIKE" else "",
            regexp_pattern=pattern if pattern_type == "REGEXP_LIKE" else "",
            name_field="name",
            comment_field="comment",
        )
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.get_metadata(MetaType.SCHEMA, "to be ignored"))
    expected_where_clause = (
        f"""WHERE local."name" {pattern_type} '{pattern}'""" if pattern else ""
    )
    expected_query = collapse_spaces(
        f"""
        SELECT
            "SCHEMA_NAME" AS "name",
            "SCHEMA_COMMENT" AS "comment"
        FROM SYS."EXA_ALL_SCHEMAS"
        {expected_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    ["pattern", "pattern_type"],
    [("", ""), ("EXASOL%", "LIKE"), ("EXASOL", "REGEXP_LIKE")],
    ids=["no-pattern", "like", "regexp"],
)
def test_find_schemas(pattern, pattern_type) -> None:
    config = McpServerSettings(
        schemas=MetaListSettings(
            enable=True,
            like_pattern=pattern if pattern_type == "LIKE" else "",
            regexp_pattern=pattern if pattern_type == "REGEXP_LIKE" else "",
            name_field="name",
            comment_field="comment",
        )
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_schemas())
    expected_where_clause = (
        f"""WHERE local."name" {pattern_type} '{pattern}'""" if pattern else ""
    )
    expected_query = collapse_spaces(
        f"""
        SELECT
            S."SCHEMA_NAME" AS "name",
            S."SCHEMA_COMMENT" AS "comment",
            O."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_SCHEMAS S
        JOIN (
            SELECT
                "SCHEMA",
                CONCAT('[', GROUP_CONCAT(DISTINCT "OBJ_INFO" SEPARATOR ', '), ']') AS "{INFO_COLUMN}"
            FROM (
                SELECT
                    "TABLE_SCHEMA" AS "SCHEMA",
                    CONCAT(
                        '{{"TABLE": "', "TABLE_NAME",
                        NVL2("TABLE_COMMENT", CONCAT('", "COMMENT": "', "TABLE_COMMENT"), ''),
                        '"}}'
                    ) AS "OBJ_INFO"
                FROM SYS."EXA_ALL_TABLES"
                UNION
                SELECT
                    "VIEW_SCHEMA" AS "SCHEMA",
                    CONCAT(
                        '{{"VIEW": "', "VIEW_NAME",
                        NVL2("VIEW_COMMENT", CONCAT('", "COMMENT": "', "VIEW_COMMENT"), ''),
                        '"}}'
                    ) AS "OBJ_INFO"
                FROM SYS."EXA_ALL_VIEWS"
                UNION
                SELECT
                    "FUNCTION_SCHEMA" AS "SCHEMA",
                    CONCAT(
                        '{{"FUNCTION": "', "FUNCTION_NAME",
                        NVL2("FUNCTION_COMMENT", CONCAT('", "COMMENT": "', "FUNCTION_COMMENT"), ''),
                        '"}}'
                    ) AS "OBJ_INFO"
                FROM SYS."EXA_ALL_FUNCTIONS"
                UNION
                SELECT
                    "SCRIPT_SCHEMA" AS "SCHEMA",
                    CONCAT(
                        '{{"SCRIPT": "', "SCRIPT_NAME",
                        NVL2("SCRIPT_COMMENT", CONCAT('", "COMMENT": "', "SCRIPT_COMMENT"), ''),
                        '"}}'
                    ) AS "OBJ_INFO"
                FROM SYS."EXA_ALL_SCRIPTS"
                WHERE "SCRIPT_TYPE"='UDF'
                )
            GROUP BY "SCHEMA"
        ) O ON S."SCHEMA_NAME" = O."SCHEMA"
        {expected_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    ["schema_name", "schema_regex_pattern", "table_regex_pattern"],
    [
        ("", "", ""),
        ("EXA_TOOLBOX", "EXA", ""),
        ("", "EXA", ""),
        ("", "", "PUB"),
        ("", "EXA%", "PUB%"),
    ],
    ids=[
        "no-predicates",
        "exact-schema",
        "schema-pattern",
        "table-pattern",
        "all-patterns",
    ],
)
def test_find_tables(schema_name, schema_regex_pattern, table_regex_pattern) -> None:
    config = McpServerSettings(
        schemas=MetaListSettings(regexp_pattern=schema_regex_pattern),
        tables=MetaListSettings(
            enable=True,
            regexp_pattern=table_regex_pattern,
            name_field="name",
            comment_field="comment",
        ),
        views=MetaListSettings(enable=False),
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_tables(schema_name))
    if schema_name:
        expected_column_where_clause = f"""WHERE "COLUMN_SCHEMA" = '{schema_name}'"""
    elif schema_regex_pattern:
        expected_column_where_clause = (
            f"""WHERE "COLUMN_SCHEMA" REGEXP_LIKE '{schema_regex_pattern}'"""
        )
    else:
        expected_column_where_clause = ""
    expected_table_where_clause = (
        f"""WHERE local."name" REGEXP_LIKE '{table_regex_pattern}'"""
        if table_regex_pattern
        else ""
    )
    expected_query = collapse_spaces(
        f"""
        WITH C AS (
            SELECT
                "SCHEMA",
                "TABLE",
                CONCAT('[', GROUP_CONCAT(DISTINCT "OBJ_INFO" SEPARATOR ', '), ']') AS "{INFO_COLUMN}"
            FROM (
                SELECT
                    "COLUMN_SCHEMA" AS "SCHEMA", "COLUMN_TABLE" AS "TABLE",
                    CONCAT(
                        '{{"COLUMN": "', "COLUMN_NAME",
                        NVL2("COLUMN_COMMENT", CONCAT('", "COMMENT": "', "COLUMN_COMMENT"), ''),
                        '"}}'
                    ) AS "OBJ_INFO"
                FROM SYS."EXA_ALL_COLUMNS"
                {expected_column_where_clause}
            )
            GROUP BY "SCHEMA", "TABLE"
        )
        SELECT
            T."TABLE_NAME" AS "name",
            T."TABLE_COMMENT" AS "comment",
            T."TABLE_SCHEMA" AS "schema",
            C."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_TABLES T
        JOIN C ON
            T."TABLE_SCHEMA" = C."SCHEMA" AND
            T."TABLE_NAME" = C."TABLE"
        {expected_table_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    ["schema_name", "schema_like_pattern", "table_like_pattern", "view_like_pattern"],
    [
        ("", "", "", ""),
        ("EXA_TOOLBOX", "EXA%", "", ""),
        ("", "EXA%", "", ""),
        ("", "", "PUB%", ""),
        ("", "", "", "AUDITING%"),
        ("", "EXA%", "PUB%", "AUDITING%"),
    ],
    ids=[
        "no-predicates",
        "exact-schema",
        "schema-pattern",
        "table-pattern",
        "view-pattern",
        "all-patterns",
    ],
)
def test_find_tables_and_views(
    schema_name, schema_like_pattern, table_like_pattern, view_like_pattern
) -> None:
    config = McpServerSettings(
        schemas=MetaListSettings(like_pattern=schema_like_pattern),
        tables=MetaListSettings(
            enable=True,
            like_pattern=table_like_pattern,
            name_field="name",
            comment_field="comment",
        ),
        views=MetaListSettings(
            enable=True,
            like_pattern=view_like_pattern,
            name_field="name",
            comment_field="comment",
        ),
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_tables(schema_name))
    if schema_name:
        expected_column_where_clause = f"""WHERE "COLUMN_SCHEMA" = '{schema_name}'"""
    elif schema_like_pattern:
        expected_column_where_clause = (
            f"""WHERE "COLUMN_SCHEMA" LIKE '{schema_like_pattern}'"""
        )
    else:
        expected_column_where_clause = ""
    expected_table_where_clause = (
        f"""WHERE local."name" LIKE '{table_like_pattern}'"""
        if table_like_pattern
        else ""
    )
    expected_view_where_clause = (
        f"""WHERE local."name" LIKE '{view_like_pattern}'"""
        if view_like_pattern
        else ""
    )
    expected_query = collapse_spaces(
        f"""
        WITH C AS (
            SELECT
                "SCHEMA",
                "TABLE",
                CONCAT('[', GROUP_CONCAT(DISTINCT "OBJ_INFO" SEPARATOR ', '), ']') AS "{INFO_COLUMN}"
            FROM (
                SELECT
                    "COLUMN_SCHEMA" AS "SCHEMA", "COLUMN_TABLE" AS "TABLE",
                    CONCAT(
                        '{{"COLUMN": "', "COLUMN_NAME",
                        NVL2("COLUMN_COMMENT", CONCAT('", "COMMENT": "', "COLUMN_COMMENT"), ''),
                        '"}}'
                    ) AS "OBJ_INFO"
                FROM SYS."EXA_ALL_COLUMNS"
                {expected_column_where_clause}
            )
            GROUP BY "SCHEMA", "TABLE"
        )
        SELECT
            T."TABLE_NAME" AS "name",
            T."TABLE_COMMENT" AS "comment",
            T."TABLE_SCHEMA" AS "schema",
            C."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_TABLES T
        JOIN C ON
            T."TABLE_SCHEMA" = C."SCHEMA" AND
            T."TABLE_NAME" = C."TABLE"
        {expected_table_where_clause}
        UNION
        SELECT
            T."VIEW_NAME" AS "name",
            T."VIEW_COMMENT" AS "comment",
            T."VIEW_SCHEMA" AS "schema",
            C."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_VIEWS T
        JOIN C ON
            T."VIEW_SCHEMA" = C."SCHEMA" AND
            T."VIEW_NAME" = C."TABLE"
        {expected_view_where_clause}
    """
    )
    assert query == expected_query
