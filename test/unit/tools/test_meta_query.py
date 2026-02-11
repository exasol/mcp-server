from dataclasses import dataclass
from test.utils.text_utils import collapse_spaces

import pytest

from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
    MetaColumnSettings,
    MetaListSettings,
)
from exasol.ai.mcp.server.tools.meta_query import (
    INFO_COLUMN,
    ExasolMetaQuery,
    MetaType,
    SysInfoType,
)


def _column_config(case_sensitive: bool) -> McpServerSettings:
    return McpServerSettings(
        columns=MetaColumnSettings(
            enable=True,
            name_field="name",
            comment_field="comment",
            type_field="type",
            constraint_name_field="constraint_name",
            constraint_type_field="constraint_type",
            constraint_columns_field="constraint_columns",
            referenced_schema_field="referenced_schema",
            referenced_table_field="referenced_table",
            referenced_columns_field="referenced_columns",
            columns_field="columns",
            constraints_field="constraints",
            table_comment_field="table_comment",
        ),
        case_sensitive=case_sensitive,
    )


def _column_predicate(column: str, value: str, case_sensitive: bool) -> str:
    if case_sensitive:
        return f""""{column}" = '{value}'"""
    return f"""UPPER("{column}") = '{value.upper()}'"""


@dataclass
class MetaParams:
    schema_name: str = ""
    schema_pattern: str = ""
    schema_pattern_type: str = ""
    obj_name_pattern: str = ""
    obj_name_pattern_type: str = ""
    obj2_name_pattern: str = ""
    obj2_name_pattern_type: str = ""
    expected_where_clause: str = ""
    case_sensitive: bool = False

    @staticmethod
    def _meta_settings(pattern: str, pattern_type: str) -> MetaListSettings:
        return MetaListSettings(
            enable=True,
            like_pattern=pattern if pattern_type == "LIKE" else "",
            regexp_pattern=pattern if pattern_type == "REGEXP_LIKE" else "",
            name_field="name",
            comment_field="comment",
            schema_field="schema",
        )

    @property
    def schema_settings(self):
        return self._meta_settings(self.schema_pattern, self.schema_pattern_type)

    @property
    def db_obj_settings(self):
        return self._meta_settings(self.obj_name_pattern, self.obj_name_pattern_type)

    @property
    def db_obj2_settings(self):
        return self._meta_settings(self.obj2_name_pattern, self.obj2_name_pattern_type)

    @staticmethod
    def _db_obj_based_where_clause(column: str, pattern: str, pattern_type: str) -> str:
        if pattern_type == "LIKE":
            return f"""WHERE "{column}" LIKE '{pattern}'"""
        elif pattern_type == "REGEXP_LIKE":
            return f"""WHERE REGEXP_INSTR("{column}", '{pattern}') <> 0"""
        return ""

    @property
    def schema_based_where_clause(self):
        return self._db_obj_based_where_clause(
            "SCHEMA_NAME", self.schema_pattern, self.schema_pattern_type
        )

    def db_obj_based_where_clause(self, meta_name: str) -> str:
        return self._db_obj_based_where_clause(
            f"{meta_name}_NAME", self.obj_name_pattern, self.obj_name_pattern_type
        )

    def db_obj2_based_where_clause(self, meta_name: str) -> str:
        return self._db_obj_based_where_clause(
            f"{meta_name}_NAME", self.obj2_name_pattern, self.obj2_name_pattern_type
        )

    @property
    def column_based_where_clause(self) -> str:
        if self.schema_name:
            return f'WHERE {_column_predicate("COLUMN_SCHEMA", self.schema_name, self.case_sensitive)}'
        elif self.schema_pattern:
            return self._db_obj_based_where_clause(
                "COLUMN_SCHEMA", self.schema_pattern, self.schema_pattern_type
            )
        return ""


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(
            schema_name="exa_toolbox",
            expected_where_clause="""WHERE UPPER("TABLE_SCHEMA") = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            schema_name="exa_toolbox",
            case_sensitive=True,
            expected_where_clause="""WHERE "TABLE_SCHEMA" = 'exa_toolbox'""",
        ),
        MetaParams(
            obj_name_pattern="PUB",
            obj_name_pattern_type="REGEXP_LIKE",
            expected_where_clause="""WHERE REGEXP_INSTR("TABLE_NAME", 'PUB') <> 0""",
        ),
        MetaParams(
            schema_name="exa_toolbox",
            schema_pattern="EXA%",
            schema_pattern_type="LIKE",
            obj_name_pattern="PUB%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE "TABLE_NAME" LIKE 'PUB%' AND UPPER("TABLE_SCHEMA") = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            schema_pattern="EXA",
            schema_pattern_type="REGEXP_LIKE",
            obj_name_pattern="PUB%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE "TABLE_NAME" LIKE 'PUB%' AND REGEXP_INSTR("TABLE_SCHEMA", 'EXA') <> 0""",
        ),
    ],
    ids=[
        "all-tables",
        "exact-schema",
        "exact-schema-case-sensitive",
        "table-pattern",
        "exact-schema-table-pattern",
        "schema-and-table-patterns",
    ],
)
def test_get_metadata(meta_params):
    config = McpServerSettings(
        schemas=meta_params.schema_settings,
        tables=meta_params.db_obj_settings,
        case_sensitive=meta_params.case_sensitive,
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(
        meta_query.get_metadata(MetaType.TABLE, meta_params.schema_name)
    )
    expected_query = collapse_spaces(
        f"""
        SELECT
            "TABLE_NAME" AS "name",
            "TABLE_COMMENT" AS "comment",
            "TABLE_SCHEMA" AS "schema"
        FROM SYS.EXA_ALL_TABLES
        {meta_params.expected_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(expected_where_clause="""WHERE "SCRIPT_TYPE" = 'UDF'"""),
        MetaParams(
            schema_name="exa_toolbox",
            expected_where_clause="""WHERE "SCRIPT_TYPE" = 'UDF' AND UPPER("SCRIPT_SCHEMA") = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            schema_name="exa_toolbox",
            case_sensitive=True,
            expected_where_clause="""WHERE "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" = 'exa_toolbox'""",
        ),
        MetaParams(
            obj_name_pattern="BUCKETFS%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE "SCRIPT_NAME" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF'""",
        ),
        MetaParams(
            schema_name="exa_toolbox",
            schema_pattern="EXA%",
            schema_pattern_type="LIKE",
            obj_name_pattern="BUCKETFS%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE "SCRIPT_NAME" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF' AND UPPER("SCRIPT_SCHEMA") = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            schema_pattern="EXA%",
            schema_pattern_type="LIKE",
            obj_name_pattern="BUCKETFS%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE "SCRIPT_NAME" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" LIKE 'EXA%'""",
        ),
    ],
    ids=[
        "all-tables",
        "exact-schema",
        "exact-schema-case-sensitive",
        "table-pattern",
        "exact-schema-table-pattern",
        "schema-and-table-patterns",
    ],
)
def test_get_script_metadata(meta_params):
    config = McpServerSettings(
        schemas=meta_params.schema_settings,
        scripts=meta_params.db_obj_settings,
        case_sensitive=meta_params.case_sensitive,
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(
        meta_query.get_metadata(MetaType.SCRIPT, meta_params.schema_name)
    )
    expected_query = collapse_spaces(
        f"""
        SELECT
            "SCRIPT_NAME" AS "name",
            "SCRIPT_COMMENT" AS "comment",
            "SCRIPT_SCHEMA" AS "schema"
        FROM SYS.EXA_ALL_SCRIPTS
        {meta_params.expected_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(schema_pattern="EXASOL%", schema_pattern_type="LIKE"),
        MetaParams(schema_pattern="EXASOL", schema_pattern_type="REGEXP_LIKE"),
    ],
    ids=["no-pattern", "like", "regexp"],
)
def test_get_schema_metadata(meta_params):
    config = McpServerSettings(schemas=meta_params.schema_settings)
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.get_metadata(MetaType.SCHEMA, "to be ignored"))
    expected_query = collapse_spaces(
        f"""
        SELECT
            "SCHEMA_NAME" AS "name",
            "SCHEMA_COMMENT" AS "comment"
        FROM SYS.EXA_ALL_SCHEMAS
        {meta_params.schema_based_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_get_object_metadata(case_sensitive) -> None:
    config = McpServerSettings(case_sensitive=case_sensitive)
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(
        meta_query.get_object_metadata(MetaType.FUNCTION, "my_schema", "my_table")
    )
    expected_query = collapse_spaces(
        f"""
        SELECT * FROM SYS.EXA_ALL_FUNCTIONS
        WHERE
            {_column_predicate("FUNCTION_SCHEMA", 'my_schema', case_sensitive)} AND
            {_column_predicate("FUNCTION_NAME", 'my_table', case_sensitive)}
        """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(schema_pattern="EXASOL%", schema_pattern_type="LIKE"),
        MetaParams(schema_pattern="EXASOL", schema_pattern_type="REGEXP_LIKE"),
    ],
    ids=["no-pattern", "like", "regexp"],
)
def test_find_schemas(meta_params) -> None:
    config = McpServerSettings(
        schemas=meta_params.schema_settings,
        tables=MetaListSettings(enable=True),
        views=MetaListSettings(enable=True),
        functions=MetaListSettings(enable=True),
        scripts=MetaListSettings(enable=True),
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_schemas())
    expected_query = collapse_spaces(
        f"""
        SELECT
            "S"."SCHEMA_NAME" AS "name",
            "S"."SCHEMA_COMMENT" AS "comment",
            "O"."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_SCHEMAS AS "S"
        JOIN
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
            FROM SYS.EXA_ALL_TABLES
            UNION
            SELECT
                "VIEW_SCHEMA" AS "SCHEMA",
                CONCAT(
                    '{{"VIEW": "', "VIEW_NAME",
                    NVL2("VIEW_COMMENT", CONCAT('", "COMMENT": "', "VIEW_COMMENT"), ''),
                    '"}}'
                ) AS "OBJ_INFO"
            FROM SYS.EXA_ALL_VIEWS
            UNION
            SELECT
                "FUNCTION_SCHEMA" AS "SCHEMA",
                CONCAT(
                    '{{"FUNCTION": "', "FUNCTION_NAME",
                    NVL2("FUNCTION_COMMENT", CONCAT('", "COMMENT": "', "FUNCTION_COMMENT"), ''),
                    '"}}'
                ) AS "OBJ_INFO"
            FROM SYS.EXA_ALL_FUNCTIONS
            UNION
            SELECT
                "SCRIPT_SCHEMA" AS "SCHEMA",
                CONCAT(
                    '{{"SCRIPT": "', "SCRIPT_NAME",
                    NVL2("SCRIPT_COMMENT", CONCAT('", "COMMENT": "', "SCRIPT_COMMENT"), ''),
                    '"}}'
                ) AS "OBJ_INFO"
            FROM SYS.EXA_ALL_SCRIPTS
            WHERE "SCRIPT_TYPE" = 'UDF'
            )
        GROUP BY "SCHEMA"
        AS "O" ON "S"."SCHEMA_NAME" = "O"."SCHEMA"
        {meta_params.schema_based_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(
            schema_name="exa_toolbox",
            schema_pattern="EXA",
            schema_pattern_type="REGEXP_LIKE",
        ),
        MetaParams(schema_name="exa_toolbox", case_sensitive=True),
        MetaParams(schema_pattern="EXA", schema_pattern_type="REGEXP_LIKE"),
        MetaParams(obj_name_pattern="PUB", obj_name_pattern_type="REGEXP_LIKE"),
        MetaParams(
            schema_pattern="EXA",
            schema_pattern_type="REGEXP_LIKE",
            obj_name_pattern="PUB",
            obj_name_pattern_type="REGEXP_LIKE",
        ),
    ],
    ids=[
        "no-predicates",
        "exact-schema",
        "exact-schema-case-sensitive",
        "schema-pattern",
        "table-pattern",
        "all-patterns",
    ],
)
def test_find_tables(meta_params) -> None:
    config = McpServerSettings(
        schemas=meta_params.schema_settings,
        tables=meta_params.db_obj_settings,
        views=MetaListSettings(enable=False),
        case_sensitive=meta_params.case_sensitive,
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_tables(meta_params.schema_name))
    expected_query = collapse_spaces(
        f"""
        WITH "C" AS (
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
                FROM SYS.EXA_ALL_COLUMNS
                {meta_params.column_based_where_clause}
            )
            GROUP BY "SCHEMA", "TABLE"
        )
        SELECT
            "T"."TABLE_NAME" AS "name",
            "T"."TABLE_COMMENT" AS "comment",
            "T"."TABLE_SCHEMA" AS "schema",
            "C"."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_TABLES AS "T"
        JOIN "C" ON
            "T"."TABLE_SCHEMA" = "C"."SCHEMA" AND
            "T"."TABLE_NAME" = "C"."TABLE"
        {meta_params.db_obj_based_where_clause('TABLE')}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(
            schema_name="exa_toolbox", schema_pattern="EXA%", schema_pattern_type="LIKE"
        ),
        MetaParams(schema_name="exa_toolbox", case_sensitive=True),
        MetaParams(schema_pattern="EXA%", schema_pattern_type="LIKE"),
        MetaParams(obj_name_pattern="PUB%", obj_name_pattern_type="LIKE"),
        MetaParams(obj2_name_pattern="AUDITING%", obj2_name_pattern_type="LIKE"),
        MetaParams(
            schema_pattern="EXA%",
            schema_pattern_type="LIKE",
            obj_name_pattern="PUB%",
            obj_name_pattern_type="LIKE",
            obj2_name_pattern="AUDITING%",
            obj2_name_pattern_type="LIKE",
        ),
    ],
    ids=[
        "no-predicates",
        "exact-schema",
        "exact-schema-case-sensitive",
        "schema-pattern",
        "table-pattern",
        "view-pattern",
        "all-patterns",
    ],
)
def test_find_tables_and_views(meta_params) -> None:
    config = McpServerSettings(
        schemas=meta_params.schema_settings,
        tables=meta_params.db_obj_settings,
        views=meta_params.db_obj2_settings,
        case_sensitive=meta_params.case_sensitive,
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_tables(meta_params.schema_name))
    expected_query = collapse_spaces(
        f"""
        WITH "C" AS (
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
                FROM SYS.EXA_ALL_COLUMNS
                {meta_params.column_based_where_clause}
            )
            GROUP BY "SCHEMA", "TABLE"
        )
        SELECT
            "T"."TABLE_NAME" AS "name",
            "T"."TABLE_COMMENT" AS "comment",
            "T"."TABLE_SCHEMA" AS "schema",
            "C"."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_TABLES AS "T"
        JOIN "C" ON
            "T"."TABLE_SCHEMA" = "C"."SCHEMA" AND
            "T"."TABLE_NAME" = "C"."TABLE"
        {meta_params.db_obj_based_where_clause('TABLE')}
        UNION
        SELECT
            "T"."VIEW_NAME" AS "name",
            "T"."VIEW_COMMENT" AS "comment",
            "T"."VIEW_SCHEMA" AS "schema",
            "C"."{INFO_COLUMN}"
        FROM SYS.EXA_ALL_VIEWS AS "T"
        JOIN "C" ON
            "T"."VIEW_SCHEMA" = "C"."SCHEMA" AND
            "T"."VIEW_NAME" = "C"."TABLE"
        {meta_params.db_obj2_based_where_clause('VIEW')}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_columns(case_sensitive) -> None:
    meta_query = ExasolMetaQuery(_column_config(case_sensitive))
    query = collapse_spaces(meta_query.describe_columns("my'_schema", "my'_table"))
    expected_query = collapse_spaces(
        f"""
        SELECT
            "COLUMN_NAME" AS "name",
            "COLUMN_TYPE" AS "type",
            "COLUMN_COMMENT" AS "comment"
        FROM SYS.EXA_ALL_COLUMNS
        WHERE
            {_column_predicate("COLUMN_SCHEMA", "my''_schema", case_sensitive)} AND
            {_column_predicate("COLUMN_TABLE", "my''_table", case_sensitive)}
        """
    )
    assert query == expected_query


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_describe_constraints(case_sensitive) -> None:
    meta_query = ExasolMetaQuery(_column_config(case_sensitive))
    query = collapse_spaces(meta_query.describe_constraints("my_schema", "my_table"))
    expected_query = collapse_spaces(
        f"""
        SELECT
            FIRST_VALUE("CONSTRAINT_TYPE") AS "constraint_type",
            CASE LEFT("CONSTRAINT_NAME", 4) WHEN 'SYS_' THEN NULL
                ELSE "CONSTRAINT_NAME" END AS "constraint_name",
            GROUP_CONCAT(DISTINCT "COLUMN_NAME" ORDER BY "ORDINAL_POSITION")
                AS "constraint_columns",
            FIRST_VALUE("REFERENCED_SCHEMA") AS "referenced_schema",
            FIRST_VALUE("REFERENCED_TABLE") AS "referenced_table",
            GROUP_CONCAT(DISTINCT "REFERENCED_COLUMN" ORDER BY "ORDINAL_POSITION")
                AS "referenced_columns"
        FROM SYS.EXA_ALL_CONSTRAINT_COLUMNS
        WHERE
            {_column_predicate("CONSTRAINT_SCHEMA", "my_schema", case_sensitive)} AND
            {_column_predicate("CONSTRAINT_TABLE", "my_table", case_sensitive)}
        GROUP BY "CONSTRAINT_NAME"
        """
    )
    assert query == expected_query


@pytest.mark.parametrize("case_sensitive", [True, False])
def test_get_table_comment(case_sensitive) -> None:
    meta_query = ExasolMetaQuery(McpServerSettings(case_sensitive=case_sensitive))
    query = collapse_spaces(meta_query.get_table_comment("my_schema", "my_table"))
    expected_query = collapse_spaces(
        f"""
        SELECT "TABLE_COMMENT" AS "COMMENT" FROM SYS.EXA_ALL_TABLES
        WHERE
            {_column_predicate("TABLE_SCHEMA", "my_schema", case_sensitive)} AND
            {_column_predicate("TABLE_NAME", "my_table", case_sensitive)}
        UNION
        SELECT "VIEW_COMMENT" AS "COMMENT" FROM SYS.EXA_ALL_VIEWS
        WHERE
            {_column_predicate("VIEW_SCHEMA", "my_schema", case_sensitive)} AND
            {_column_predicate("VIEW_NAME", "my_table", case_sensitive)}
        LIMIT 1
        """
    )
    assert query == expected_query


def test_get_sql_types() -> None:
    query = collapse_spaces(ExasolMetaQuery.get_sql_types())
    expected_query = collapse_spaces(
        'SELECT "TYPE_NAME", "CREATE_PARAMS", "PRECISION" FROM SYS.EXA_SQL_TYPES'
    )
    assert query == expected_query


@pytest.mark.parametrize("info_type", [SysInfoType.SYSTEM, SysInfoType.STATISTICS])
def test_get_system_table_list(info_type) -> None:
    config = McpServerSettings()
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.get_system_tables(info_type))
    expected_query = collapse_spaces(
        f"""
        SELECT
            "SCHEMA_NAME" AS "{config.tables.schema_field}",
            "OBJECT_NAME" AS "{config.tables.name_field}",
            "OBJECT_COMMENT" AS "{config.tables.comment_field}"
        FROM SYS.EXA_SYSCAT
        WHERE UPPER("SCHEMA_NAME") = '{info_type.value.upper()}'
    """
    )
    assert query == expected_query


@pytest.mark.parametrize("info_type", [SysInfoType.SYSTEM, SysInfoType.STATISTICS])
def test_get_system_table_details(info_type) -> None:
    config = McpServerSettings()
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.get_system_tables(info_type, "the_table"))
    expected_query = collapse_spaces(
        f"""
        SELECT
            "SCHEMA_NAME" AS "{config.tables.schema_field}",
            "OBJECT_NAME" AS "{config.tables.name_field}",
            "OBJECT_COMMENT" AS "{config.tables.comment_field}"
        FROM SYS.EXA_SYSCAT
        WHERE UPPER("SCHEMA_NAME") = '{info_type.value.upper()}'
        AND UPPER("OBJECT_NAME") = 'THE_TABLE'
    """
    )
    assert query == expected_query


@pytest.mark.parametrize("reserved", [True, False])
def test_get_keywords(reserved) -> None:
    query = collapse_spaces(ExasolMetaQuery.get_keywords(reserved, "a"))
    expected_query = collapse_spaces(
        'SELECT "KEYWORD" FROM SYS.EXA_SQL_KEYWORDS '
        f"""WHERE "RESERVED" = {str(reserved).upper()} AND LEFT("KEYWORD", 1) = 'A' """
        'ORDER BY "KEYWORD"'
    )
    assert query == expected_query
