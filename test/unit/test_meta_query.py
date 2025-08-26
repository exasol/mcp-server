from dataclasses import dataclass
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

    @property
    def schema_based_where_clause(self):
        return (
            f"""WHERE local."name" {self.schema_pattern_type} '{self.schema_pattern}'"""
            if self.schema_pattern
            else ""
        )

    @staticmethod
    def _db_obj_based_where_clause(pattern: str, pattern_type: str) -> str:
        return f"""WHERE local."name" {pattern_type} '{pattern}'""" if pattern else ""

    @property
    def db_obj_based_where_clause(self):
        return self._db_obj_based_where_clause(
            self.obj_name_pattern, self.obj_name_pattern_type
        )

    @property
    def db_obj2_based_where_clause(self):
        return self._db_obj_based_where_clause(
            self.obj2_name_pattern, self.obj2_name_pattern_type
        )

    @property
    def column_based_where_clause(self) -> str:
        if self.schema_name:
            return f"""WHERE "COLUMN_SCHEMA" = '{self.schema_name}'"""
        elif self.schema_pattern:
            return f"""WHERE "COLUMN_SCHEMA" {self.schema_pattern_type} '{self.schema_pattern}'"""
        return ""


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(
            schema_name="EXA_TOOLBOX",
            expected_where_clause="""WHERE "TABLE_SCHEMA" = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            obj_name_pattern="PUB",
            obj_name_pattern_type="REGEXP_LIKE",
            expected_where_clause="""WHERE local."name" REGEXP_LIKE 'PUB'""",
        ),
        MetaParams(
            schema_name="EXA_TOOLBOX",
            schema_pattern="EXA%",
            schema_pattern_type="LIKE",
            obj_name_pattern="PUB%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE local."name" LIKE 'PUB%' AND "TABLE_SCHEMA" = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            schema_pattern="EXA",
            schema_pattern_type="REGEXP_LIKE",
            obj_name_pattern="PUB%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE local."name" LIKE 'PUB%' AND "TABLE_SCHEMA" REGEXP_LIKE 'EXA'""",
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
def test_get_metadata(meta_params):
    config = McpServerSettings(
        schemas=meta_params.schema_settings,
        tables=meta_params.db_obj_settings,
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
        FROM SYS."EXA_ALL_TABLES"
        {meta_params.expected_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(expected_where_clause="""WHERE "SCRIPT_TYPE" = 'UDF'"""),
        MetaParams(
            schema_name="EXA_TOOLBOX",
            expected_where_clause="""WHERE "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            obj_name_pattern="BUCKETFS%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE local."name" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF'""",
        ),
        MetaParams(
            schema_name="EXA_TOOLBOX",
            schema_pattern="EXA%",
            schema_pattern_type="LIKE",
            obj_name_pattern="BUCKETFS%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE local."name" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" = 'EXA_TOOLBOX'""",
        ),
        MetaParams(
            schema_pattern="EXA%",
            schema_pattern_type="LIKE",
            obj_name_pattern="BUCKETFS%",
            obj_name_pattern_type="LIKE",
            expected_where_clause="""WHERE local."name" LIKE 'BUCKETFS%' AND "SCRIPT_TYPE" = 'UDF' AND "SCRIPT_SCHEMA" LIKE 'EXA%'""",
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
def test_get_script_metadata(meta_params):
    config = McpServerSettings(
        schemas=meta_params.schema_settings, scripts=meta_params.db_obj_settings
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
        FROM SYS."EXA_ALL_SCRIPTS"
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
        FROM SYS."EXA_ALL_SCHEMAS"
        {meta_params.schema_based_where_clause}
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
        {meta_params.schema_based_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(
            schema_name="EXA_TOOLBOX",
            schema_pattern="EXA",
            schema_pattern_type="REGEXP_LIKE",
        ),
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
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_tables(meta_params.schema_name))
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
                {meta_params.column_based_where_clause}
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
        {meta_params.db_obj_based_where_clause}
    """
    )
    assert query == expected_query


@pytest.mark.parametrize(
    "meta_params",
    [
        MetaParams(),
        MetaParams(
            schema_name="EXA_TOOLBOX", schema_pattern="EXA%", schema_pattern_type="LIKE"
        ),
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
    )
    meta_query = ExasolMetaQuery(config)
    query = collapse_spaces(meta_query.find_tables(meta_params.schema_name))
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
                {meta_params.column_based_where_clause}
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
        {meta_params.db_obj_based_where_clause}
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
        {meta_params.db_obj2_based_where_clause}
    """
    )
    assert query == expected_query
