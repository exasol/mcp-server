from enum import Enum
from functools import cache
from textwrap import dedent
from typing import Any

from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaListSettings,
)
from exasol.ai.mcp.server.utils import sql_text_value

INFO_COLUMN = "SUPPORT_INFO"
"""
Column with additional information, to be used for filtering.
"""


class MetaType(Enum):
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
    VIEW = "VIEW"
    FUNCTION = "FUNCTION"
    SCRIPT = "SCRIPT"
    COLUMN = "COLUMN"


def _where_clause(*predicates) -> str:
    condition = " AND ".join(filter(bool, predicates))
    if condition:
        return f"WHERE {condition}"
    return ""


def _get_meta_predicates(column: str, conf: MetaListSettings) -> list[str]:
    """
    Constructs predicates for the provided column using the LIKE and REGEXP_LIKE
    patterns in the provided configuration.
    """
    predicates: list[str] = []
    if conf.like_pattern:
        predicates.append(f'"{column}" LIKE {sql_text_value(conf.like_pattern)}')
    if conf.regexp_pattern:
        predicates.append(
            f'"{column}" REGEXP_LIKE {sql_text_value(conf.regexp_pattern)}'
        )
    return predicates


def _inner_meta_query(
    meta_type: MetaType, output_types: list[MetaType], *predicates
) -> str:
    """
    Builds a query collecting names and comments for the given type of meta and putting
    them in a json string. The comment element is omitted if NULL.

    Args:
        meta_type:
            Metadata type to collect the data for.
        output_types:
            List of metadata types to go in the SELECT list. This should correspond to
            the `meta_type`. For example, the output type TABLE can be used only with
            the meta type COLUMN, but 'SCHEMA' can be used with any meta type.
        predicates:
            WHERE clause predicates.

    Example output for `meta_name` = 'TABLE', output_types = ['SCHEMA'].
    {
        'SCHEMA_NAME': '<schema name>',
        'OBJ_INFO': '{"TABLE": "<table1 name>", "COMMENT": "<table1 comment>"}'
    }
    """
    meta_name = meta_type.value
    select_list = ", ".join(
        f'"{meta_name}_{out_type.value}" AS "{out_type.value}"'
        for out_type in output_types
    )
    return dedent(
        f"""
        SELECT
            {select_list},
            CONCAT(
                '{{"{meta_name}": "', "{meta_name}_NAME",
                NVL2("{meta_name}_COMMENT", CONCAT('", "COMMENT": "', "{meta_name}_COMMENT"), ''),
                '"}}'
            ) AS "OBJ_INFO"
        FROM SYS."EXA_ALL_{meta_name}S"
        {_where_clause(*predicates)}
    """
    )


class ExasolMetaQuery:
    """
    A query builder class, constructing metadata queries based on the provided server
    configuration.
    """

    def __init__(self, config: McpServerSettings):
        self._config = config
        self._meta_conf = {
            MetaType.SCHEMA: config.schemas,
            MetaType.TABLE: config.tables,
            MetaType.VIEW: config.views,
            MetaType.FUNCTION: config.functions,
            MetaType.SCRIPT: config.scripts,
        }

    @property
    def config(self) -> McpServerSettings:
        return self._config

    def get_metadata(self, meta_type: MetaType, schema_name: str | None = None) -> str:
        """
        A generic metadata query. Collects the DB object name, schema and comment
        for a given object type. Applies visibility restrictions for the type of
        metadata in question and for the schema. Optionally, limits the output to
        the objects in one particular schema.

        Args:
            meta_type:
                Metadata type to collect the data for.
            schema_name:
                An optional schema name provided in the call to the tool. Ignored if
                meta_name=='SCHEMA'. In all other cases, if the name is specified, it
                will be included in the WHERE clause. Otherwise, the query will return
                objects from all visible schemas.
        """
        conf = self._meta_conf[meta_type]
        meta_name = meta_type.value
        select_list = [
            f'"{meta_name}_NAME" AS "{conf.name_field}"',
            f'"{meta_name}_COMMENT" AS "{conf.comment_field}"',
        ]
        predicates = [conf.select_predicate]
        if meta_type == MetaType.SCRIPT:
            predicates.append(""" "SCRIPT_TYPE" = 'UDF' """)
        if meta_type != MetaType.SCHEMA:
            schema_column = f"{meta_name}_SCHEMA"
            if schema_name:
                predicates.append(f'"{schema_column}" = {sql_text_value(schema_name)}')
            else:
                # Adds the schema restriction if specified in the settings.
                predicates.extend(
                    _get_meta_predicates(schema_column, self.config.schemas)
                )
            select_list.append(f'"{schema_column}" AS "{conf.schema_field}"')
        return dedent(
            f"""
            SELECT {', '.join(select_list)}
            FROM SYS."EXA_ALL_{meta_name}S"
            {_where_clause(*predicates)}
        """
        )

    @cache
    def find_schemas(self) -> str:
        """
        Collects names, comments and the support information for schemas within
        the defined schema visibility.

        The support information includes the names and comments of the database
        objects within the schema. The visibility rules defined for each type of
        metadata is applied when the support information is collected.
        The support information is formated as json. For an example see the
        `_inner_meta_query`.
        """
        inner_queries = "UNION".join(
            _inner_meta_query(
                meta_type,
                [MetaType.SCHEMA],
                predicate,
                *_get_meta_predicates(
                    f"{meta_type.value}_NAME", self._meta_conf[meta_type]
                ),
            )
            for meta_type, predicate in [
                (MetaType.TABLE, ""),
                (MetaType.VIEW, ""),
                (MetaType.FUNCTION, ""),
                (MetaType.SCRIPT, """"SCRIPT_TYPE"='UDF'"""),
            ]
            if self._meta_conf[meta_type].enable
        )
        predicate = self._config.schemas.select_predicate
        return dedent(
            f"""
            SELECT
                S."SCHEMA_NAME" AS "{self._config.schemas.name_field}",
                S."SCHEMA_COMMENT" AS "{self._config.schemas.comment_field}",
                O."{INFO_COLUMN}"
            FROM SYS.EXA_ALL_SCHEMAS S
            JOIN (
                SELECT
                    "SCHEMA",
                    CONCAT('[', GROUP_CONCAT(DISTINCT "OBJ_INFO" SEPARATOR ', '), ']') AS "{INFO_COLUMN}"
                FROM ({inner_queries})
                GROUP BY "SCHEMA"
            ) O ON S."SCHEMA_NAME" = O."SCHEMA"
            {_where_clause(predicate)}
        """
        )

    def find_tables(self, schema_name: str | None) -> str:
        """
        Collects names, comments and support information for tables and/or views,
        depending on what is enabled. The visibility rules defined for the schemas,
        tables and views are applied. An optional `schema_name`, if provided, restricts
        the listing of the tables and views to this particular schema.

        The support information includes the names and comments of the columns.
        It is formated as json. For an example see the `_inner_meta_query`.
        """
        if schema_name:
            predicates = [f'"COLUMN_SCHEMA" = {sql_text_value(schema_name)}']
        else:
            predicates = _get_meta_predicates("COLUMN_SCHEMA", self.config.schemas)
        inner_query = _inner_meta_query(
            MetaType.COLUMN, [MetaType.SCHEMA, MetaType.TABLE], *predicates
        )
        main_query = "UNION".join(
            dedent(
                f"""
                SELECT
                    T."{meta_name}_NAME" AS "{conf.name_field}",
                    T."{meta_name}_COMMENT" AS "{conf.comment_field}",
                    T."{meta_name}_SCHEMA" AS "{conf.schema_field}",
                    C."{INFO_COLUMN}"
                FROM SYS.EXA_ALL_{meta_name}S T
                JOIN C ON
                    T."{meta_name}_SCHEMA" = C."SCHEMA" AND
                    T."{meta_name}_NAME" = C."TABLE"
                {_where_clause(conf.select_predicate)}
            """
            )
            for meta_name, conf in [
                ("TABLE", self._config.tables),
                ("VIEW", self._config.views),
            ]
            if conf.enable
        )
        return dedent(
            f"""
            WITH C AS (
                SELECT
                    "SCHEMA",
                    "TABLE",
                    CONCAT('[', GROUP_CONCAT(DISTINCT "OBJ_INFO" SEPARATOR ', '), ']') AS "{INFO_COLUMN}"
                FROM ({inner_query})
                GROUP BY "SCHEMA", "TABLE"
            )
            {main_query}
        """
        )

    def describe_columns(self, schema_name: str, table_name: str) -> str:
        """
        Gathers a list of columns in a given table.
        """
        conf = self._config.columns
        return dedent(
            f"""
            SELECT
                COLUMN_NAME AS "{conf.name_field}",
                COLUMN_TYPE AS "{conf.type_field}",
                COLUMN_COMMENT AS "{conf.comment_field}"
            FROM SYS.EXA_ALL_COLUMNS
            WHERE
                COLUMN_SCHEMA = {sql_text_value(schema_name)} AND
                COLUMN_TABLE = {sql_text_value(table_name)}
        """
        )

    def describe_constraints(self, schema_name: str, table_name: str) -> str:
        """
        Gathers a list of constraints for a given table.
        """
        conf = self._config.columns
        return dedent(
            f"""
            SELECT
                FIRST_VALUE(CONSTRAINT_TYPE) AS "{conf.constraint_type_field}",
                CASE LEFT(CONSTRAINT_NAME, 4) WHEN 'SYS_' THEN NULL
                    ELSE CONSTRAINT_NAME END AS "{conf.constraint_name_field}",
                GROUP_CONCAT(DISTINCT COLUMN_NAME ORDER BY ORDINAL_POSITION)
                    AS "{conf.constraint_columns_field}",
                FIRST_VALUE(REFERENCED_SCHEMA) AS "{conf.referenced_schema_field}",
                FIRST_VALUE(REFERENCED_TABLE) AS "{conf.referenced_table_field}",
                GROUP_CONCAT(DISTINCT REFERENCED_COLUMN ORDER BY ORDINAL_POSITION)
                    AS "{conf.referenced_columns_field}"
            FROM SYS.EXA_ALL_CONSTRAINT_COLUMNS
            WHERE
                CONSTRAINT_SCHEMA = {sql_text_value(schema_name)} AND
                CONSTRAINT_TABLE = {sql_text_value(table_name)}
            GROUP BY CONSTRAINT_NAME
        """
        )

    @staticmethod
    def get_table_comment(schema_name: str, table_name: str) -> str | None:
        """
        The query returns a single row with a comment for a given table or view.
        """
        # `table_name` can be the name of a table or a view.
        # This query tries both possibilities. The UNION clause collapses
        # the result into a single non-NULL distinct value.
        return dedent(
            f"""
            SELECT TABLE_COMMENT AS COMMENT FROM SYS.EXA_ALL_TABLES
            WHERE
                TABLE_SCHEMA = {sql_text_value(schema_name)} AND
                TABLE_NAME = {sql_text_value(table_name)}
            UNION
            SELECT VIEW_COMMENT AS COMMENT FROM SYS.EXA_ALL_VIEWS
            WHERE
                VIEW_SCHEMA = {sql_text_value(schema_name)} AND
                VIEW_NAME = {sql_text_value(table_name)}
            LIMIT 1;
        """
        )
