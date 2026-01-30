import re
from enum import Enum
from functools import cache

import sqlglot.expressions as exp

from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaListSettings,
)

INFO_COLUMN = "SUPPORT_INFO"
"""
Column with additional information, to be used for filtering.
"""

GROUP_CONCAT = "GROUP_CONCAT_"
"""
A surrogate name for the GROUP_CONCAT function, to be replaced with "GROUP_CONCAT"
in the syntax correction.
"""


class MetaType(Enum):
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
    VIEW = "VIEW"
    FUNCTION = "FUNCTION"
    SCRIPT = "SCRIPT"
    COLUMN = "COLUMN"


class SysInfoType(Enum):
    SYSTEM = "SYS"
    STATISTICS = "EXA_STATISTICS"


@cache
def _get_group_concat_pattern() -> re.Pattern:
    return re.compile(
        rf'{GROUP_CONCAT}\(\s*((?:DISTINCT)?\s+"\w+"),?(\s+ORDER\sBY\s+"\w+")?\s*\)'
    )


def _fix_group_concat(query_sql: str, separator: str | None = None) -> str:
    """
    Corrects the GROUP_CONCAT syntax, as the Exasol syntax is not yet supported.
    """
    pattern = _get_group_concat_pattern()
    repl_separator = f" SEPARATOR '{separator}'" if separator else ""
    return pattern.sub(rf"GROUP_CONCAT(\1\2{repl_separator})", query_sql)


def _get_meta_predicates(column: str, conf: MetaListSettings) -> list[exp.Predicate]:
    """
    Constructs predicates for the provided column using the LIKE and REGEXP_LIKE
    patterns in the provided configuration.
    """
    predicates: list[exp.Predicate] = []
    if conf.like_pattern:
        predicates.append(exp.column(column).like(conf.like_pattern))
    if conf.regexp_pattern:
        predicates.append(
            exp.func(
                "REGEXP_INSTR",
                exp.column(column),
                exp.Literal.string(conf.regexp_pattern),
            ).neq(0)
        )
    return predicates


def _inner_meta_query(
    meta_type: MetaType, output_types: list[MetaType], *predicates
) -> exp.Query:
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
    select_list = [
        exp.column(f"{meta_name}_{out_type.value}").as_(out_type.value)
        for out_type in output_types
    ]
    query = (
        exp.Select()
        .select(
            *select_list,
            exp.func(
                "CONCAT",
                exp.Literal.string(f'{{"{meta_name}": "'),
                exp.column(f"{meta_name}_NAME"),
                exp.func(
                    "NVL2",
                    exp.column(f"{meta_name}_COMMENT"),
                    exp.func(
                        "CONCAT",
                        exp.Literal.string('", "COMMENT": "'),
                        exp.column(f"{meta_name}_COMMENT"),
                    ),
                    exp.Literal.string(""),
                ),
                exp.Literal.string('"}'),
            ).as_("OBJ_INFO"),
        )
        .from_(exp.Table(this=f"EXA_ALL_{meta_name}S", db="SYS"))
        .where(*predicates)
    )
    return query


@cache
def _info_concat_func() -> exp.Expression:
    return exp.func(
        "CONCAT",
        exp.Literal.string("["),
        exp.func(GROUP_CONCAT, exp.Distinct(expressions=[exp.column("OBJ_INFO")])),
        exp.Literal.string("]"),
    ).as_(INFO_COLUMN)


def is_system_schema(schema_name: str) -> bool:
    return schema_name.upper() in ["SYS", "EXA_STATISTICS"]


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

    def _get_column_eq_predicate(
        self, column: str, value: str, table: str | None = None
    ) -> exp.Predicate:
        """
        Builds an expression of a text column equality, considering the
        case sensitivity setting.
        """
        if self.config.case_sensitive:
            return exp.column(column, table=table).eq(exp.Literal.string(value))
        return exp.func("UPPER", exp.column(column, table=table)).eq(
            exp.Literal.string(value.upper())
        )

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
        meta_name_column = f"{meta_name}_NAME"
        select_list = [
            exp.column(meta_name_column).as_(conf.name_field),
            exp.column(f"{meta_name}_COMMENT").as_(conf.comment_field),
        ]
        predicates = _get_meta_predicates(meta_name_column, conf)
        if meta_type == MetaType.SCRIPT:
            predicates.append(exp.column("SCRIPT_TYPE").eq("UDF"))
        if meta_type != MetaType.SCHEMA:
            schema_column = f"{meta_name}_SCHEMA"
            if schema_name:
                predicates.append(
                    self._get_column_eq_predicate(schema_column, schema_name)
                )
            else:
                # Adds the schema restriction if specified in the settings.
                predicates.extend(
                    _get_meta_predicates(schema_column, self.config.schemas)
                )
            select_list.append(exp.column(schema_column).as_(conf.schema_field))
        query = (
            exp.Select()
            .select(*select_list)
            .from_(exp.Table(this=f"EXA_ALL_{meta_name}S", db="SYS"))
            .where(*predicates)
        )
        return query.sql(dialect="exasol", identify=True)

    def get_object_metadata(
        self, meta_type: MetaType, schema_name: str, obj_name: str
    ) -> str:
        """
        Builds a query requesting metadata for a given database object.
        """
        meta_name = meta_type.value
        query = (
            exp.Select()
            .select(exp.Star())
            .from_(exp.Table(this=f"EXA_ALL_{meta_name}S", db="SYS"))
            .where(
                exp.and_(
                    self._get_column_eq_predicate(f"{meta_name}_SCHEMA", schema_name),
                    self._get_column_eq_predicate(f"{meta_name}_NAME", obj_name),
                )
            )
        )
        return query.sql(dialect="exasol", identify=True)

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
        inner_queries = exp.Paren(
            this=exp.union(
                *(
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
                        (
                            MetaType.SCRIPT,
                            exp.column("SCRIPT_TYPE").eq(exp.Literal.string("UDF")),
                        ),
                    ]
                    if self._meta_conf[meta_type].enable
                ),
                dialect="exasol",
            )
        )

        predicates = _get_meta_predicates("SCHEMA_NAME", self._config.schemas)

        query = (
            exp.Select()
            .select(
                exp.column("SCHEMA_NAME", table="S").as_(
                    self._config.schemas.name_field
                ),
                exp.column("SCHEMA_COMMENT", table="S").as_(
                    self._config.schemas.comment_field
                ),
                exp.column(INFO_COLUMN, db="O"),
            )
            .from_(exp.Table(this="EXA_ALL_SCHEMAS", db="SYS").as_("S"))
            .where(*predicates)
            .join(
                exp.Select()
                .select(exp.column("SCHEMA"), _info_concat_func())
                .from_(inner_queries)
                .group_by("SCHEMA")
                .as_("O"),
                on=exp.column("SCHEMA_NAME", table="S").eq(
                    exp.column("SCHEMA", table="O")
                ),
            )
        )
        query_sql = query.sql(dialect="exasol", identify=True)
        return _fix_group_concat(query_sql, separator=", ")

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
            predicates = [self._get_column_eq_predicate("COLUMN_SCHEMA", schema_name)]
        else:
            predicates = _get_meta_predicates("COLUMN_SCHEMA", self.config.schemas)
        inner_query = _inner_meta_query(
            MetaType.COLUMN, [MetaType.SCHEMA, MetaType.TABLE], *predicates
        )
        cte = (
            exp.Select()
            .select(exp.column("SCHEMA"), exp.column("TABLE"), _info_concat_func())
            .from_(exp.Paren(this=inner_query))
            .group_by("SCHEMA", "TABLE")
        )
        main_queries = [
            (
                exp.Select()
                .select(
                    exp.column(f"{meta_name}_NAME", table="T").as_(conf.name_field),
                    exp.column(f"{meta_name}_COMMENT", table="T").as_(
                        conf.comment_field
                    ),
                    exp.column(f"{meta_name}_SCHEMA", table="T").as_(conf.schema_field),
                    exp.column(INFO_COLUMN, table="C"),
                )
                .from_(exp.Table(this=f"EXA_ALL_{meta_name}S", db="SYS").as_("T"))
                .join(
                    "C",
                    on=exp.and_(
                        exp.column(f"{meta_name}_SCHEMA", table="T").eq(
                            exp.column("SCHEMA", table="C")
                        ),
                        exp.column(f"{meta_name}_NAME", table="T").eq(
                            exp.column("TABLE", table="C")
                        ),
                    ),
                )
                .where(*_get_meta_predicates(f"{meta_name}_NAME", conf))
            )
            for meta_name, conf in [
                ("TABLE", self._config.tables),
                ("VIEW", self._config.views),
            ]
            if conf.enable
        ]
        query = main_queries[0] if len(main_queries) == 1 else exp.union(*main_queries)
        query = query.with_(alias="C", as_=cte)
        query_sql = query.sql(dialect="exasol", identify=True)
        return _fix_group_concat(query_sql, separator=", ")

    def describe_columns(self, schema_name: str, table_name: str) -> str:
        """
        Gathers a list of columns in a given table.
        """
        conf = self._config.columns
        # The table where the colum information should be pulled from is different for
        # user tables and system tables.
        source = (
            "EXA_SYS_COLUMNS" if is_system_schema(schema_name) else "EXA_ALL_COLUMNS"
        )
        query = (
            exp.Select()
            .select(
                exp.column("COLUMN_NAME").as_(conf.name_field),
                exp.column("COLUMN_TYPE").as_(conf.type_field),
                exp.column("COLUMN_COMMENT").as_(conf.comment_field),
            )
            .from_(exp.Table(this=source, db="SYS"))
            .where(
                exp.and_(
                    self._get_column_eq_predicate("COLUMN_SCHEMA", schema_name),
                    self._get_column_eq_predicate("COLUMN_TABLE", table_name),
                )
            )
        )
        return query.sql(dialect="exasol", identify=True)

    def describe_constraints(self, schema_name: str, table_name: str) -> str:
        """
        Gathers a list of constraints for a given table.
        """
        conf = self._config.columns
        query = (
            exp.Select()
            .select(
                exp.FirstValue(this=exp.column("CONSTRAINT_TYPE")).as_(
                    conf.constraint_type_field
                ),
                exp.case(exp.func("LEFT", exp.column("CONSTRAINT_NAME"), 4))
                .when(exp.Literal.string("SYS_"), exp.Null())
                .else_(exp.column("CONSTRAINT_NAME"))
                .as_(conf.constraint_name_field),
                exp.func(
                    GROUP_CONCAT,
                    exp.Distinct(expressions=[exp.column("COLUMN_NAME")]),
                    exp.Order(expressions=[exp.column("ORDINAL_POSITION")]),
                ).as_(conf.constraint_columns_field),
                exp.FirstValue(this=exp.column("REFERENCED_SCHEMA")).as_(
                    conf.referenced_schema_field
                ),
                exp.FirstValue(this=exp.column("REFERENCED_TABLE")).as_(
                    conf.referenced_table_field
                ),
                exp.func(
                    GROUP_CONCAT,
                    exp.Distinct(expressions=[exp.column("REFERENCED_COLUMN")]),
                    exp.Order(expressions=[exp.column("ORDINAL_POSITION")]),
                ).as_(conf.referenced_columns_field),
            )
            .from_(exp.Table(this="EXA_ALL_CONSTRAINT_COLUMNS", db="SYS"))
            .where(
                exp.and_(
                    self._get_column_eq_predicate("CONSTRAINT_SCHEMA", schema_name),
                    self._get_column_eq_predicate("CONSTRAINT_TABLE", table_name),
                )
            )
            .group_by("CONSTRAINT_NAME")
        )
        query_sql = query.sql(dialect="exasol", identify=True)
        return _fix_group_concat(query_sql)

    def get_table_comment(self, schema_name: str, table_name: str) -> str:
        """
        The query returns a single row with a comment for a given table or view.
        """
        # `table_name` can be the name of a table or a view.
        # This query tries both possibilities. The UNION clause collapses
        # the result into a single non-NULL distinct value.
        queries = [
            exp.Select()
            .select(exp.column(f"{meta_name}_COMMENT").as_("COMMENT"))
            .from_(exp.Table(this=f"EXA_ALL_{meta_name}S", db="SYS"))
            .where(
                exp.and_(
                    self._get_column_eq_predicate(f"{meta_name}_SCHEMA", schema_name),
                    self._get_column_eq_predicate(f"{meta_name}_NAME", table_name),
                )
            )
            for meta_name in ["TABLE", "VIEW"]
        ]
        query = exp.Union(this=queries[0], expression=queries[1]).limit(1)
        return query.sql(dialect="exasol", identify=True)

    @staticmethod
    def get_sql_types() -> str:
        """
        The query returns a list of primary SQL types defined in the EXA_SQL_TYPES
        table. It selects the "TYPE_NAME" and "CREATE_PARAMS" columns.
        """
        query = (
            exp.Select()
            .select(
                exp.column("TYPE_NAME"),
                exp.column("CREATE_PARAMS"),
                exp.column("PRECISION"),
            )
            .from_(exp.Table(this="EXA_SQL_TYPES", db="SYS"))
        )
        return query.sql(dialect="exasol", identify=True)

    def get_system_tables(
        self, info_type: SysInfoType, table_name: str | None = None
    ) -> str:
        """
        Collects names and comments for the system or statistics tables and views.
        The output will be restricted to one table if it's name is specified.
        """
        conf = self._config.tables
        predicates = [self._get_column_eq_predicate("SCHEMA_NAME", info_type.value)]
        if table_name:
            predicates.append(self._get_column_eq_predicate("OBJECT_NAME", table_name))
        query = (
            exp.Select()
            .select(
                exp.column("SCHEMA_NAME").as_(conf.schema_field),
                exp.column("OBJECT_NAME").as_(conf.name_field),
                exp.column("OBJECT_COMMENT").as_(conf.comment_field),
            )
            .from_(exp.Table(this="EXA_SYSCAT", db="SYS"))
            .where(*predicates)
        )
        return query.sql(dialect="exasol", identify=True)

    @staticmethod
    def get_keywords(reserved: bool, letter: str) -> str:
        """
        The query returns a list of keywords that start from the given letter.
        The list is restricted to keywords that are either reserved words or not
        reserver words, as per the argument.
        """
        query = (
            exp.Select()
            .select(exp.column("KEYWORD"))
            .from_(exp.Table(this="EXA_SQL_KEYWORDS", db="SYS"))
            .where(
                exp.column("RESERVED").eq(exp.Boolean(this=reserved)),
                exp.func("LEFT", exp.column("KEYWORD"), 1).eq(
                    exp.Literal.string(letter.upper())
                ),
            )
            .order_by(exp.column("KEYWORD"))
        )
        return query.sql(dialect="exasol", identify=True)
