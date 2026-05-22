import re
from functools import cache
from typing import (
    Annotated,
    Any,
    TypeVar,
    cast,
)

import exasol.bucketfs as bfs
from fastmcp import (
    Context,
    FastMCP,
)
from pydantic import (
    BaseModel,
    Field,
)
from sqlglot import (
    exp,
    parse_one,
)
from sqlglot.errors import ParseError
from starlette.responses import JSONResponse

from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.setup.server_settings import McpServerSettings
from exasol.ai.mcp.server.tools.bucketfs_tools import BucketFsTools
from exasol.ai.mcp.server.tools.meta_query import (
    INFO_COLUMN,
    ExasolMetaQuery,
    MetaType,
    SysInfoType,
    is_system_schema,
)
from exasol.ai.mcp.server.tools.parameter_parser import (
    FuncParameterParser,
    ScriptParameterParser,
)
from exasol.ai.mcp.server.tools.parameter_pattern import (
    parameter_list_pattern,
    regex_flags,
)
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    DBColumn,
    DBColumnSummary,
    DBConstraint,
    DBEmitFunction,
    DBObject,
    DBReturnFunction,
    DBTable,
    DBTableSummary,
    QualifiedDBObject,
    SQLTypeInfo,
)
from exasol.ai.mcp.server.utils.keyword_search import keyword_filter

SchemaNameArg = Annotated[str, Field(description="Name of the database schema")]

OptionalSchemaNameArg = Annotated[
    str | None,
    Field(
        description=(
            "An optional name of the database schema. "
            "If specified, restricts the search to objects in this schema."
        ),
        default="",
    ),
]

KeywordsArg = Annotated[
    list[str],
    Field(
        description=(
            "The list of keywords to rank and filter the result. "
            "The tool is looking for these keywords in the database object "
            "names and comments. "
            "The list should include common inflections of each keyword."
        )
    ),
]

TableNameArg = Annotated[str, Field(description="Name of the table or view")]

FunctionNameArg = Annotated[str, Field(description="Name of the function")]

QueryArg = Annotated[
    str,
    Field(
        description=(
            "SQL Query. "
            "In an query, the names of database objects, such as schemas, "
            "tables and columns should be enclosed in double quotes. "
            "A reference to a table or function should include a reference to its schema. "
            "The SELECT column list cannot have both the * and explicit column names."
        )
    ),
]

RowLimitArg = Annotated[
    int | None,
    Field(
        description=(
            "If specified, wraps the query in SELECT * FROM (<query>) LIMIT <row_limit> "
            "to preview a sample of results without fetching all rows."
        ),
        default=None,
        ge=1,
    ),
]

M = TypeVar("M", bound=BaseModel)


@cache
def _get_emits_pattern() -> re.Pattern:
    pattern = rf"EMITS\s*\({parameter_list_pattern}\)"
    return re.compile(pattern, flags=regex_flags)


def verify_query(query: str) -> bool:
    """
    Verifies that the query is a valid SELECT query.
    Declines any other types of statements including the SELECT INTO.
    """

    # Here is a fix for the SQLGlot deficiency in understanding the syntax of variadic
    # emit UDF. The EMITS clause in the SELECT statement is currently not recognised.
    # To let SQLGlot validate the query, this clause must be pinched away.
    query = _get_emits_pattern().sub("", query)

    try:
        ast = parse_one(query, read="exasol")
        if isinstance(ast, exp.Select):
            return "into" not in ast.args
        return False
    except ParseError:
        return False


def remove_info_column(result: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Removes the column with extra information, collected for data filtering.
    """
    for row in result:
        if INFO_COLUMN in row:
            row.pop(INFO_COLUMN)
    return result


_NUMERIC_TYPE_PREFIXES = frozenset(
    [
        "DECIMAL",
        "NUMERIC",
        "DOUBLE",
        "DOUBLE PRECISION",
        "FLOAT",
        "REAL",
        "INTEGER",
        "INT",
        "BIGINT",
        "SMALLINT",
        "SHORTINT",
        "BYTEINT",
        "TINYINT",
        "NUMBER",
    ]
)


def _is_numeric_type(sql_type: str) -> bool:
    return sql_type.split("(")[0].strip().upper() in _NUMERIC_TYPE_PREFIXES


def _build_stats_query(table_ref: exp.Table, columns: list[DBColumn]) -> str:
    """
    Builds a single-row SELECT that computes:
    - ROW_COUNT: total row count
    - DISTINCT_i: COUNT(DISTINCT col) for every column
    - MIN_i / MAX_i: min and max for numeric columns
    - NULL_COUNT_i: COUNT(*) - COUNT(col) — number of NULLs per column
    Positional aliases avoid collisions with arbitrary column names.
    """
    select_exprs: list[exp.Expression] = [
        exp.alias_(exp.Count(this=exp.Star()), "ROW_COUNT"),
    ]
    for i, col in enumerate(columns):
        col_ref = exp.Column(this=exp.Identifier(this=col.name, quoted=True))
        select_exprs.append(
            exp.alias_(
                exp.Count(this=exp.Distinct(expressions=[col_ref])),
                f"DISTINCT_{i}",
            )
        )
        if _is_numeric_type(col.type):
            select_exprs.append(exp.alias_(exp.Min(this=col_ref), f"MIN_{i}"))
            select_exprs.append(exp.alias_(exp.Max(this=col_ref), f"MAX_{i}"))
        null_col_ref = exp.Column(this=exp.Identifier(this=col.name, quoted=True))
        select_exprs.append(
            exp.alias_(
                exp.Sub(
                    this=exp.Count(this=exp.Star()),
                    expression=exp.Count(this=null_col_ref),
                ),
                f"NULL_COUNT_{i}",
            )
        )
    return exp.Select().from_(table_ref).select(*select_exprs).sql(dialect="exasol")


def _build_top_values_query(table_ref: exp.Table, col: DBColumn, top_n: int) -> str:
    """
    Builds a query that returns the top_n most frequent non-NULL values of a column,
    ordered by descending frequency.
    """

    def col_ref() -> exp.Column:
        return exp.Column(this=exp.Identifier(this=col.name, quoted=True))

    return (
        exp.Select()
        .from_(table_ref)
        .select(col_ref())
        .where(exp.Not(this=exp.Is(this=col_ref(), expression=exp.Null())))
        .group_by(col_ref())
        .order_by(exp.Ordered(this=exp.Count(this=exp.Star()), desc=True))
        .limit(top_n)
        .sql(dialect="exasol")
    )


def _build_sample_query(table_ref: exp.Table, sample_size: int) -> str:
    return (
        exp.Select()
        .from_(table_ref)
        .select(exp.Star())
        .limit(sample_size)
        .sql(dialect="exasol")
    )


def _build_column_summaries(
    columns: list[DBColumn],
    stats_row: dict[str, Any] | None,
    column_top_values: list[list[Any]],
) -> list[DBColumnSummary]:
    summaries = []
    row_count = int(stats_row.get("ROW_COUNT", 0) or 0) if stats_row else 0
    for i, col in enumerate(columns):
        is_numeric = _is_numeric_type(col.type)
        raw_min = stats_row.get(f"MIN_{i}") if is_numeric and stats_row else None
        raw_max = stats_row.get(f"MAX_{i}") if is_numeric and stats_row else None
        null_count = int(stats_row.get(f"NULL_COUNT_{i}", 0) or 0) if stats_row else 0
        summaries.append(
            DBColumnSummary(
                name=col.name,
                type=col.type,
                comment=col.comment,
                distinct_count=stats_row.get(f"DISTINCT_{i}", 0) if stats_row else 0,
                min=str(raw_min) if raw_min is not None else None,
                max=str(raw_max) if raw_max is not None else None,
                top_values=column_top_values[i],
                has_nulls=null_count > 0,
                null_percentage=(
                    round(null_count / row_count * 100) if row_count > 0 else 0
                ),
            )
        )
    return summaries


def _build_preview_query(query: str, row_limit: int) -> str:
    return (
        exp.select(exp.Star())
        .from_(parse_one(query, read="exasol").subquery())
        .limit(row_limit)
        .sql(dialect="exasol")
    )


_PROFILE_TABLE = exp.Table(
    this=exp.Identifier(this="EXA_USER_PROFILE_LAST_DAY"),
    db=exp.Identifier(this="EXA_STATISTICS"),
)

_PROFILE_COLUMNS = (
    "PART_NAME",
    "PART_INFO",
    "OBJECT_SCHEMA",
    "OBJECT_NAME",
    "OBJECT_ROWS",
    "DURATION",
    "CPU",
)


def _build_profile_select(query: str) -> str:
    """
    Builds a SELECT against EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY that returns
    the execution plan rows for the given query. sqlglot handles single-quote escaping.
    """
    subquery = (
        exp.Select()
        .from_(_PROFILE_TABLE)
        .select(exp.Max(this=exp.column("STMT_ID")))
        .where(exp.column("SQL_TEXT").eq(exp.Literal.string(query)))
    )
    return (
        exp.Select()
        .from_(_PROFILE_TABLE)
        .select(*_PROFILE_COLUMNS)
        .where(exp.column("STMT_ID").eq(subquery.subquery()))
        .order_by(exp.column("PART_ID"))
        .sql(dialect="exasol")
    )


class ExasolMCPServer(FastMCP):
    """
    An Exasol MCP server based on FastMCP.

    Args:
        connection:
            pyexasol connection wrapper.
        config:
            The server configuration.
        bucketfs_location:
            Optional BucketFS PathLike object. If not provided or None
            the BucketFS tools should not be registered.
        kwargs:
            Extra arguments to be passed to FastMCP.
    """

    def __init__(
        self,
        connection: DbConnection,
        config: McpServerSettings,
        bucketfs_location: bfs.path.PathLike | None = None,
        **kwargs,
    ) -> None:
        super().__init__(name="exasol-mcp", **kwargs)
        self.connection = connection
        self.bucketfs_tools = (
            BucketFsTools(bucketfs_location, config)
            if bucketfs_location is not None
            else None
        )
        self.meta_query = ExasolMetaQuery(config)

    @property
    def config(self) -> McpServerSettings:
        return self.meta_query.config

    def _execute_meta_query(
        self, query: str, model_cls: type[M], keywords: list[str] | None = None
    ) -> list[M]:
        """
        Executes a metadata query and returns the result as a list of instances
        of the specified class.
        Applies the keyword fitter if provided.
        Removes the column with extra information that could be added to the result
        to assist filtering. This is necessary to avoid polluting the LLM's context
        with data it doesn't need at the current stage.
        """
        result = self.connection.execute_query(query).fetchall()
        if keywords:
            result = keyword_filter(result, keywords, language=self.config.language)
            result = remove_info_column(result)
        return [model_cls.model_validate(row) for row in result]

    def list_schemas(self) -> list[DBObject]:
        if not self.config.schemas.enable:
            raise RuntimeError("The schema listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.SCHEMA)
        return self._execute_meta_query(query, DBObject)

    def find_schemas(self, keywords: KeywordsArg) -> list[DBObject]:
        if not self.config.schemas.enable:
            raise RuntimeError("The schema listing is disabled.")

        query = self.meta_query.find_schemas()
        return self._execute_meta_query(query, DBObject, keywords)

    def list_tables(self, schema_name: SchemaNameArg) -> list[QualifiedDBObject]:
        table_conf = self.config.tables
        view_conf = self.config.views
        if (not table_conf.enable) and (not view_conf.enable):
            raise RuntimeError("Both the table and the view listings are disabled.")

        query = "\nUNION\n".join(
            self.meta_query.get_metadata(meta_type, schema_name)
            for meta_type, conf in zip(
                [MetaType.TABLE, MetaType.VIEW], [table_conf, view_conf]
            )
            if conf.enable
        )
        return self._execute_meta_query(query, QualifiedDBObject)

    def find_tables(
        self, keywords: KeywordsArg, schema_name: OptionalSchemaNameArg
    ) -> list[QualifiedDBObject]:
        if (not self.config.tables.enable) and (not self.config.views.enable):
            raise RuntimeError("Both the table and the view listings are disabled.")

        query = self.meta_query.find_tables(schema_name)
        return self._execute_meta_query(query, QualifiedDBObject, keywords)

    def list_functions(self, schema_name: SchemaNameArg) -> list[QualifiedDBObject]:
        if not self.config.functions.enable:
            raise RuntimeError("The function listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.FUNCTION, schema_name)
        return self._execute_meta_query(query, QualifiedDBObject)

    def find_functions(
        self, keywords: KeywordsArg, schema_name: OptionalSchemaNameArg
    ) -> list[QualifiedDBObject]:
        if not self.config.functions.enable:
            raise RuntimeError("The function listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.FUNCTION, schema_name)
        return self._execute_meta_query(query, QualifiedDBObject, keywords)

    def list_scripts(self, schema_name: SchemaNameArg) -> list[QualifiedDBObject]:
        if not self.config.scripts.enable:
            raise RuntimeError("The script listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.SCRIPT, schema_name)
        return self._execute_meta_query(query, QualifiedDBObject)

    def find_scripts(
        self, keywords: KeywordsArg, schema_name: OptionalSchemaNameArg
    ) -> list[QualifiedDBObject]:
        if not self.config.scripts.enable:
            raise RuntimeError("The script listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.SCRIPT, schema_name)
        return self._execute_meta_query(query, QualifiedDBObject, keywords)

    def describe_columns(
        self, schema_name: SchemaNameArg, table_name: TableNameArg
    ) -> list[DBColumn]:
        """
        Returns the list of columns in the given table. Currently, this is a part of
        the `describe_table` tool, but it can be used independently in the future.
        """
        if not self.config.columns.enable:
            raise RuntimeError("The column listing is disabled.")

        query = self.meta_query.describe_columns(schema_name, table_name)
        return self._execute_meta_query(query, DBColumn)

    def describe_constraints(
        self, schema_name: SchemaNameArg, table_name: TableNameArg
    ) -> list[DBConstraint]:
        """
        Returns the list of constraints in the given table. Currently, this is a part
        of the `describe_table` tool, but it can be used independently in the future.
        """
        if not self.config.columns.enable:
            raise RuntimeError("The constraint listing is disabled.")

        query = self.meta_query.describe_constraints(schema_name, table_name)
        return self._execute_meta_query(query, DBConstraint)

    def describe_table(
        self, schema_name: SchemaNameArg, table_name: TableNameArg
    ) -> DBTable:

        system_table = is_system_schema(schema_name)
        if system_table:
            query = self.meta_query.get_system_tables(schema_name, table_name)
        else:
            query = self.meta_query.describe_table(schema_name, table_name)
        table_meta = self._execute_meta_query(query, QualifiedDBObject)
        if not table_meta:
            raise ValueError(f"The table or view {schema_name}.{table_name} not found.")

        return DBTable(
            **table_meta[0].model_dump(),
            columns=self.describe_columns(schema_name, table_name),
            constraints=(
                None
                if system_table
                else self.describe_constraints(schema_name, table_name)
            ),
        )

    def _get_table_comment(self, schema_name: str, table_name: str) -> str | None:
        if is_system_schema(schema_name):
            query = self.meta_query.get_system_tables(schema_name, table_name)
        else:
            query = self.meta_query.describe_table(schema_name, table_name)
        table_meta = self._execute_meta_query(query, QualifiedDBObject)
        if not table_meta:
            raise ValueError(f"The table or view {schema_name}.{table_name} not found.")
        return table_meta[0].comment

    def _fetch_column_top_values(
        self, table_ref: exp.Table, columns: list[DBColumn], top_n: int
    ) -> list[list[Any]]:
        result = []
        for col in columns:
            result.append(
                self.connection.execute_query(
                    _build_top_values_query(table_ref, col, top_n), snapshot=False
                ).fetchcol()
            )
        return result

    def summarize_table(
        self,
        schema_name: SchemaNameArg,
        table_name: TableNameArg,
        sample_size: Annotated[
            int,
            Field(
                description="Number of sample rows to include in the result",
                default=10,
                ge=1,
                le=100,
            ),
        ] = 10,
        top_values: Annotated[
            int,
            Field(
                description="Number of most common distinct values to return per column",
                default=5,
                ge=1,
                le=100,
            ),
        ] = 5,
    ) -> DBTableSummary:
        if not self.config.enable_summarize_table:
            raise RuntimeError("The table summarization is disabled.")
        columns = self.describe_columns(schema_name, table_name)
        comment = self._get_table_comment(schema_name, table_name)
        table_ref = exp.Table(
            this=exp.Identifier(this=table_name, quoted=True),
            db=exp.Identifier(this=schema_name, quoted=True),
        )
        stats_row = self.connection.execute_query(
            _build_stats_query(table_ref, columns), snapshot=False
        ).fetchone()
        column_top_values = self._fetch_column_top_values(
            table_ref, columns, top_values
        )
        sample_data = self.connection.execute_query(
            _build_sample_query(table_ref, sample_size), snapshot=False
        ).fetchall()
        return DBTableSummary(
            schema=schema_name,
            name=table_name,
            comment=comment,
            row_count=stats_row.get("ROW_COUNT", 0) if stats_row else 0,
            columns=_build_column_summaries(columns, stats_row, column_top_values),
            sample=sample_data,
        )

    def describe_function(
        self, schema_name: SchemaNameArg, func_name: FunctionNameArg
    ) -> DBReturnFunction:
        parser = FuncParameterParser(connection=self.connection, settings=self.config)
        return cast(DBReturnFunction, parser.describe(schema_name, func_name))

    def describe_script(
        self, schema_name: SchemaNameArg, func_name: FunctionNameArg
    ) -> DBReturnFunction | DBEmitFunction:
        parser = ScriptParameterParser(connection=self.connection, settings=self.config)
        return cast(
            DBReturnFunction | DBEmitFunction, parser.describe(schema_name, func_name)
        )

    def execute_query(
        self, query: QueryArg, row_limit: RowLimitArg = None
    ) -> list[dict[str, Any]]:
        if not self.config.enable_read_query:
            raise RuntimeError("Query execution is disabled.")
        if not verify_query(query):
            raise ValueError("The query is invalid or not a SELECT statement.")
        effective_query = (
            _build_preview_query(query, row_limit) if row_limit is not None else query
        )
        return self.connection.execute_query(effective_query, snapshot=False).fetchall()

    def profile_query(self, query: QueryArg) -> list[dict[str, Any]]:
        if not self.config.enable_query_profiling:
            raise RuntimeError("Query profiling is disabled.")
        if not verify_query(query):
            raise ValueError("The query is invalid or not a SELECT statement.")
        statements = [
            "ALTER SESSION SET PROFILE = 'ON'",
            query,
            "FLUSH STATISTICS",
            "ALTER SESSION SET PROFILE = 'OFF'",
            _build_profile_select(query),
        ]
        return self.connection.execute_query(statements, snapshot=False).fetchall()

    async def execute_write_query(self, query: QueryArg, ctx: Context) -> str | None:
        if not self.config.enable_write_query:
            raise RuntimeError(
                "The execution of Data Definition and "
                "Data Manipulation queries is disabled."
            )

        if self.config.disable_elicitation:
            self.connection.execute_query(query, snapshot=False)
            return None

        class QueryElicitation(BaseModel):
            sql: str = Field(default=query)

        confirmation = await ctx.elicit(
            message=(
                "The following Data Definition or Data Manipulation query will be "
                "executed if permitted. Please review the query carefully to ensure "
                "it will not cause unintended changes in the data. Modify the query "
                "if need. Finally, accept or decline the query execution."
            ),
            response_type=QueryElicitation,
        )
        if confirmation.action == "accept":
            accepted_query = confirmation.data.sql
            self.connection.execute_query(accepted_query, snapshot=False)
            if accepted_query != query:
                return accepted_query
            return None
        elif confirmation.action == "reject":
            raise InterruptedError("The query execution is declined by the user.")
        else:  # cancel
            raise InterruptedError("The query execution is cancelled by the user.")

    def list_sql_types(self) -> list[SQLTypeInfo]:
        query = ExasolMetaQuery.get_sql_types()
        return self._execute_meta_query(query, SQLTypeInfo)

    def _list_system_tables(self, info_type: SysInfoType) -> list[str]:
        query = self.meta_query.get_system_tables(info_type.value)
        result = self._execute_meta_query(query, QualifiedDBObject)
        # To save the token space, we only return the names
        return [obj.name for obj in result]

    def list_system_tables(self) -> list[str]:
        return self._list_system_tables(SysInfoType.SYSTEM)

    def list_statistics_tables(self) -> list[str]:
        return self._list_system_tables(SysInfoType.STATISTICS)

    def describe_system_table(self, table_name: TableNameArg) -> DBTable:
        return self.describe_table(SysInfoType.SYSTEM.value, table_name)

    def describe_statistics_table(self, table_name: TableNameArg) -> DBTable:
        return self.describe_table(SysInfoType.STATISTICS.value, table_name)

    def list_keywords(
        self,
        reserved: Annotated[
            bool,
            Field(
                description=(
                    "If set to True, the tool selects reserved keywords, "
                    "otherwise non-reserved keywords."
                )
            ),
        ],
        letter: Annotated[
            str, Field(description="selects keywords starting from this letter")
        ],
    ) -> list[str]:
        query = ExasolMetaQuery.get_keywords(reserved, letter)
        return self.connection.execute_query(query).fetchcol()

    def health_check(self) -> JSONResponse:
        """
        A simple health check, runs a trivial query to verify that the DB is accessible.
        """
        try:
            result = self.connection.execute_query("SELECT 1", no_auth=True).fetchval()
            if result == 1:
                return JSONResponse(
                    {"status": "healthy", "service": "exasol-mcp-server"}
                )
        except Exception:  # pylint: disable=broad-except
            pass  # nosec
        return JSONResponse({"status": "unhealthy", "service": "exasol-mcp-server"})
