import re
from functools import cache
from typing import (
    Annotated,
    Any,
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
from exasol.ai.mcp.server.setup.server_settings import (
    ExaDbResult,
    McpServerSettings,
)
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
from exasol.ai.mcp.server.utils.keyword_search import keyword_filter

TABLE_USAGE = (
    "In an SQL query, the names of database objects, such as schemas, "
    "tables and columns should be enclosed in double quotes. "
    "A reference to a table should include a reference to its schema. "
    "The SELECT column list cannot have both the * and explicit column names."
)

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

ScriptNameArg = Annotated[str, Field(description="Name of the script")]

QueryArg = Annotated[str, Field(description="SQL Query")]


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


def _take_column(column_name: str, result: ExaDbResult) -> list[str]:
    return [row[column_name] for row in result.result]


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
        self, query: str, keywords: list[str] | None = None
    ) -> ExaDbResult:
        """
        Executes a metadata query and returns the result as a list of dictionaries.
        Applies the keyword fitter if provided.
        Removes the column with extra information that could be added to the result
        to assist filtering. This is necessary to avoid polluting the LLM's context
        with data it doesn't need at the current stage.
        """
        result = self.connection.execute_query(query).fetchall()
        if keywords:
            result = keyword_filter(result, keywords, language=self.config.language)
        return ExaDbResult(remove_info_column(result))

    def list_schemas(self) -> ExaDbResult:
        if not self.config.schemas.enable:
            raise RuntimeError("The schema listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.SCHEMA)
        return self._execute_meta_query(query)

    def find_schemas(self, keywords: KeywordsArg) -> ExaDbResult:
        if not self.config.schemas.enable:
            raise RuntimeError("The schema listing is disabled.")

        query = self.meta_query.find_schemas()
        return self._execute_meta_query(query, keywords)

    def list_tables(self, schema_name: SchemaNameArg) -> ExaDbResult:
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
        return self._execute_meta_query(query)

    def find_tables(
        self, keywords: KeywordsArg, schema_name: OptionalSchemaNameArg
    ) -> ExaDbResult:
        if (not self.config.tables.enable) and (not self.config.views.enable):
            raise RuntimeError("Both the table and the view listings are disabled.")

        query = self.meta_query.find_tables(schema_name)
        return self._execute_meta_query(query, keywords)

    def list_functions(self, schema_name: SchemaNameArg) -> ExaDbResult:
        if not self.config.functions.enable:
            raise RuntimeError("The function listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.FUNCTION, schema_name)
        return self._execute_meta_query(query)

    def find_functions(
        self, keywords: KeywordsArg, schema_name: OptionalSchemaNameArg
    ) -> ExaDbResult:
        if not self.config.functions.enable:
            raise RuntimeError("The function listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.FUNCTION, schema_name)
        return self._execute_meta_query(query, keywords)

    def list_scripts(self, schema_name: SchemaNameArg) -> ExaDbResult:
        if not self.config.scripts.enable:
            raise RuntimeError("The script listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.SCRIPT, schema_name)
        return self._execute_meta_query(query)

    def find_scripts(
        self, keywords: KeywordsArg, schema_name: OptionalSchemaNameArg
    ) -> ExaDbResult:
        if not self.config.scripts.enable:
            raise RuntimeError("The script listing is disabled.")

        query = self.meta_query.get_metadata(MetaType.SCRIPT, schema_name)
        return self._execute_meta_query(query, keywords)

    def describe_columns(
        self, schema_name: SchemaNameArg, table_name: TableNameArg
    ) -> ExaDbResult:
        """
        Returns the list of columns in the given table. Currently, this is a part of
        the `describe_table` tool, but it can be used independently in the future.
        """
        if not self.config.columns.enable:
            raise RuntimeError("The column listing is disabled.")

        query = self.meta_query.describe_columns(schema_name, table_name)
        return self._execute_meta_query(query)

    def describe_constraints(
        self, schema_name: SchemaNameArg, table_name: TableNameArg
    ) -> ExaDbResult:
        """
        Returns the list of constraints in the given table. Currently, this is a part
        of the `describe_table` tool, but it can be used independently in the future.
        """
        if not self.config.columns.enable:
            raise RuntimeError("The constraint listing is disabled.")

        query = self.meta_query.describe_constraints(schema_name, table_name)
        return self._execute_meta_query(query)

    def get_table_comment(
        self, schema_name: SchemaNameArg, table_name: TableNameArg
    ) -> str | None:
        query = self.meta_query.get_table_comment(schema_name, table_name)
        comment_row = self.connection.execute_query(query).fetchone()
        if comment_row is None:
            return None
        table_comment = next(iter(comment_row.values()))
        if table_comment is None:
            return None
        return str(table_comment)

    def describe_table(
        self, schema_name: SchemaNameArg, table_name: TableNameArg
    ) -> dict[str, Any]:

        conf = self.config.columns
        columns = self.describe_columns(schema_name, table_name)
        result = {
            conf.columns_field: columns.result,
            conf.usage_field: TABLE_USAGE,
        }

        # There is no constraints for a system table. The comment is also not needed,
        # as it should be in the resource, where the name of the system table came from.
        if not is_system_schema(schema_name):
            constraints = self.describe_constraints(schema_name, table_name)
            table_comment = self.get_table_comment(schema_name, table_name)
            result[conf.constraints_field] = constraints.result
            result[conf.table_comment_field] = table_comment

        return result

    def describe_function(
        self, schema_name: SchemaNameArg, func_name: FunctionNameArg
    ) -> dict[str, Any]:
        parser = FuncParameterParser(connection=self.connection, settings=self.config)
        return parser.describe(schema_name, func_name)

    def describe_script(
        self, schema_name: SchemaNameArg, script_name: ScriptNameArg
    ) -> dict[str, Any]:
        parser = ScriptParameterParser(connection=self.connection, settings=self.config)
        return parser.describe(schema_name, script_name)

    def execute_query(self, query: QueryArg) -> ExaDbResult:
        if not self.config.enable_read_query:
            raise RuntimeError("Query execution is disabled.")
        if verify_query(query):
            result = self.connection.execute_query(query, snapshot=False).fetchall()
            return ExaDbResult(result)
        raise ValueError("The query is invalid or not a SELECT statement.")

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

    def list_sql_types(self) -> ExaDbResult:
        query = ExasolMetaQuery.get_sql_types()
        return self._execute_meta_query(query)

    def _list_system_tables(self, info_type: SysInfoType) -> list[str]:
        query = self.meta_query.get_system_tables(info_type)
        return _take_column(
            self.config.tables.name_field, self._execute_meta_query(query)
        )

    def list_system_tables(self) -> list[str]:
        return self._list_system_tables(SysInfoType.SYSTEM)

    def list_statistics_tables(self) -> list[str]:
        return self._list_system_tables(SysInfoType.STATISTICS)

    def _describe_system_table(
        self, info_type: SysInfoType, table_name: str
    ) -> ExaDbResult:
        query = self.meta_query.get_system_tables(info_type, table_name)
        return self._execute_meta_query(query)

    def describe_system_table(self, table_name: TableNameArg) -> ExaDbResult:
        return self._describe_system_table(SysInfoType.SYSTEM, table_name)

    def describe_statistics_table(self, table_name: TableNameArg) -> ExaDbResult:
        return self._describe_system_table(SysInfoType.STATISTICS, table_name)

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
        return _take_column("KEYWORD", self._execute_meta_query(query))

    def health_check(self) -> JSONResponse:
        """
        A simple health check, runs a trivial query to verify that the DB is accessible.
        """
        try:
            result = self.connection.execute_query("SELECT 1").fetchval()
            if result == 1:
                return JSONResponse(
                    {"status": "healthy", "service": "exasol-mcp-server"}
                )
        except Exception:  # pylint: disable=broad-except
            pass  # nosec
        return JSONResponse({"status": "unhealthy", "service": "exasol-mcp-server"})
