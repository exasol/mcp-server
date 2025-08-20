import json
import os
import re
from functools import cache
from textwrap import dedent
from typing import (
    Annotated,
    Any,
)

import pyexasol
from fastmcp import FastMCP
from pydantic import (
    Field,
    ValidationError,
)
from sqlglot import (
    exp,
    parse_one,
)
from sqlglot.errors import ParseError

from exasol.ai.mcp.server.parameter_parser import (
    FuncParameterParser,
    ScriptParameterParser,
)
from exasol.ai.mcp.server.parameter_pattern import (
    parameter_list_pattern,
    regex_flags,
)
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
    MetaListSettings,
)
from exasol.ai.mcp.server.utils import (
    keyword_filter,
    sql_text_value,
)

ENV_SETTINGS = "EXA_MCP_SETTINGS"
ENV_DSN = "EXA_DSN"
ENV_USER = "EXA_USER"
ENV_PASSWORD = "EXA_PASSWORD"

TABLE_USAGE = (
    "In an SQL query, the names of database objects, such as schemas, "
    "tables and columns should be enclosed in double quotes. "
    "A reference to a table should include a reference to its schema. "
    "The SELECT column list cannot have both the * and explicit column names."
)

SCHEMA_NAME_TYPE = Annotated[str, Field(description="name of the database schema")]

OPTIONAL_SCHEMA_NAME_TYPE = Annotated[
    str | None, Field(description="optional name of the database schema", default="")
]

KEYWORDS_TYPE = Annotated[list[str], "list of keywords to filter and order the result"]

TABLE_NAME_TYPE = Annotated[str, "name of the table"]

FUNCTION_NAME_TYPE = Annotated[str, "name of the function"]

SCRIPT_NAME_TYPE = Annotated[str, "name of the script"]


def _where_clause(*predicates) -> str:
    condition = " AND ".join(filter(bool, predicates))
    if condition:
        return f"WHERE {condition}"
    return ""


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


class ExasolMCPServer(FastMCP):
    """
    An Exasol MCP server based on FastMCP.

    Args:
        connection:
            pyexasol connection object. Note: the connection should be created
            with `fetch_dict`=True. The server sets this option to True anyway.
        config:
            The server configuration.
    """

    def __init__(
        self, connection: pyexasol.ExaConnection, config: McpServerSettings
    ) -> None:
        super().__init__(name="exasol-mcp")
        self.connection = connection
        self.connection.options["fetch_dict"] = True
        self.config = config
        self._register_tools()

    def _register_tools(self):
        if self.config.schemas.enable:
            self.tool(
                self.list_schemas,
                description=(
                    "The tool lists schemas in the Exasol Database. "
                    "For each schema, it provides the name and an optional comment."
                ),
            )
            self.tool(
                self.find_schemas,
                description=(
                    "The tool finds schemas in the Exasol Database by looking for the "
                    "specified keywords in their names and comments. "
                    "For each schema it finds, it provides the name and an optional "
                    "comment."
                ),
            )
        if self.config.tables.enable or self.config.views.enable:
            self.tool(
                self.list_tables,
                description=(
                    "The tool lists tables and views in the specified schema of the "
                    "the Exasol Database. For each table and view, it provides the "
                    "name and an optional comment."
                ),
            )
            self.tool(
                self.find_tables,
                description=(
                    "The tool finds tables and views in the Exasol Database by looking "
                    "for the specified keywords in their names and comments. "
                    "For each table or view it finds, it provides the name and an "
                    "optional comment. An optional `schema_name` argument allows "
                    "restricting the search to tables and views in the specified schema."
                ),
            )
        if self.config.functions.enable:
            self.tool(
                self.list_functions,
                description=(
                    "The tool lists functions in the specified schema of the Exasol "
                    "Database. For each function, it provides the name and an optional "
                    "comment."
                ),
            )
            self.tool(
                self.find_functions,
                description=(
                    "The tool finds functions in the Exasol Database by looking for "
                    "the specified keywords in their names and comments. For each "
                    "function it finds, it provides the name and an optional comment. "
                    "An optional `schema_name` argument allows restricting the search "
                    "to functions in the specified schema."
                ),
            )
        if self.config.scripts.enable:
            self.tool(
                self.list_scripts,
                description=(
                    "The tool lists the user defined functions (UDF) in the specified "
                    "schema of the Exasol Database. For each UDF, it provides the name "
                    "and an optional comment."
                ),
            )
            self.tool(
                self.find_scripts,
                description=(
                    "The tool finds the user defined functions (UDF) in the Exasol "
                    "Database by looking for the specified keywords in their names and "
                    "comments. For each UDF it finds, it provides the name and an "
                    "optional comment. An optional `schema_name` argument allows "
                    "restricting the search to UDFs in the specified schema."
                ),
            )
        if self.config.columns.enable:
            self.tool(
                self.describe_table,
                description=(
                    "The tool describes the specified table or view in the specified "
                    "schema of the Exasol Database. The description includes the list "
                    "of columns and for a table also the list of constraints. For each "
                    "column the tool provides the name, the SQL data type and an "
                    "optional comment. For each constraint it provides its type, e.g. "
                    "PRIMARY KEY, the list of columns the constraint is applied to and "
                    "an optional name. For a FOREIGN KEY it also provides the referenced "
                    "schema, table and a list of columns in the referenced table."
                ),
            )
        if self.config.parameters.enable:
            self.tool(
                self.describe_function,
                description=(
                    "The tool describes the specified function in the specified schema "
                    "of the Exasol Database. It provides the list of input parameters "
                    "and the return SQL type. For each parameter it specifies the name "
                    "and the SQL type."
                ),
            )
            self.tool(
                self.describe_script,
                description=(
                    "The tool describes the specified user defined function (UDF) in "
                    "the specified schema of the Exasol Database. It provides the "
                    "list of input parameters, the list of emitted parameters or the "
                    "SQL type of a single returned value. For each parameter it "
                    "provides the name and the SQL type. Both the input and the "
                    "emitted parameters can be dynamic or, in other words, flexible. "
                    "The dynamic parameters are indicated with ... (triple dot) string "
                    "instead of the parameter list. The description includes some usage "
                    "notes and a call example."
                ),
            )
        if self.config.enable_read_query:
            self.tool(
                self.execute_query,
                description=(
                    "The tool executes the specified query in the Exasol Database. The "
                    "query must be a SELECT statement. The tool returns data selected "
                    "by the query."
                ),
            )

    def _build_meta_query(
        self, meta_name: str, conf: MetaListSettings, schema_name: str, *predicates
    ) -> str:
        """
        Builds a metadata query.

        Args:
            meta_name:
                Must be one of "SCHEMA", "TABLE", "VIEW", "FUNCTION", or "SCRIPT".
            conf:
                Metadata type settings, which is a part of the server configuration.
            schema_name:
                An optional schema name provided in the call to the tool. Ignored if
                meta_name=='SCHEMA'. In all other cases, if the name is specified, it
                will be included in the WHERE clause. Otherwise, the query will return
                objects from all visible schemas.
            predicates:
                Any additional predicates to be used in the WHERE clause.
        """
        predicates = [conf.select_predicate, *predicates]
        if meta_name != "SCHEMA":
            schema_column = f"{meta_name}_SCHEMA"
            if schema_name:
                predicates.append(f"{schema_column} = {sql_text_value(schema_name)}")
            else:
                # Adds the schema restriction if specified in the settings.
                schema_conf = self.config.schemas
                if schema_conf.like_pattern:
                    predicates.append(
                        f"{schema_column} LIKE "
                        f"{sql_text_value(schema_conf.like_pattern)}"
                    )
                if schema_conf.regexp_pattern:
                    predicates.append(
                        f"{schema_column} REGEXP_LIKE "
                        f"{sql_text_value(schema_conf.regexp_pattern)}"
                    )
        return dedent(
            f"""
            SELECT {meta_name}_NAME AS "{conf.name_field}", {meta_name}_COMMENT AS "{conf.comment_field}"
            FROM SYS.EXA_ALL_{meta_name}S
            {_where_clause(*predicates)}
        """
        )

    def _execute_meta_query(
        self, query: str, keywords: list[str] | None = None
    ) -> ExaDbResult:
        """
        Executes a metadata query and returns the result as a list of dictionaries.
        Applies the keyword fitter if provided.
        """
        result = self.connection.meta.execute_snapshot(query=query).fetchall()
        if keywords:
            result = keyword_filter(result, keywords)
        return ExaDbResult(result)

    def _find_schemas(self, keywords: list[str] | None = None) -> ExaDbResult:
        conf = self.config.schemas
        if not conf.enable:
            raise RuntimeError("The schema listing is disabled.")

        query = self._build_meta_query("SCHEMA", conf, "")
        return self._execute_meta_query(query, keywords)

    def find_schemas(self, keywords: KEYWORDS_TYPE) -> ExaDbResult:
        return self._find_schemas(keywords)

    def list_schemas(self) -> ExaDbResult:
        return self._find_schemas()

    def _find_tables(
        self, keywords: list[str] | None = None, schema_name: str | None = None
    ) -> ExaDbResult:
        table_conf = self.config.tables
        view_conf = self.config.views
        if (not table_conf.enable) and (not view_conf.enable):
            raise RuntimeError("Both the table and the view listings are disabled.")

        query = "\nUNION\n".join(
            self._build_meta_query(meta_name, conf, schema_name)
            for meta_name, conf in zip(["TABLE", "VIEW"], [table_conf, view_conf])
            if conf.enable
        )
        return self._execute_meta_query(query, keywords)

    def find_tables(
        self, keywords: KEYWORDS_TYPE, schema_name: OPTIONAL_SCHEMA_NAME_TYPE
    ) -> ExaDbResult:
        return self._find_tables(keywords, schema_name)

    def list_tables(self, schema_name: SCHEMA_NAME_TYPE) -> ExaDbResult:
        return self._find_tables(schema_name=schema_name)

    def _find_functions(
        self, keywords: list[str] | None = None, schema_name: str | None = None
    ) -> ExaDbResult:
        conf = self.config.functions
        if not conf.enable:
            raise RuntimeError("The function listing is disabled.")

        query = self._build_meta_query("FUNCTION", conf, schema_name)
        return self._execute_meta_query(query, keywords)

    def find_functions(
        self, keywords: KEYWORDS_TYPE, schema_name: OPTIONAL_SCHEMA_NAME_TYPE
    ) -> ExaDbResult:
        return self._find_functions(keywords, schema_name)

    def list_functions(self, schema_name: SCHEMA_NAME_TYPE) -> ExaDbResult:
        return self._find_functions(schema_name=schema_name)

    def _find_scripts(
        self, keywords: list[str] | None = None, schema_name: str | None = None
    ) -> ExaDbResult:
        conf = self.config.scripts
        if not conf.enable:
            raise RuntimeError("The script listing is disabled.")

        query = self._build_meta_query(
            "SCRIPT", conf, schema_name, "SCRIPT_TYPE = 'UDF'"
        )
        return self._execute_meta_query(query, keywords)

    def find_scripts(
        self, keywords: KEYWORDS_TYPE, schema_name: OPTIONAL_SCHEMA_NAME_TYPE
    ) -> ExaDbResult:
        return self._find_scripts(keywords, schema_name)

    def list_scripts(self, schema_name: SCHEMA_NAME_TYPE) -> ExaDbResult:
        return self._find_scripts(schema_name=schema_name)

    def describe_columns(
        self, schema_name: SCHEMA_NAME_TYPE, table_name: TABLE_NAME_TYPE
    ) -> ExaDbResult:
        """
        Returns the list of columns in the given table. Currently, this is a part of
        the `describe_table` tool, but it can be used independently in the future.
        """
        conf = self.config.columns
        if not conf.enable:
            raise RuntimeError("The column listing is disabled.")

        query = dedent(
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
        return self._execute_meta_query(query)

    def describe_constraints(
        self, schema_name: SCHEMA_NAME_TYPE, table_name: TABLE_NAME_TYPE
    ) -> ExaDbResult:
        """
        Returns the list of constraints in the given table. Currently, this is a part
        of the `describe_table` tool, but it can be used independently in the future.
        """
        conf = self.config.columns
        if not conf.enable:
            raise RuntimeError("The constraint listing is disabled.")

        query = dedent(
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
        return self._execute_meta_query(query)

    def get_table_comment(self, schema_name: str, table_name: str) -> str | None:
        # `table_name` can be the name of a table or a view.
        # This query tries both possibilities. The UNION clause collapses
        # the result into a single non-NULL distinct value.
        query = dedent(
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
        comment_row = self.connection.meta.execute_snapshot(query=query).fetchone()
        if comment_row is None:
            return None
        table_comment = next(iter(comment_row.values()))
        if table_comment is None:
            return None
        return str(table_comment)

    def describe_table(
        self, schema_name: SCHEMA_NAME_TYPE, table_name: TABLE_NAME_TYPE
    ) -> dict[str, Any]:

        conf = self.config.columns
        columns = self.describe_columns(schema_name, table_name)
        constraints = self.describe_constraints(schema_name, table_name)
        table_comment = self.get_table_comment(schema_name, table_name)

        return {
            conf.columns_field: columns.result,
            conf.constraints_field: constraints.result,
            conf.table_comment_field: table_comment,
            conf.usage_field: TABLE_USAGE,
        }

    def describe_function(
        self, schema_name: SCHEMA_NAME_TYPE, func_name: FUNCTION_NAME_TYPE
    ) -> dict[str, Any]:
        parser = FuncParameterParser(
            connection=self.connection, conf=self.config.parameters
        )
        return parser.describe(schema_name, func_name)

    def describe_script(
        self, schema_name: SCHEMA_NAME_TYPE, script_name: SCRIPT_NAME_TYPE
    ) -> dict[str, Any]:
        parser = ScriptParameterParser(
            connection=self.connection, conf=self.config.parameters
        )
        return parser.describe(schema_name, script_name)

    def execute_query(
        self, query: Annotated[str, Field(description="select query")]
    ) -> ExaDbResult:
        if not self.config.enable_read_query:
            raise RuntimeError("Query execution is disabled.")
        if verify_query(query):
            result = self.connection.execute(query=query).fetchall()
            return ExaDbResult(result)
        raise ValueError("The query is invalid or not a SELECT statement.")


def get_mcp_settings() -> McpServerSettings:
    """
    Reads optional settings. They can be provided either in a json string stored in the
    EXA_MCP_SETTINGS environment variable or in a json file. In the latter case
    EXA_MCP_SETTINGS must contain the file path.
    """
    try:
        settings_text = os.environ.get(ENV_SETTINGS)
        if not settings_text:
            return McpServerSettings()
        elif re.match(r"^\s*\{.*\}\s*$", settings_text):
            return McpServerSettings.model_validate_json(settings_text)
        elif os.path.isfile(settings_text):
            with open(settings_text) as f:
                return McpServerSettings.model_validate(json.load(f))
        raise ValueError(
            "Invalid MCP Server configuration settings. The configuration "
            "environment variable should either contain a json string or "
            "point to an existing json file."
        )
    except (ValidationError, json.decoder.JSONDecodeError) as config_error:
        raise ValueError("Invalid MCP Server configuration settings.") from config_error


def main():
    """
    Main entry point that creates and runs the MCP server.
    """
    dsn = os.environ[ENV_DSN]
    user = os.environ[ENV_USER]
    password = os.environ[ENV_PASSWORD]
    mcp_settings = get_mcp_settings()

    conn = pyexasol.connect(
        dsn=dsn, user=user, password=password, fetch_dict=True, compression=True
    )

    mcp_server = ExasolMCPServer(connection=conn, config=mcp_settings)
    mcp_server.run()


if __name__ == "__main__":
    main()
