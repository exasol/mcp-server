import json
import os
from textwrap import dedent
from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field
from pyexasol import (
    ExaConnection,
    connect,
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
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
    MetaSettings,
)
from exasol.ai.mcp.server.utils import sql_text_value


def _where_clause(*predicates) -> str:
    condition = " AND ".join(filter(bool, predicates))
    if condition:
        return f"WHERE {condition}"
    return ""


def vet_query(query: str) -> bool:
    """
    Verifies that the query is a valid SELECT query.
    Declines any other types of statements including the SELECT INTO.
    """
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

    def __init__(self, connection: ExaConnection, config: McpServerSettings) -> None:
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
                    "Lists schemas in the Exasol Database. "
                    "For each schema provides the name and an optional comment."
                ),
            )
        if self.config.tables.enable or self.config.views.enable:
            self.tool(
                self.list_tables,
                description=(
                    "Lists tables in the specified schema of the Exasol Database. "
                    "For each table provides the name and an optional comment."
                ),
            )
        if self.config.functions.enable:
            self.tool(
                self.list_functions,
                description=(
                    "Lists functions in the specified schema of the Exasol Database. "
                    "For each function provides the name and an optional comment."
                ),
            )
        if self.config.scripts.enable:
            self.tool(
                self.list_scripts,
                description=(
                    "Lists the user defined functions (UDF) in the specified schema of "
                    "the Exasol Database. For each function provides the name and an "
                    "optional comment."
                ),
            )
        if self.config.columns.enable:
            self.tool(
                self.describe_table,
                description=(
                    "Describes the specified table in the specified schema of the "
                    "Exasol Database. Returns the list of table columns. For each "
                    "column provides the name, the data type, an optional comment, "
                    "the primary key flag and the foreign key flag."
                ),
            )
        if self.config.parameters.enable:
            self.tool(
                self.describe_function,
                description=(
                    "Describes the specified function in the specified schema of the "
                    "Exasol Database. Returns the list of input parameters and the "
                    "return type. For each parameter provides the name and the type."
                ),
            )
            self.tool(
                self.describe_script,
                description=(
                    "Describes the specified user defined function in the specified "
                    "schema of the Exasol Database. Returns the list of input "
                    "parameters, the list or emitted parameters or the SQL type of a "
                    "single returned value. For each parameter provides the name and "
                    "the SQL type. Both the input and the emitted parameters can by "
                    "dynamic, or, in other words, flexible. The dynamic parameters are "
                    "indicated with ... (triple dot) string instead of the parameter "
                    "list. A user defined function with dynamic input parameters can "
                    "be called using the same syntax as a normal function. If the "
                    "function output is emitted dynamically, the list of output "
                    "parameters must be provided in the call. This can be achieved by "
                    "appending the select statement with the special term "
                    "emits"
                    " "
                    "followed by the parameter list in the brackets."
                ),
            )
        if self.config.enable_query:
            self.tool(
                self.execute_query,
                description=(
                    "Executes the specified query in the specified schema of the "
                    "Exasol Database. The query must be a SELECT statement. Returns "
                    "the results selected by the query."
                ),
            )

    def _build_meta_query(
        self, meta_name: str, conf: MetaSettings, schema_name: str, *predicates
    ) -> str:
        """
        Builds a metadata query.

        Args:
            meta_name:
                Must be one of "SCHEMA", "TABLE", "VIEW", "FUNCTION", or "SCRIPT".
            conf:
                Metadata type settings, which is a part of the server configuration.
            schema_name:
                The schema name provided in the call to the tool. Ignored if meta_name
                =='SCHEMA'. Otherwise, if it's empty the current schema of the pyexasol
                connection may be used. If there is no current schema, the query will
                return objects from all schemas.
            predicates:
                Any additional predicates to be used in the WHERE clause.
        """
        predicates = [conf.select_predicate, *predicates]
        if meta_name != "SCHEMA":
            schema_name = schema_name or self.connection.current_schema()
            if schema_name:
                predicates.append(f"{meta_name}_SCHEMA = {sql_text_value(schema_name)}")
        return dedent(
            f"""
            SELECT {meta_name}_NAME AS "{conf.name_field}", {meta_name}_COMMENT AS "{conf.comment_field}"
            FROM SYS.EXA_ALL_{meta_name}S
            {_where_clause(*predicates)}
        """
        )

    def _execute_meta_query(self, query: str) -> ExaDbResult:
        result = self.connection.meta.execute_snapshot(query=query).fetchall()
        return ExaDbResult(result)

    def list_schemas(self) -> ExaDbResult:
        conf = self.config.schemas
        if not conf.enable:
            raise RuntimeError("Schema listing is disabled.")

        query = self._build_meta_query("SCHEMA", conf, "")
        return self._execute_meta_query(query)

    def list_tables(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
    ) -> ExaDbResult:
        table_conf = self.config.tables
        view_conf = self.config.views
        if (not table_conf.enable) and (not view_conf.enable):
            raise RuntimeError("Both table and view listings are disabled.")

        query = "\nUNION\n".join(
            self._build_meta_query(meta_name, conf, schema_name)
            for meta_name, conf in zip(["TABLE", "VIEW"], [table_conf, view_conf])
            if conf.enable
        )
        return self._execute_meta_query(query)

    def list_functions(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
    ) -> ExaDbResult:
        conf = self.config.functions
        if not conf.enable:
            raise RuntimeError("Function listing is disabled.")

        query = self._build_meta_query("FUNCTION", conf, schema_name)
        return self._execute_meta_query(query)

    def list_scripts(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
    ) -> ExaDbResult:
        conf = self.config.scripts
        if not conf.enable:
            raise RuntimeError("Script listing is disabled.")

        query = self._build_meta_query(
            "SCRIPT", conf, schema_name, "SCRIPT_TYPE = 'UDF'"
        )
        return self._execute_meta_query(query)

    def describe_table(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
        table_name: Annotated[str, Field(description="name of the table", default="")],
    ) -> ExaDbResult:
        conf = self.config.columns
        if not conf.enable:
            raise RuntimeError("Column listing is disabled.")
        schema_name = schema_name or self.connection.current_schema()
        if not schema_name:
            raise ValueError("Schema name is not provided.")
        if not table_name:
            raise ValueError("Table name is not provided.")

        c_predicates = [
            f"COLUMN_SCHEMA = {sql_text_value(schema_name)}",
            f"COLUMN_TABLE = {sql_text_value(table_name)}",
        ]
        s_predicates = [
            f"CONSTRAINT_SCHEMA = {sql_text_value(schema_name)}",
            f"CONSTRAINT_TABLE = {sql_text_value(table_name)}",
        ]
        query = dedent(
            f"""
            SELECT
                    C.COLUMN_NAME AS "{conf.name_field}",
                    C.COLUMN_TYPE AS "{conf.type_field}",
                    C.COLUMN_COMMENT AS "{conf.comment_field}",
                    NVL2(P.COLUMN_NAME, TRUE, FALSE) AS "{conf.primary_key_field}",
                    NVL2(F.COLUMN_NAME, TRUE, FALSE) AS "{conf.foreign_key_field}"
            FROM SYS.EXA_ALL_COLUMNS C
            LEFT JOIN (
                    SELECT COLUMN_NAME
                    FROM SYS.EXA_ALL_CONSTRAINT_COLUMNS
                    {_where_clause(*s_predicates, "CONSTRAINT_TYPE = 'PRIMARY KEY'")}
            ) P ON P.COLUMN_NAME=C.COLUMN_NAME
            LEFT JOIN (
                    SELECT COLUMN_NAME
                    FROM SYS.EXA_ALL_CONSTRAINT_COLUMNS
                    {_where_clause(*s_predicates, "CONSTRAINT_TYPE = 'FOREIGN KEY'")}
            ) F ON F.COLUMN_NAME=C.COLUMN_NAME
            {_where_clause(*c_predicates, conf.select_predicate)}
        """
        )
        return self._execute_meta_query(query)

    def describe_function(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
        func_name: Annotated[
            str, Field(description="name of the function", default="")
        ],
    ) -> ExaDbResult:
        parser = FuncParameterParser(
            connection=self.connection,
            conf=self.config.parameters,
            tool_name=self.describe_function.__name__,
        )
        return parser.describe(schema_name, func_name)

    def describe_script(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
        script_name: Annotated[
            str, Field(description="name of the script", default="")
        ],
    ) -> ExaDbResult:
        parser = ScriptParameterParser(
            connection=self.connection,
            conf=self.config.parameters,
            tool_name=self.describe_script.__name__,
        )
        return parser.describe(schema_name, script_name)

    def execute_query(
        self, query: Annotated[str, Field(description="select query")]
    ) -> ExaDbResult:
        if vet_query(query):
            result = self.connection.execute(query=query).fetchall()
            return ExaDbResult(result)
        raise ValueError("The query is invalid or not a SELECT statement.")


if __name__ == "__main__":

    # For now, expect the DB connection parameters to be stored in the environment.
    dsn = os.environ["EXA_DSN"]
    user = os.environ["EXA_USER"]
    password = os.environ["EXA_PASSWORD"]
    mcp_settings = json.loads(os.environ.get("EXA_MCP_SETTINGS", "{}"))

    conn = connect(
        dsn=dsn, user=user, password=password, fetch_dict=True, compression=True
    )

    mcp_server = ExasolMCPServer(conn, **mcp_settings)
    mcp_server.run()
