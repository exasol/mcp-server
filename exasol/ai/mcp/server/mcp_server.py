import json
import os
import traceback
from textwrap import dedent
from typing import Annotated

from fastmcp import FastMCP
from mcp.types import TextContent
from pydantic import Field
from pyexasol import (
    ExaConnection,
    connect,
)

from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaSettings,
)
from exasol.ai.mcp.server.utils import report_error


def _where_clause(*predicates) -> str:
    condition = " AND ".join(filter(bool, predicates))
    if condition:
        return f"WHERE {condition}"
    return ""


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
                predicates.append(f"{meta_name}_SCHEMA = '{schema_name}'")
        return dedent(
            f"""
            SELECT {meta_name}_NAME AS "{conf.name_field}", {meta_name}_COMMENT AS "{conf.comment_field}"
            FROM SYS.EXA_ALL_{meta_name}S
            {_where_clause(*predicates)}
        """
        )

    def _execute_query(
        self, tool_name: str, query: str, use_snapshot: bool = True
    ) -> TextContent:
        """
        Executes the specified query and returns the result data in the form of json
        wrapped in a TextContent. For example
        TextContent(
            type='text',
            text='[{"name": "MY_TABLE1", "comment": "first table"}, {"name": "MY_TABLE2", "comment": "second table"}]'
        )
        If an error occurs, returns
        TextContent(
            type='text',
            text='{"error": "<error stack>"}'
        )

        By default, executes a lock-free request using the pyexasol
        `meta.execute_snapshot` method. This is a recommended way of running a metadata
        query. For a normal query the `use_snapshot` should be set to `False`.
        """
        try:
            if use_snapshot:
                result = self.connection.meta.execute_snapshot(query=query).fetchall()
            else:
                result = self.connection.execute(query=query).fetchall()
            result_json = json.dumps(result)
            return TextContent(type="text", text=result_json)
        except Exception:  # pylint: disable=broad-exception-caught
            return report_error(tool_name, traceback.format_exc())

    def list_schemas(self) -> TextContent:
        tool_name = self.list_functions.__name__
        conf = self.config.schemas
        query = self._build_meta_query("SCHEMA", conf, "")
        return self._execute_query(tool_name, query)

    def list_tables(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
    ) -> TextContent:
        tool_name = self.list_tables.__name__
        table_conf = self.config.tables
        view_conf = self.config.views

        query = "\nUNION\n".join(
            self._build_meta_query(meta_name, conf, schema_name)
            for meta_name, conf in zip(["TABLE", "VIEW"], [table_conf, view_conf])
            if conf.enable
        )
        return self._execute_query(tool_name, query)

    def list_functions(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
    ) -> TextContent:
        tool_name = self.list_functions.__name__
        conf = self.config.functions
        if not conf.enable:
            return report_error(tool_name, "Function listing is disabled.")

        query = self._build_meta_query("FUNCTION", conf, schema_name)
        return self._execute_query(tool_name, query)

    def list_scripts(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
    ) -> TextContent:
        tool_name = self.list_scripts.__name__
        conf = self.config.scripts
        if not conf.enable:
            return report_error(tool_name, "Script listing is disabled.")

        query = self._build_meta_query(
            "SCRIPT", conf, schema_name, "SCRIPT_TYPE = 'UDF'"
        )
        return self._execute_query(tool_name, query)

    def describe_table(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
        table_name: Annotated[str, Field(description="name of the table", default="")],
    ) -> TextContent:
        tool_name = self.describe_table.__name__
        conf = self.config.columns
        if not conf.enable:
            return report_error(tool_name, "Column listing is disabled.")
        schema_name = schema_name or self.connection.current_schema()
        if not schema_name:
            return report_error(tool_name, "Schema name is not provided.")
        if not table_name:
            return report_error(tool_name, "Table name is not provided.")

        c_predicates = [
            f"COLUMN_SCHEMA = '{schema_name}'",
            f"COLUMN_TABLE = '{table_name}'",
        ]
        s_predicates = [
            f"CONSTRAINT_SCHEMA = '{schema_name}'",
            f"CONSTRAINT_TABLE = '{table_name}'",
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
        return self._execute_query(tool_name, query)


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
