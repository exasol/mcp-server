import json
import logging
import os
import re
from textwrap import dedent
from typing import (
    Annotated,
    Any,
)

from fastmcp import FastMCP
from mcp.types import TextContent
from pydantic import (
    BaseModel,
    Field,
)
from pyexasol import (
    ExaConnection,
    connect,
)

logger = logging.getLogger("exasol_mcp_server")


class MetaSettings(BaseModel):
    """
    The settings for a single type of metadata, e.g. tables.
    """

    enable: bool = True
    """
    Allows to disable the listing of a particular type of metadata.
    """

    name_field: str = "name"
    """
    The name of the output field that contains the object name, e.g. "table_name".
    """

    comment_field: str = "comment"
    """
    The name of the output field that contains the comment, e.g. "table_comment".
    """

    like_pattern: str | None = None
    """
    An optional sql-style pattern for the object name filtering.

    Use case example: The user wants to create a set of purified de-normalised views on
    the existing database and limit the table listings to only these views. One way of
    achieving this is to create the views in a new schema and limit the listing of the
    schemas to this schema only. In the case of no permission to create schema, one can
    create the views in an existing schema and use some prefix for name disambiguation.
    This prefix can also be used for filtering the views in the listing.
    """
    regexp_pattern: str | None = None
    """
    An optional regular expression pattern for the object name filtering.
    Both like_pattern and regexp_pattern can be used at the same time, although there is
    not much point in doing so.
    """

    @property
    def select_predicate(self) -> str:
        """
        The SQL predicate for the object filtering by name.
        Empty string if neigher of the filtering patterns are defined.
        """
        conditions: list[str] = []
        if self.like_pattern:
            conditions.append(
                f"""local."{self.name_field}" LIKE '{self.like_pattern}'"""
            )
        if self.regexp_pattern:
            conditions.append(
                f"""local."{self.name_field}" REGEXP_LIKE '{self.regexp_pattern}'"""
            )
        return " AND ".join(conditions)


class MetaColumnSettings(MetaSettings):
    """
    The settings for listing columns when describing a table. Adds few more fields to
    the metadata output.
    """

    type_field: str = "column_type"
    primary_key_field: str = "primary_key"
    foreign_key_field: str = "foreign_key"


class MetaParameterSettings(MetaSettings):
    """
    The settings for listing input/output parameters when describing a function of a
    script.
    """

    type_field: str = "parameter_type"
    input_field: str = "inputs"
    return_field: str = "returns"
    emit_field: str = "emits"


class McpServerSettings(BaseModel):
    """
    MCP server configuration.
    """

    schemas: MetaSettings = MetaSettings(
        name_field="schema_name", comment_field="schema_comment"
    )
    tables: MetaSettings = MetaSettings(
        name_field="table_name", comment_field="table_comment"
    )
    views: MetaSettings = MetaSettings(
        enable=False, name_field="table_name", comment_field="table_comment"
    )
    functions: MetaSettings = MetaSettings(
        name_field="function_name", comment_field="function_comment"
    )
    scripts: MetaSettings = MetaSettings(
        name_field="script_name", comment_field="script_comment"
    )
    columns: MetaColumnSettings = MetaColumnSettings(
        name_field="column_name", comment_field="column_comment"
    )
    parameters: MetaParameterSettings = MetaParameterSettings(
        name_field="parameter_name"
    )


def _report_error(tool_name: str, error_message: str) -> TextContent:
    logger.error("Error in %s: %s", tool_name, error_message)
    error_json = json.dumps({"error": error_message})
    return TextContent(type="text", text=error_json)


def _where_clause(*predicates) -> str:
    condition = " AND ".join(filter(bool, predicates))
    if condition:
        return f"WHERE {condition}"
    return ""


def parse_parameter_list(
    params: str, conf: MetaParameterSettings
) -> str | list[dict[str, str]]:
    """
    Breaks the input string into parameter definitions. The input string should be
    extracted from a text of a function or a script and contain a list of parameters,
    where each parameter consists of a name and a type. The name may or may not be
    enclosed in double quotes. The list can be either an input or, in case of an EMIT
    UDF, the emit list.

    The parameters of a UDF script can be variadic, designated with "...". In such
    case, the function simply returns "...".

    Normally, the function returns a list of dictionaries, where each dictionary
    describes a parameter and includes its name and type. The dictionary keys are
    defined by the provided configuration. The double quotes in the parameter name
    gets removed.
    """

    def remove_double_quotes(di: dict[str, str]) -> dict[str, str]:
        return {key: val.strip('"') for key, val in di.items()}

    params = params.lstrip()
    if params == "...":
        return params
    type_pattern = r"(?:\s*\w+(?:\s*\([\s\w,]*\))?)+"
    pattern = (
        rf'(?:^|,)\s*(?P<{conf.name_field}>\w+|"\w+")'
        rf"\s+(?P<{conf.type_field}>{type_pattern})"
    )
    return [
        remove_double_quotes(m.groupdict())
        for m in re.finditer(pattern, params, re.IGNORECASE)
    ]


def format_return_type(param: str, conf: MetaParameterSettings) -> str | dict[str, str]:
    return {conf.type_field: param}


def extract_script_parameters(
    script_info: dict[str, Any], conf: MetaParameterSettings
) -> dict[str, Any] | None:
    """
    Parses a UDF script, extracting its input and output parameters and returning a
    json. If the script text cannot be parsed, return None. Both input and output
    parameters are returned as lists even when it's meant to be a single parameter.

    Args:
        script_info:
            The script information obtained from reading the SYS.EXA_ALL_SCRIPTS table.
            This is a row passed as a dictionary {column_name: column_value}.
        conf:
            The configuration defining the output field names.
    """
    main_pattern = (
        rf'\A\s*CREATE\s+{script_info["SCRIPT_LANGUAGE"]}\s+'
        rf'{script_info["SCRIPT_INPUT_TYPE"]}\s+SCRIPT\s+'
        rf'"{script_info["SCRIPT_NAME"]}"\s*\((?P<{conf.input_field}>.*)\)\s*'
        rf'{script_info["SCRIPT_RESULT_TYPE"]}'
    )
    if script_info["SCRIPT_RESULT_TYPE"] == "EMITS":
        pattern = rf"{main_pattern}\s*\((?P<{conf.emit_field}>.*)\)\s*AS\s+"
    else:
        pattern = rf"{main_pattern}\s+(?P<{conf.return_field}>.*)\s+AS\s+"
    m = re.match(pattern, script_info["SCRIPT_TEXT"], re.IGNORECASE)
    if m is None:
        return None
    param_func = {
        conf.input_field: parse_parameter_list,
        conf.emit_field: parse_parameter_list,
        conf.return_field: format_return_type,
    }
    return {
        param_field: param_func[param_field](params, conf)
        for param_field, params in m.groupdict().items()
        if params
    }


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
                    "For each shema provides the name and an optional comment."
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
                self.describe_script,
                description=(
                    "Describes the specified script in the specified schema of the "
                    "Exasol Database. Returns the list of the input and output "
                    "parameters. For each parameter provides the name and the type."
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
        except Exception as e:  # pylint: disable=broad-exception-caught
            return _report_error(tool_name, str(e))

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
            return _report_error(tool_name, "Function listing is disabled.")

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
            return _report_error(tool_name, "Script listing is disabled.")

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
            return _report_error(tool_name, "Column listing is disabled.")
        schema_name = schema_name or self.connection.current_schema()
        if not schema_name:
            return _report_error(tool_name, "Schema name is not provided.")
        if not table_name:
            return _report_error(tool_name, "Table name is not provided.")

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

    def describe_script(
        self,
        schema_name: Annotated[
            str, Field(description="name of the database schema", default="")
        ],
        script_name: Annotated[
            str, Field(description="name of the script", default="")
        ],
    ) -> TextContent:
        tool_name = self.describe_script.__name__
        conf = self.config.parameters
        if not conf.enable:
            return _report_error(tool_name, "Parameter listing is disabled.")
        schema_name = schema_name or self.connection.current_schema()
        if not schema_name:
            return _report_error(tool_name, "Schema name is not provided.")
        if not script_name:
            return _report_error(tool_name, "Script name is not provided.")

        predicates = [
            f"SCRIPT_SCHEMA = '{schema_name}'",
            f"SCRIPT_NAME = '{script_name}'",
        ]
        query = f"SELECT * FROM SYS.EXA_ALL_SCRIPTS {_where_clause(*predicates)}"
        try:
            result = self.connection.meta.execute_snapshot(query=query).fetchall()
            if result:
                if len(result) > 1:
                    return _report_error(tool_name, "Script metadata is ambiguous.")
                script_info = result[0]
                result = extract_script_parameters(script_info, conf)
                if result is not None:
                    result_json = json.dumps(result)
                    return TextContent(type="text", text=result_json)
            return _report_error(tool_name, "Failed to get the script metadata.")
        except Exception as e:  # pylint: disable=broad-exception-caught
            return _report_error(tool_name, str(e))


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
