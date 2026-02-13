import json
import logging
import os
import re
from logging.handlers import RotatingFileHandler
from typing import Any

import click
import exasol.bucketfs as bfs
from mcp.types import ToolAnnotations
from pydantic import ValidationError

import exasol.ai.mcp.server.connection.connection_factory as cf
from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.setup.generic_auth import (
    get_auth_kwargs,
    str_to_bool,
)
from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
)
from exasol.ai.mcp.server.tools.dialect_tools import (
    builtin_function_categories,
    describe_builtin_function,
    list_builtin_functions,
)
from exasol.ai.mcp.server.tools.mcp_server import ExasolMCPServer

ENV_SETTINGS = "EXA_MCP_SETTINGS"
""" MCP server settings json or a name of a json file with the settings """

ENV_LOG_FILE = "EXA_MCP_LOG_FILE"
ENV_LOG_LEVEL = "EXA_MCP_LOG_LEVEL"
ENV_LOG_MAX_SIZE = "EXA_MCP_LOG_MAX_SIZE"
ENV_LOG_BACKUP_COUNT = "EXA_MCP_LOG_BACKUP_COUNT"
ENV_LOG_FORMATTER = "EXA_MCP_LOG_FORMATTER"
ENV_LOG_TO_CONSOLE = "EXA_MCP_LOG_TO_CONSOLE"

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_MAX_SIZE = 1048576  # 1 MB
DEFAULT_LOG_BACKUP_COUNT = 5


def _register_list_schemas(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_schemas,
        description=(
            "The tool lists schemas in the Exasol Database. "
            "For each schema, it provides the name and an optional comment."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_schemas(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_schemas,
        description=(
            "The tool finds schemas in the Exasol Database by looking for the "
            "specified keywords in their names and comments. The list of keywords "
            "should include common inflections of each keyword. "
            "For each schema it finds, it provides the name and an optional comment."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_list_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_tables,
        description=(
            "The tool lists tables and views in the specified schema of the "
            "the Exasol Database. For each table and view, it provides the "
            "name, the schema, and an optional comment."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_tables,
        description=(
            "The tool finds tables and views in the Exasol Database by looking "
            "for the specified keywords in their names and comments. The list of "
            "keywords should include common inflections of each keyword. "
            "For each table or view the tool finds, it provides the name, the schema, "
            "and an optional comment. An optional `schema_name` argument allows "
            "restricting the search to tables and views in the specified schema."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_list_functions(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_functions,
        description=(
            "The tool lists functions in the specified schema of the Exasol "
            "Database. For each function, it provides the name, the schema, "
            "and an optional comment."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_functions(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_functions,
        description=(
            "The tool finds functions in the Exasol Database by looking for "
            "the specified keywords in their names and comments. The list of "
            "keywords should include common inflections of each keyword. "
            "For each function the tool finds, it provides the name, the schema,"
            "and an optional comment. An optional `schema_name` argument allows "
            "restricting the search to functions in the specified schema."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_list_scripts(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_scripts,
        description=(
            "The tool lists the user defined functions (UDF) in the specified "
            "schema of the Exasol Database. For each UDF, it provides the name, "
            "the schema, and an optional comment."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_scripts(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_scripts,
        description=(
            "The tool finds the user defined functions (UDF) in the Exasol Database "
            "by looking for the specified keywords in their names and comments. The "
            "list of keywords should include common inflections of each keyword. "
            "For each UDF the tool finds, it provides the name, the schema, and an "
            "optional comment. An optional `schema_name` argument allows restricting "
            "the search to UDFs in the specified schema."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_describe_table(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_table,
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
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_describe_function(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_function,
        description=(
            "The tool describes the specified function in the specified schema "
            "of the Exasol Database. It provides the list of input parameters "
            "and the return SQL type. For each parameter it specifies the name "
            "and the SQL type."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_describe_script(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_script,
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
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_execute_query(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.execute_query,
        description=(
            "The tool executes the specified query in the Exasol Database. The "
            "query must be a SELECT statement. The tool returns data selected "
            "by the query."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_execute_write_query(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.execute_write_query,
        description=(
            "The tool executes the specified DML or DDL query in the Exasol Database. "
            "The user can alter the query through elicitation. If the query was "
            "executed it its original form, the tool returns None. Otherwise, the "
            "tool returns a modified query."
        ),
        annotations=ToolAnnotations(destructiveHint=True),
    )


def _register_list_directories(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.list_directories,
            description=(
                "Lists subdirectories at the given directory of the BucketFS file "
                "system."
            ),
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_list_files(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.list_files,
            description="Lists files at the given directory of the BucketFS.",
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_find_files(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.find_files,
            description=(
                "Performs a keyword search of files in the BucketFS. The list "
                "of keywords should include common inflections of each keyword. "
                "Files are searched in the given directory and all its descendant "
                "subdirectories."
            ),
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_read_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.read_file,
            description="Reads the content of a text file in the BucketFS.",
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_write_text_to_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.write_text_to_file,
            description="Writes the provided text to a file in the BucketFS.",
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_download_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.download_file,
            description=(
                "Downloads a file from a given url and saves it at the specified path "
                "in the BucketFS. The file will overwrite an existing file."
            ),
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_delete_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.delete_file,
            description="Deletes a BucketFS file at the specified path.",
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_delete_directory(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.delete_directory,
            description=(
                "Deletes a BucketFS directory at the specified path. This operation "
                "will recursively delete all files and all subdirectories in this "
                "directory."
            ),
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_list_sql_types(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_sql_types,
        description="Lists Exasol SQL types and their parameters.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_system_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_system_tables,
        description="Lists Exasol system tables.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_describe_system_table(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_system_table,
        description="Describes specified Exasol system table.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_statistics_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_statistics_tables,
        description="Lists Exasol statistics tables.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_describe_statistics_table(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_statistics_table,
        description="Describes specified Exasol statistics table.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_keywords(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_keywords,
        description="Selects Exasol keywords.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_builtin_function_categories(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        builtin_function_categories,
        description="Lists Exasol built-in function categories.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_builtin_functions(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        list_builtin_functions,
        description="Lists Exasol built-in functions in the specified category.",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_describe_builtin_function(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        describe_builtin_function,
        description="Provides information about specified built-in function.",
        annotations={"readOnlyHint": True, "idempotentHint": True},
    )


def register_tools(mcp_server: ExasolMCPServer, config: McpServerSettings) -> None:
    if config.schemas.enable:
        _register_list_schemas(mcp_server)
        _register_find_schemas(mcp_server)
    if config.tables.enable or config.views.enable:
        _register_list_tables(mcp_server)
        _register_find_tables(mcp_server)
    if config.functions.enable:
        _register_list_functions(mcp_server)
        _register_find_functions(mcp_server)
    if config.scripts.enable:
        _register_list_scripts(mcp_server)
        _register_find_scripts(mcp_server)
    if config.columns.enable:
        _register_describe_table(mcp_server)
    if config.parameters.enable:
        _register_describe_function(mcp_server)
        _register_describe_script(mcp_server)
    if config.enable_read_query:
        _register_execute_query(mcp_server)
    if config.enable_write_query:
        _register_execute_write_query(mcp_server)
    if config.enable_read_bucketfs:
        _register_list_directories(mcp_server)
        _register_list_files(mcp_server)
        _register_find_files(mcp_server)
        _register_read_file(mcp_server)
    if config.enable_write_bucketfs:
        _register_write_text_to_file(mcp_server)
        _register_download_file(mcp_server)
        _register_delete_file(mcp_server)
        _register_delete_directory(mcp_server)
    _register_list_sql_types(mcp_server)
    _register_list_system_tables(mcp_server)
    _register_describe_system_table(mcp_server)
    _register_list_statistics_tables(mcp_server)
    _register_describe_statistics_table(mcp_server)
    _register_list_keywords(mcp_server)
    _register_builtin_function_categories(mcp_server)
    _register_list_builtin_functions(mcp_server)
    _register_describe_builtin_function(mcp_server)


def register_custom_routes(mcp_server: ExasolMCPServer) -> None:

    @mcp_server.custom_route(path="/health", methods=["GET"])
    def _health_check(request):
        return mcp_server.health_check()


def setup_logger(env: dict[str, str]) -> logging.Logger:
    """
    Configures the root logger using the info in the provided configuration dictionary.
    Return the root logger
    """
    logger = logging.getLogger()
    log_level = env[ENV_LOG_LEVEL] if ENV_LOG_LEVEL in env else DEFAULT_LOG_LEVEL
    logger.setLevel(log_level)

    # Create formatter if provided
    formatter = (
        logging.Formatter(env[ENV_LOG_FORMATTER]) if ENV_LOG_FORMATTER in env else None
    )

    # Add logging to a file, if the file is specified.
    if ENV_LOG_FILE in env:
        # Create logs directory if it doesn't exist
        log_file = env[ENV_LOG_FILE]
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Create rotating file handler
        max_bytes = (
            int(env[ENV_LOG_MAX_SIZE])
            if ENV_LOG_MAX_SIZE in env
            else DEFAULT_LOG_MAX_SIZE
        )
        backup_count = (
            int(env[ENV_LOG_BACKUP_COUNT])
            if ENV_LOG_BACKUP_COUNT in env
            else DEFAULT_LOG_BACKUP_COUNT
        )
        log_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )

        if formatter is not None:
            log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

    # Add logging to the console if specified.
    if (ENV_LOG_TO_CONSOLE in env) and str_to_bool(env[ENV_LOG_TO_CONSOLE]):
        console_handler = logging.StreamHandler()
        if formatter is not None:
            console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def get_mcp_settings(env: dict[str, Any]) -> McpServerSettings:
    """
    Reads optional settings. They can be provided either in a json string stored in the
    EXA_MCP_SETTINGS environment variable or in a json file. In the latter case
    EXA_MCP_SETTINGS must contain the file path.
    """
    try:
        settings_text = env.get(ENV_SETTINGS)
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


def create_mcp_server(
    connection: DbConnection,
    config: McpServerSettings,
    bucketfs_location: bfs.path.PathLike | None = None,
    **kwargs,
) -> ExasolMCPServer:
    """
    Creates the Exasol MCP Server and registers its tools.
    """
    mcp_server_ = ExasolMCPServer(
        connection=connection,
        config=config,
        bucketfs_location=bucketfs_location,
        **kwargs,
    )
    register_tools(mcp_server_, config)
    register_custom_routes(mcp_server_)
    return mcp_server_


def get_env() -> dict[str:Any]:
    return os.environ


def mcp_server() -> ExasolMCPServer:
    """
    Builds the Exasol MCP server and all its components.
    """
    env = get_env()
    logger = setup_logger(env)
    mcp_settings = get_mcp_settings(env)
    auth_kwargs = get_auth_kwargs()
    connection_factory = cf.get_connection_factory(env)

    connection = DbConnection(connection_factory=connection_factory)
    # Try to get the BucketFS location only if the bucketfs tools are enabled.
    if mcp_settings.enable_read_bucketfs or mcp_settings.enable_write_bucketfs:
        bucketfs_location = cf.get_bucketfs_location(env)
    else:
        bucketfs_location = None

    server = create_mcp_server(
        connection=connection,
        config=mcp_settings,
        bucketfs_location=bucketfs_location,
        **auth_kwargs,
    )
    logger.info("Exasol MCP Server created.")
    return server


def main():
    """
    Main entry point that creates and runs the MCP server locally.
    """
    server = mcp_server()
    server.run()


@click.command()
@click.option("--transport", default="http", help="MCP Transport (default: http)")
@click.option("--host", default="127.0.0.1", help="Host address (default: 127.0.0.1)")
@click.option(
    "--port",
    default=8000,
    type=click.IntRange(min=1),
    help="Port number (default: 8000)",
)
@click.option(
    "--no-auth", default=False, is_flag=True, help="Allow to run without authentication"
)
def main_http(transport, host, port, no_auth) -> None:
    """
    Runs the MCP server as a Direct HTTP Server.
    """
    server = mcp_server()
    # Verify that an authentication is in place. If not, unless this is explicitly
    # allowed, terminate the process.
    if server.auth is None:
        message = "The server has started without authentication."
        if no_auth:
            logger = logging.getLogger()
            logger.warning(message)
        else:
            raise RuntimeError(message)
    server.run(transport=transport, host=host, port=port)


if __name__ == "__main__":
    main()
