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
        name="list_exasol_schemas",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_schemas(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_schemas,
        name="find_exasol_schemas",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_list_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_tables,
        name="list_exasol_tables_and_views",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_tables,
        name="find_exasol_tables_and_views",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_list_functions(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_functions,
        name="list_exasol_custom_functions",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_functions(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_functions,
        name="find_exasol_custom_functions",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_list_scripts(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_scripts,
        name="list_exasol_user_defined_functions",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_find_scripts(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.find_scripts,
        name="find_exasol_user_defined_functions",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_describe_table(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_table,
        name="describe_exasol_table_or_view",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_describe_function(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_function,
        name="describe_exasol_custom_function",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_describe_script(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_script,
        name="describe_exasol_user_defined_function",
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_execute_query(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.execute_query,
        name="execute_exasol_query",
        description=(
            "The query must be a SELECT statement. Returns data selected by the query."
        ),
        annotations=ToolAnnotations(readOnlyHint=True),
    )


def _register_execute_write_query(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.execute_write_query,
        name="execute_exasol_write_query",
        description=(
            "Executes DML or DDL query. Returns modified query "
            "in case it was altered by the user, otherwise none."
        ),
        annotations=ToolAnnotations(destructiveHint=True),
    )


def _register_list_directories(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.list_directories,
            name="list_bucketfs_directories",
            description=("Returns subdirectories of a specified directory."),
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_list_files(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.list_files,
            name="list_bucketfs_files",
            description="Returns files in a specified directory.",
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_find_files(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.find_files,
            name="find_bucketfs_files",
            description=(
                "Performs a keyword search of files in a specified directory "
                "and all descendant subdirectories."
            ),
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_read_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.read_file,
            name="read_bucketfs_text_file",
            annotations=ToolAnnotations(readOnlyHint=True),
        )


def _register_write_text_to_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.write_text_to_file,
            name="write_text_to_bucketfs_file",
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_download_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.download_file,
            description=(
                "Downloads a file from a given url and saves in the BucketFS. "
                "The file will overwrite an existing file."
            ),
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_delete_file(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.delete_file,
            name="delete_bucketfs_file",
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_delete_directory(mcp_server: ExasolMCPServer) -> None:
    if mcp_server.bucketfs_tools is not None:
        mcp_server.tool(
            mcp_server.bucketfs_tools.delete_directory,
            name="delete_bucketfs_directory",
            description=("Will recursively delete all files and all subdirectories."),
            annotations=ToolAnnotations(destructiveHint=True),
        )


def _register_list_sql_types(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_sql_types,
        name="list_exasol_sql_types",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_system_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_system_tables,
        name="list_exasol_system_tables",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_describe_system_table(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_system_table,
        name="describe_exasol_system_table",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_statistics_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_statistics_tables,
        name="list_exasol_statistics_tables",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_describe_statistics_table(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.describe_statistics_table,
        name="describe_exasol_statistics_table",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_keywords(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_keywords,
        name="list_exasol_keywords",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_builtin_function_categories(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        builtin_function_categories,
        name="list_exasol_built_in_function_categories",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_list_builtin_functions(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        list_builtin_functions,
        name="list_exasol_built_in_functions",
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )


def _register_describe_builtin_function(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        describe_builtin_function,
        name="describe_exasol_built_in_function",
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
