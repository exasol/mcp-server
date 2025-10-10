import json
import os
import re
from collections.abc import Callable
from enum import (
    Enum,
    auto,
)

import pyexasol
from fastmcp.server.auth import (
    AuthProvider,
    OAuthProxy,
    RemoteAuthProvider,
)
from fastmcp.server.auth.providers.jwt import JWTVerifier
from fastmcp.server.dependencies import get_access_token
from pydantic import ValidationError
from pyexasol import ExaConnection

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.mcp_server import ExasolMCPServer
from exasol.ai.mcp.server.server_settings import McpServerSettings

ENV_SETTINGS = "EXA_MCP_SETTINGS"
""" MCP server settings json or a name of a json file with the settings """
ENV_DSN = "EXA_DSN"
""" Exasol DB server DSN """
ENV_USER = "EXA_USER"
""" The DB user name to be used by the MCP server """
ENV_PASSWORD = "EXA_PASSWORD"
""" The DB password for password authentication """
ENV_JWKS_URI = "EXA_JWKS_URI"
""" JSON Web Key Set endpoint for remote token verification """
ENV_AUTH_SERVERS = "EXA_AUTH_SERVERS"
""" Comma-separated list of authorization servers that support DCR """
ENV_BASE_URL = "EXA_BASE_URL"
""" The base URL of the MCP server, where a callback endpoint is created """
ENV_AUTH_ENDPOINT = "EXA_AUTH_ENDPOINT"
""" Authorization endpoint of an OAuth provider with manual client registration """
ENV_TOKEN_ENDPOINT = "EXA_TOKEN_ENDPOINT"
""" Token endpoint of an OAuth provider with manual client registration """
ENV_CLIENT_ID = "EXA_CLIENT_ID"
""" Client ID issued by an OAuth provider with manual client registration """
ENV_CLIENT_SECRET = "EXA_CLIENT_SECRET"
""" Client secret issued by an OAuth provider with manual client registration """


class AuthenticationMethod(Enum):
    PASSWORD = auto()
    OPEN_ID = auto()
    LDAP = auto()
    KERBEROS = auto()


def _register_list_schemas(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_schemas,
        description=(
            "The tool lists schemas in the Exasol Database. "
            "For each schema, it provides the name and an optional comment."
        ),
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
    )


def _register_list_tables(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_tables,
        description=(
            "The tool lists tables and views in the specified schema of the "
            "the Exasol Database. For each table and view, it provides the "
            "name, the schema, and an optional comment."
        ),
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
    )


def _register_list_functions(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_functions,
        description=(
            "The tool lists functions in the specified schema of the Exasol "
            "Database. For each function, it provides the name, the schema, "
            "and an optional comment."
        ),
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
    )


def _register_list_scripts(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.list_scripts,
        description=(
            "The tool lists the user defined functions (UDF) in the specified "
            "schema of the Exasol Database. For each UDF, it provides the name, "
            "the schema, and an optional comment."
        ),
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
    )


def _register_execute_query(mcp_server: ExasolMCPServer) -> None:
    mcp_server.tool(
        mcp_server.execute_query,
        description=(
            "The tool executes the specified query in the Exasol Database. The "
            "query must be a SELECT statement. The tool returns data selected "
            "by the query."
        ),
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


def create_mcp_server(
    connection: DbConnection, config: McpServerSettings, **kwargs
) -> ExasolMCPServer:
    """
    Creates the Exasol MCP Server and registers its tools.
    """
    mcp_server = ExasolMCPServer(connection=connection, config=config, **kwargs)
    register_tools(mcp_server, config)
    return mcp_server


def get_auth_provider() -> AuthProvider | None:

    def from_env(params: dict[str, str]) -> dict[str, str]:
        return {k: os.environ[v] for k, v in params.items() if v in os.environ}

    if ENV_BASE_URL in os.environ and ENV_JWKS_URI in os.environ:
        token_verifier = JWTVerifier(
            jwks_uri=os.environ[ENV_JWKS_URI],
            base_url=os.environ[ENV_BASE_URL],
        )
        if ENV_AUTH_SERVERS in os.environ:
            return RemoteAuthProvider(
                authorization_servers=os.environ[ENV_AUTH_SERVERS].split(","),
                base_url=os.environ[ENV_BASE_URL],
                token_verifier=token_verifier,
            )
        else:
            oauth_proxy_kwargs = {
                "upstream_authorization_endpoint": ENV_AUTH_ENDPOINT,
                "upstream_token_endpoint": ENV_TOKEN_ENDPOINT,
                "upstream_client_id": ENV_CLIENT_ID,
                "upstream_client_secret": ENV_CLIENT_SECRET,
            }
            if all(v in os.environ for v in oauth_proxy_kwargs.values()):
                # debugging
                print(from_env(oauth_proxy_kwargs))
                return OAuthProxy(
                    **from_env(oauth_proxy_kwargs),
                    base_url=os.environ[ENV_BASE_URL],
                    token_verifier=token_verifier,
                )
        return token_verifier
    return None


def get_connection_factory(
    auth_method: AuthenticationMethod, **kwargs
) -> Callable[[], ExaConnection]:
    conn_kwargs = {
        "dsn": os.environ[ENV_DSN],
        "user": os.environ[ENV_USER],
        "fetch_dict": True,
        "compression": True,
    }
    conn_kwargs.update(kwargs)
    if auth_method == AuthenticationMethod.PASSWORD:
        conn_kwargs["password"] = os.environ.get(ENV_PASSWORD)

    def connection_factory() -> ExaConnection:
        if auth_method == AuthenticationMethod.OPEN_ID:
            conn_kwargs["access_token"] = get_access_token().token
        return pyexasol.connect(**conn_kwargs)

    return connection_factory


def main():
    """
    Main entry point that creates and runs the MCP server.
    """
    mcp_settings = get_mcp_settings()
    auth = get_auth_provider()
    if auth is None:
        auth_method = AuthenticationMethod.PASSWORD
        auth_kwargs = {}
    else:
        auth_method = AuthenticationMethod.OPEN_ID
        auth_kwargs = {"auth": auth}
    connection_factory = get_connection_factory(auth_method)

    connection = DbConnection(connection_factory=connection_factory)

    mcp_server = create_mcp_server(
        connection=connection, config=mcp_settings, **auth_kwargs
    )
    mcp_server.run()


if __name__ == "__main__":
    main()
