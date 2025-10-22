import json
import os
import re
from collections.abc import (
    Callable,
    Generator,
)
from contextlib import contextmanager
from typing import (
    Any,
    ContextManager,
)

import pyexasol
from fastmcp.server.dependencies import get_access_token
from pydantic import ValidationError
from pyexasol import ExaConnection

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.generic_auth import get_auth_kwargs
from exasol.ai.mcp.server.mcp_server import ExasolMCPServer
from exasol.ai.mcp.server.named_object_pool import NamedObjectPool
from exasol.ai.mcp.server.server_settings import McpServerSettings

ENV_SETTINGS = "EXA_MCP_SETTINGS"
""" MCP server settings json or a name of a json file with the settings """
ENV_DSN = "EXA_DSN"
""" Exasol DB server DSN """
ENV_USER = "EXA_USER"
""" The DB user name to be used by the MCP server """
ENV_PASSWORD = "EXA_PASSWORD"
""" The DB password for password authentication """
ENV_ACCESS_TOKEN = "EXA_ACCESS_TOKEN"
""" Bearer access token  """
ENV_REFRESH_TOKEN = "EXA_REFRESH_TOKEN"
""" Bearer refresh token  """
ENV_USERNAME_CLAIM = "EXA_USERNAME_CLAIM"
"""The name of the claim in the access token containing the DB username"""
ENV_POOL_SIZE = "EXA_POOL_SIZE"
"""The capacity of the connection pool"""

DEFAULT_CONN_POOL_SIZE = 5


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
    connection: DbConnection, config: McpServerSettings, **kwargs
) -> ExasolMCPServer:
    """
    Creates the Exasol MCP Server and registers its tools.
    """
    mcp_server = ExasolMCPServer(connection=connection, config=config, **kwargs)
    register_tools(mcp_server, config)
    return mcp_server


def get_oidc_user(username_claim: str | None) -> tuple[str | None, str]:
    token = get_access_token()
    return token.claims.get(username_claim), token.token


def _create_connection_kwargs(
    env: dict[str:Any], **extra_kwargs
) -> tuple[dict[str, Any], bool]:
    """
    Creates pyexasol.connect kwargs based on the provided configuration parameters.
    Also returns an indicator whether the OpenID authentication is used.
    Raises a ValueError if the set of configuration parameters is incomplete.
    """
    common_kwargs = {
        "dsn": env[ENV_DSN],
        "fetch_dict": True,
        "compression": True,
    }
    common_kwargs.update(extra_kwargs)

    # Infer the authentication method.
    use_open_id = False
    if ENV_PASSWORD in env:
        common_kwargs["password"] = env[ENV_PASSWORD]
    elif ENV_ACCESS_TOKEN in env:
        common_kwargs["access_token"] = env[ENV_ACCESS_TOKEN]
    elif ENV_REFRESH_TOKEN in env:
        common_kwargs["refresh_token"] = env[ENV_REFRESH_TOKEN]
    else:
        use_open_id = True

    # Validate the configuration. This, however, is not a definitive test.
    # The ENV_USERNAME_CLAIM may be set but not actually work. In that case the
    # exception will be raised in the factory. But we prefer it to be raised here.
    if (ENV_USER not in env) and not (use_open_id and (ENV_USERNAME_CLAIM in env)):
        raise ValueError(
            "The inferred authentication method requires a database username"
        )
    return common_kwargs, use_open_id


def _create_connection_pool(env: dict[str:Any]) -> NamedObjectPool[ExaConnection]:
    pool_size = (
        int(env[ENV_POOL_SIZE]) if ENV_POOL_SIZE in env else DEFAULT_CONN_POOL_SIZE
    )
    return NamedObjectPool(capacity=pool_size, cleanup=lambda conn: conn.close())


def get_connection_factory(
    env: dict[str:Any], **extra_kwargs
) -> Callable[[], ContextManager[ExaConnection]]:
    """
    Returns the pyexasol connection factory required by a DBConnection object.
    Authentication method will be inferred from the provided configuration
    parameters. Currently, the parameters come from environment variables.
    Going forward, the configuration parameters will be kept in the NBC secret store.

    The MCP server supports the same authentication methods as pyexasol. Currently,
    these are password and an OpenID token.

    The MCP server can be deployed in two ways: locally or as a remote http server.
    In the latter case the server works in the multiple user mode and its toos must be
    protected with OAuth2 authorization. The server can identify the user by looking
    at the claims in the access token. Most identity providers allow setting a custom
    claim or offer a choice of standard claims that can be used to store the DB
    username. The server needs to know the name of this claim.

    This gives us three basic options for the database connection:
    - The server is configured to use a specific username and either password or bearer
      token authentication. This works for both single and multiple user modes.
      The server's DB user must have the permission that is the least common denominator
      of the permissions of the users that are allowed to access the MCP server.

    - The server extracts the DB username, along with the token, from the MCP Auth
      context and uses that to open the connection. This option is suitable for
      multiuser mode, when the following two conditions are met:
      1. The users' authentication with the chosen identity provider is configured to
         add their DB usernames as a clim in the access token.
      2. The correspondent DB users are also authenticated using OpenID, with access
         token (refresh token is currently not supported). The database verifies the
         token with the same identity provider as the MCP server.

    - The server extracts the access token from the MCP Auth context, but uses its
      own username. This is a workaround for the multiuser mode, when either of the
      above conditions cannot be met. For this option we will not cache the connection,
      otherwise a token issued for one user could be used to execute a query requested
      by another user. As in the first option, the permissions of the server's DB user
      must be set accordingly.
    """
    common_kwargs, use_open_id = _create_connection_kwargs(env, **extra_kwargs)
    connection_pool = _create_connection_pool(env)

    @contextmanager
    def connection_factory() -> Generator[ExaConnection, None, None]:
        # In the OpenID mode try to get the current username, as well as the access
        # token, from the MCP context. That failed, use the default username.
        # Also, use the default username in all other modes.
        use_pool = True
        if use_open_id:
            user, token = get_oidc_user(env.get(ENV_USERNAME_CLAIM))
            if not user:
                if ENV_USER not in env:
                    raise RuntimeError(
                        "Cannot extract database username from the MCP "
                        "context, and no default username is specified"
                    )
                user = env[ENV_USER]
                use_pool = False
        else:
            user, token = env[ENV_USER], ""

        # Try to get the connection for the current user from the pool,
        # unless the pool is disabled.
        connection = connection_pool.checkout(user) if use_pool else None

        # Open a new one if needed.
        if (connection is None) or connection.is_closed:
            conn_kwargs = dict(common_kwargs)
            conn_kwargs["user"] = user
            if use_open_id:
                conn_kwargs["access_token"] = token
            connection = pyexasol.connect(**conn_kwargs)

        yield connection

        # Return the connection back to the pool, unless it has been closed
        # or the pool is disabled.
        if use_pool and not connection.is_closed:
            connection_pool.checkin(user, connection)

    return connection_factory


def get_env() -> dict[str:Any]:
    return os.environ


def main():
    """
    Main entry point that creates and runs the MCP server.
    """
    env = get_env()
    mcp_settings = get_mcp_settings(env)
    auth_kwargs = get_auth_kwargs()
    connection_factory = get_connection_factory(env)

    connection = DbConnection(connection_factory=connection_factory)

    mcp_server = create_mcp_server(
        connection=connection, config=mcp_settings, **auth_kwargs
    )
    mcp_server.run()


if __name__ == "__main__":
    main()
