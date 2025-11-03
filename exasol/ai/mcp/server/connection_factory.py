from collections.abc import (
    Callable,
    Generator,
)
from contextlib import contextmanager
from typing import (
    Any,
    ContextManager,
)

import exasol.saas.client.api_access as saas_api
import fastmcp.server.dependencies as fmcp_api
import pyexasol
import sqlglot.expressions as exp

from exasol.ai.mcp.server.named_object_pool import NamedObjectPool

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
ENV_SAAS_HOST = "EXA_SAAS_HOST"
ENV_SAAS_ACCOUNT_ID = "EXA_SAAS_ACCOUNT_ID"
ENV_SAAS_PAT = "EXA_SAAS_PAT"
ENV_SAAS_PAT_HEADER = "EXA_SAAS_PAT_HEADER"
ENV_SAAS_DATABASE_ID = "EXA_SAAS_DATABASE_ID"
ENV_SAAS_DATABASE_NAME = "EXA_SAAS_DATABASE_NAME"

DEFAULT_CONN_POOL_SIZE = 5


def local_env_complete(env: dict[str, Any]) -> bool:
    return all(v in env for v in [ENV_DSN, ENV_USER]) and any(
        v in env for v in [ENV_PASSWORD, ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN]
    )


def oidc_env_complete(env: dict[str, Any]) -> bool:
    return all(v in env for v in [ENV_DSN, ENV_USERNAME_CLAIM])


def saas_env_complete(env: dict[str, Any]) -> bool:
    return (
        all(v in env for v in [ENV_SAAS_HOST, ENV_SAAS_ACCOUNT_ID])
        and any(v in env for v in [ENV_SAAS_PAT, ENV_SAAS_PAT_HEADER])
        and any(v in env for v in [ENV_SAAS_DATABASE_ID, ENV_SAAS_DATABASE_NAME])
    )


def _copy_kwargs(env: dict[str, Any], name_map: dict[str, str]) -> dict[str, Any]:
    return {val: env[key] for key, val in name_map.items() if key in env}


def get_local_kwargs(env: dict[str, Any]) -> dict[str, Any]:
    return _copy_kwargs(
        env,
        {
            ENV_DSN: "dsn",
            ENV_USER: "user",
            ENV_PASSWORD: "password",
            ENV_ACCESS_TOKEN: "access_token",
            ENV_REFRESH_TOKEN: "refresh_token",
        },
    )


def get_saas_kwargs(env: dict[str, Any]) -> dict[str, Any]:
    saas_params = _copy_kwargs(
        env,
        {
            ENV_SAAS_HOST: "host",
            ENV_SAAS_ACCOUNT_ID: "account_id",
            ENV_SAAS_PAT: "pat",
            ENV_SAAS_DATABASE_ID: "database_id",
            ENV_SAAS_DATABASE_NAME: "database_name",
        },
    )
    if "pat" not in saas_params:
        headers = fmcp_api.get_http_headers()
        if env[ENV_SAAS_PAT_HEADER] not in headers:
            raise RuntimeError(
                "The SAAS PAT is not specified and cannot be extracted from the call headers."
            )
        saas_params["pat"] = headers[env[ENV_SAAS_PAT_HEADER]]
    return saas_api.get_connection_params(**saas_params)


def get_common_kwargs(env: dict[str, Any]) -> dict[str, Any]:
    # TODO: Add SSL parameters.
    return {
        "fetch_dict": True,
        "compression": True,
    }


def _create_connection_pool(
    env: dict[str:Any],
) -> NamedObjectPool[pyexasol.ExaConnection]:
    pool_size = int(env.get(ENV_POOL_SIZE, DEFAULT_CONN_POOL_SIZE))
    return NamedObjectPool(capacity=pool_size, cleanup=lambda conn: conn.close())


def _build_impersonate_query(user: str) -> str:
    # I can't figure out how to construct this query properly in SQLGlot
    user_id = exp.Identifier(this=user, quoted=True)
    return f'IMPERSONATE {user_id.sql(dialect="exasol")}'


def get_oidc_user(username_claim: str | None) -> tuple[str | None, str | None]:
    token = fmcp_api.get_access_token()
    if token is None:
        return None, None
    return token.claims.get(username_claim), token.token


def get_connection_factory(
    env: dict[str:Any], **extra_kwargs
) -> Callable[[], ContextManager[pyexasol.ExaConnection]]:
    """
    Returns the pyexasol connection factory required by a DBConnection object.
    Authentication method will be inferred from the provided configuration
    parameters. Currently, the parameters come from environment variables.
    Going forward, the configuration parameters will be kept in the NBC secret store.

    The MCP server supports the same authentication methods as pyexasol. Currently,
    these are password and an OpenID token.

    The MCP server can be deployed in two ways: locally or as a remote http server.
    In the latter case the server works in the multiple user mode and its tools must be
    protected with OAuth2 authorization. The server can identify the user by looking
    at the claims in the access token. Most identity providers allow setting a custom
    claim or offer a choice of standard claims that can be used to store the DB
    username. The server needs to know the name of this claim.

    This gives us three basic options for the database connection:
    - The server is configured to use its own database credentials (username and either
      password or an OpenID token). No attempt is made to identify the actual user
      accessing the server tools. This works for both single and multiple user modes.
      The server tools may still be protected with OAuth2 authorization, but as far as
      the database connection is concerned this is irrelevant.
      The server's DB user must have the permission that is the least common denominator
      of the permissions of the users that are allowed to access the MCP server.

    - The server extracts the DB username, along with the token, from the MCP Auth
      context and uses that to open the connection. This option is suitable for
      multiuser mode, when the following two conditions are met:
      1. The users' authentication with the chosen identity provider is configured to
         add their DB usernames as a claim in the access token.
      2. The correspondent DB users are also authenticated using OpenID, with an access
         token (refresh token is currently not supported). The database verifies the
         token with the same identity provider as the MCP server. The subject, the DB
         user is identified with in the database, should, according to RFC 9068, match
         the subject field in the access token issued to this user.

    - The last option is a blend between the first two. It works in a multiuser mode,
      when the first of the above conditions is met but the second is not. The connection
      is opened using the pre-configured database credentials, as in the first option.
      But since the actual username can be identified, the connection impersonates this
      user. All subsequent queries are executed under this user's permissions. For this
      to work the server's user must have the "IMPERSONATE ANY USER" or "IMPERSONATION ON
      <user/role>" privilege.
    """

    # Validate the configuration. Here we only check the presence of a sufficient set
    # of environment variables to configure one of possible connection modes (Local,
    # OIDC or SaaS). Whether these variables are set correctly is another question. If
    # they are not, an exception will be raised in the factory. But we prefer it to be
    # raised here.
    if (
        (not local_env_complete(env))
        and (not oidc_env_complete(env))
        and (not saas_env_complete(env))
    ):
        raise ValueError("Insufficient database connection configuration")

    connection_pool = _create_connection_pool(env)

    @contextmanager
    def connection_factory() -> Generator[pyexasol.ExaConnection, None, None]:
        if saas_env_complete(env):
            conn_kwargs = get_saas_kwargs(env)
            user: str | None = None
        else:
            conn_kwargs = get_local_kwargs(env)
            user, token = get_oidc_user(env.get(ENV_USERNAME_CLAIM))
            if not local_env_complete(env):
                # If not using pre-configured server credentials then
                # authenticate with the token extracted from the MCP context.
                if user and token:
                    conn_kwargs["user"] = user
                    conn_kwargs["access_token"] = token
                else:
                    raise RuntimeError(
                        "Cannot extract user credentials from the MCP context, "
                        "and default credentials are not specified."
                    )
        user = user or conn_kwargs["user"]

        # Try to get the connection for the current user from the pool.
        connection = connection_pool.checkout(user)

        # Open a new one if needed.
        if (connection is None) or connection.is_closed:
            conn_kwargs.update(get_common_kwargs(env))
            connection = pyexasol.connect(**conn_kwargs)
            if user != conn_kwargs["user"]:
                # If the actual username is known, and it's different from the
                # username in the connection then impersonate the actual user.
                query = _build_impersonate_query(user)
                connection.execute(query)

        yield connection

        # Return the connection back to the pool, unless it has been closed.
        if not connection.is_closed:
            connection_pool.checkin(user, connection)

    return connection_factory
