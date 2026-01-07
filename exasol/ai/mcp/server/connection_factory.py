import json
import logging
import ssl
from collections.abc import (
    Callable,
    Generator,
)
from contextlib import contextmanager
from pathlib import Path
from typing import (
    Any,
    ContextManager,
)

import exasol.bucketfs as bfs
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
""" SaaS host, defaults to https://cloud.exasol.com/ """
ENV_SAAS_ACCOUNT_ID = "EXA_SAAS_ACCOUNT_ID"
""" SaaS account id """
ENV_SAAS_PAT = "EXA_SAAS_PAT"
""" SaaS PAT in case the server's own credentials are used to connect to SaaS DB """
ENV_SAAS_PAT_HEADER = "EXA_SAAS_PAT_HEADER"
""" Name of the header where the SaaS user's PAT is passed, e.g. x-api-key """
ENV_SAAS_DATABASE_ID = "EXA_SAAS_DATABASE_ID"
""" Name of the SaaS database id, if known """
ENV_SAAS_DATABASE_NAME = "EXA_SAAS_DATABASE_NAME"
""" Name of the SaaS database, if the id is unknown """
ENV_SSL_CERT_VALIDATION = "EXA_SSL_CERT_VALIDATION"
""" Verify other peers’ certificates (yes/no) """
ENV_SSL_TRUSTED_CA = "EXA_SSL_TRUSTED_CA"
""" Directory where Certification Authority (CA) certificates are stored, or a single CA file """
ENV_SSL_CLIENT_CERT = "EXA_SSL_CLIENT_CERT"
""" Own certificate file in PEM format """
ENV_SSL_PRIVATE_KEY = "EXA_SSL_PRIVATE_KEY"
""" Certificate's private key file """
ENV_LOG_CLAIMS = "EXA_LOG_CLAIMS"
""" Log OAuth claims if available (yes/no) """
ENV_LOG_HTTP_HEADERS = "EXA_LOG_HTTP_HEADERS"
""" Log headers from the current HTTP request if available (yes/no) """
ENV_BUCKETFS_HOST = "EXA_BUCKETFS_HOST"
""" On-prem BucketFS host name """
ENV_BUCKETFS_PORT = "EXA_BUCKETFS_PORT"
""" On-prem BucketFS port number """
ENV_BUCKETFS_SERVICE = "EXA_BUCKETFS_SERVICE"
""" On-prem BucketFS service name (not required in most cases) """
ENV_BUCKETFS_BUCKET = "EXA_BUCKETFS_BUCKET"
""" On-prem BucketFS bucket name ("default" if not specified) """
ENV_BUCKETFS_USER = "EXA_BUCKETFS_USER"
""" On-prem BucketFS user name """
ENV_BUCKETFS_PASSWORD = "EXA_BUCKETFS_PASSWORD"
""" On-prem BucketFS user password """
ENV_BUCKETFS_PATH = "EXA_BUCKETFS_PATH"
""" Optional root directory in the bucket (defaults to the bucket root) """

DEFAULT_CONN_POOL_SIZE = 5
DEFAULT_SAAS_HOST = "https://cloud.exasol.com"

logger = logging.getLogger(__name__)


def local_env_complete(env: dict[str, Any]) -> bool:
    return all(v in env for v in [ENV_DSN, ENV_USER]) and any(
        v in env for v in [ENV_PASSWORD, ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN]
    )


def oidc_env_complete(env: dict[str, Any]) -> bool:
    return all(v in env for v in [ENV_DSN, ENV_USERNAME_CLAIM])


def saas_env_complete(env: dict[str, Any]) -> bool:
    return (
        (ENV_SAAS_ACCOUNT_ID in env)
        and any(v in env for v in [ENV_SAAS_PAT, ENV_SAAS_PAT_HEADER])
        and any(v in env for v in [ENV_SAAS_DATABASE_ID, ENV_SAAS_DATABASE_NAME])
    )


def _copy_kwargs(env: dict[str, Any], name_map: dict[str, str]) -> dict[str, Any]:
    return {val: env[key] for key, val in name_map.items() if key in env}


def get_local_kwargs(env: dict[str, Any]) -> dict[str, Any]:
    """
    Returns pyexasol.connect(...) arguments in case of On-Prem pre-configured
    server's credentials.
    """
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
    """
    Returns pyexasol.connect(...) arguments in case of SaaS backend.
    If PAT is not pre-configured, the function will attempt to extract it from the
    headers. The returned arguments is the result of an OpenAPI call that resolves
    the SaaS username and password from the provided account id and the database id
    or name.
    """
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
    if "host" not in saas_params:
        saas_params["host"] = DEFAULT_SAAS_HOST
    if "pat" not in saas_params:
        headers = fmcp_api.get_http_headers()
        if env[ENV_SAAS_PAT_HEADER] not in headers:
            raise RuntimeError(
                "The SAAS PAT is not specified and cannot be extracted from the call headers."
            )
        saas_params["pat"] = headers[env[ENV_SAAS_PAT_HEADER]]
    return saas_api.get_connection_params(**saas_params)


def optional_bool_from_env(env: dict[str, Any], var_name: str) -> bool | None:
    if var_name not in env:
        return None
    v = env[var_name].lower()
    if v in ["true", "yes", "y"]:
        return True
    elif v in ["false", "no", "n"]:
        return False
    else:
        raise ValueError(f"Unable to interpret {var_name} as a boolean value.")


def get_ssl_options(env: dict[str, Any]) -> dict[str, Any]:
    """
    Extracts SSL parameters from the provided configuration.
    Returns a dictionary in the websocket-client format
    (see https://websocket-client.readthedocs.io/en/latest/faq.html#what-else-can-i-do-with-sslopts)
    """
    sslopt: dict[str, object] = {}

    # Is server certificate validation required?
    certificate_validation = optional_bool_from_env(env, ENV_SSL_CERT_VALIDATION)
    if certificate_validation is not None:
        sslopt["cert_reqs"] = (
            ssl.CERT_REQUIRED if certificate_validation else ssl.CERT_NONE
        )

    # Is a bundle with trusted CAs provided?
    trusted_ca = env.get(ENV_SSL_TRUSTED_CA)
    if trusted_ca:
        trusted_ca_path = Path(trusted_ca)
        if trusted_ca_path.is_dir():
            sslopt["ca_cert_path"] = trusted_ca
        elif trusted_ca_path.is_file():
            sslopt["ca_certs"] = trusted_ca
        else:
            raise ValueError(f"Trusted CA location {trusted_ca} doesn't exist.")

    # Is client's own certificate provided?
    client_certificate = env.get(ENV_SSL_CLIENT_CERT)
    if client_certificate:
        if not Path(client_certificate).is_file():
            raise ValueError(f"Certificate file {client_certificate} doesn't exist.")
        sslopt["certfile"] = client_certificate
        private_key = env.get(ENV_SSL_PRIVATE_KEY)
        if private_key:
            if not Path(private_key).is_file():
                raise ValueError(f"Private key file {private_key} doesn't exist.")
            sslopt["keyfile"] = private_key

    return sslopt


def get_common_kwargs(env: dict[str, Any]) -> dict[str, Any]:
    common_kwargs = {
        "fetch_dict": True,
        "compression": True,
    }
    ssl_opt = get_ssl_options(env)
    if ssl_opt:
        common_kwargs["websocket_sslopt"] = ssl_opt
    return common_kwargs


def _create_connection_pool(
    env: dict[str:Any],
) -> NamedObjectPool[pyexasol.ExaConnection]:
    pool_size = int(env.get(ENV_POOL_SIZE, DEFAULT_CONN_POOL_SIZE))
    return NamedObjectPool(capacity=pool_size, cleanup=lambda conn: conn.close())


def _build_impersonate_query(user: str) -> str:
    # Currently, the IMPERSONATE query is not supported by SQLGlot.
    user_id = exp.Identifier(this=user, quoted=True)
    return f'IMPERSONATE {user_id.sql(dialect="exasol")}'


def get_oidc_user(username_claim: str | None) -> tuple[str | None, str | None]:
    token = fmcp_api.get_access_token()
    if token is None:
        return None, None
    return token.claims.get(username_claim), token.token


def log_connection(conn_kwargs: dict[str, Any], user: str, env: dict[str, Any]) -> None:
    """
    Logs a "db-connection" info message in a json format. Records the provided
    connection parameters and the username, the connection if opened with or the one
    that is impersonated.
    Optionally, records the OAuth claims and HTTP headers that can be found in the
    MCP context. This is controlled by the correspondent environment variables.
    Values that are not json-compatible are replaced with their type names.
    """
    masked_args = ["password", "access_token", "refresh_token"]
    conn_data = {
        "conn-kwargs": {
            k: "***" if k in masked_args else v for k, v in conn_kwargs.items()
        },
        "user": user,
    }
    log_claims = optional_bool_from_env(env, ENV_LOG_CLAIMS)
    if log_claims:
        token = fmcp_api.get_access_token()
        if (token is not None) and token.claims:
            conn_data["oauth-claims"] = token.claims
    log_headers = optional_bool_from_env(env, ENV_LOG_HTTP_HEADERS)
    if log_headers:
        headers = fmcp_api.get_http_headers()
        if headers:
            conn_data["http-headers"] = headers
    log_data = {"db-connection": conn_data}

    try:
        log_json = json.dumps(
            log_data, ensure_ascii=False, default=lambda o: type(o).__name__
        )
        logger.info(log_json)
    except (ValueError, TypeError) as e:
        logger.warning("Unable to log connection info: %s", e)


def get_connection_factory(
    env: dict[str, Any], **extra_kwargs
) -> Callable[[], ContextManager[pyexasol.ExaConnection]]:
    """
    Returns the pyexasol connection factory required by a DBConnection object.
    Authentication method will be inferred from the provided configuration
    parameters. Currently, the parameters come from environment variables.
    Going forward, the configuration parameters will be kept in the NBC secret store.

    For the On-Prem backend, the MCP server supports the same authentication methods as
    pyexasol. Currently, these are password and an OpenID token. To authenticate with
    the SaaS backend, the MCP server uses the Personal Access Token (PAT).

    The MCP server can be deployed in two ways: locally or as a remote http server.
    In the latter case the server works in the multiple user mode and its tools would
    normally be protected with OAuth2 authorization. The way to identify the user
    depends on the used backend.

    With an OnPrem database the MCP server can identify the user by looking at the
    claims in the access token. Most identity providers allow setting a custom claim
    or offer a choice of standard claims that can be used to store the DB username.
    The server needs to know the name of this claim.

    With a SaaS database, the PAT can be put in the call headers. The server needs to
    know the name of the header. In a way, the authentication is delegated to the SaaS
    database, the PAT itself being an access token. Therefore, a separate authorization
    of the MCP tools is optional.

    This gives us five different options for the database connection:
    *** On-Prem ***

    - A.
      The server is configured to use its own database credentials (username and either
      password or an OpenID token). No attempt is made to identify the actual user
      accessing the server tools. This works for both single and multiple user modes.
      The server tools may still be protected with OAuth2 authorization, but as far as
      the database connection is concerned this is irrelevant.
      The server's DB user must have the permission that is the least common denominator
      of the permissions of the users that are allowed to access the MCP server.

    - B.
      The server extracts the DB username, along with the token, from the MCP Auth
      context and uses that to open the connection. This option is suitable for
      multiuser mode, when the following two conditions are met:
      1. The users' authentication with the chosen identity provider is configured to
         add their DB usernames as a claim in the access token.
      2. The correspondent DB users are also authenticated using OpenID, with an access
         token (refresh token is currently not supported). The database verifies the
         token with the same identity provider as the MCP server. The subject, the DB
         user is identified with in the database, should, according to RFC 9068, match
         the subject field in the access token issued to this user.

    - C.
      The last option is a blend between the first two. It works in a multiuser mode,
      when the first of the above conditions is met but the second is not. The connection
      is opened using the pre-configured database credentials, as in the first option.
      But since the actual username can be identified, the connection impersonates this
      user. All subsequent queries are executed under this user's permissions. For this
      to work the server's user must have the "IMPERSONATE ANY USER" or "IMPERSONATION ON
      <user/role>" privilege.

      *** SaaS ***

      - D.
        The SaaS analogue of the option A. The server's database connection parameters
        include the SaaS host, account id, PAT and the database name or id.

      - E.
        The SaaS analogue of the option B, but much simpler. The PAT gets extracted from
        the headers and then used as in D.
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
            if (ENV_USERNAME_CLAIM in env) and (not user):
                raise RuntimeError(
                    f"Username not found in the OAuth claim {ENV_USERNAME_CLAIM}"
                )
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
            conn_kwargs.update(extra_kwargs)
            connection = pyexasol.connect(**conn_kwargs)
            if user != conn_kwargs["user"]:
                # If the actual username is known, and it's different from the
                # username in the connection then impersonate the actual user.
                query = _build_impersonate_query(user)
                connection.execute(query)
            log_connection(conn_kwargs, user, env)

        yield connection

        # Return the connection back to the pool, unless it has been closed.
        if not connection.is_closed:
            connection_pool.checkin(user, connection)

    return connection_factory


def get_bucketfs_location(env: dict[str, Any]) -> bfs.path.PathLike | None:
    """
    If sufficient configuration parameters are provided, opens a BucketFS connection
    and returns a PathLike of the bucket root or a specified root directory.

    Here, the word "connection" is used superficially. No actual connection exists.
    A successfully created BucketFS PathLike object indicates that the specified
    bucket is accessible, subject to read/write permissions.

    If BucketFS is inaccessible, logs a warning and returns None.
    """
    kwargs = _copy_kwargs(
        env,
        {
            ENV_BUCKETFS_HOST: "bucketfs_host",
            ENV_BUCKETFS_PORT: "bucketfs_port",
            ENV_BUCKETFS_SERVICE: "bucketfs_name",
            ENV_BUCKETFS_BUCKET: "bucket",
            ENV_BUCKETFS_USER: "bucketfs_user",
            ENV_BUCKETFS_PASSWORD: "bucketfs_password",
            ENV_SAAS_HOST: "saas_url",
            ENV_SAAS_ACCOUNT_ID: "saas_account_id",
            ENV_SAAS_PAT: "saas_token",
            ENV_SAAS_DATABASE_ID: "saas_database_id",
            ENV_SAAS_DATABASE_NAME: "saas_database_name",
            ENV_BUCKETFS_PATH: "path_in_bucket",
        },
    )
    if "bucketfs_port" in kwargs:
        kwargs["bucketfs_port"] = int(kwargs["bucketfs_port"])
    certificate_validation = optional_bool_from_env(env, ENV_SSL_CERT_VALIDATION)
    if certificate_validation is not None:
        kwargs["verify"] = certificate_validation

    try:
        return bfs.path.infer_path(**kwargs)
    except Exception as e:
        logger.warning("Unable to open BucketFS location. %s", e)
    return None
