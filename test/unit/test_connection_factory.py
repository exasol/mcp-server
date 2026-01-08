import json
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Optional,
)
from unittest.mock import patch

import pytest
from fastmcp.server.auth import AccessToken

from exasol.ai.mcp.server.connection_factory import (
    DEFAULT_SAAS_HOST,
    ENV_ACCESS_TOKEN,
    ENV_BUCKETFS_BUCKET,
    ENV_BUCKETFS_PASSWORD,
    ENV_BUCKETFS_PATH,
    ENV_BUCKETFS_SERVICE,
    ENV_BUCKETFS_URL,
    ENV_BUCKETFS_USER,
    ENV_DSN,
    ENV_LOG_CLAIMS,
    ENV_LOG_HTTP_HEADERS,
    ENV_PASSWORD,
    ENV_REFRESH_TOKEN,
    ENV_SAAS_ACCOUNT_ID,
    ENV_SAAS_DATABASE_ID,
    ENV_SAAS_DATABASE_NAME,
    ENV_SAAS_HOST,
    ENV_SAAS_PAT,
    ENV_SAAS_PAT_HEADER,
    ENV_SSL_CERT_VALIDATION,
    ENV_SSL_CLIENT_CERT,
    ENV_SSL_PRIVATE_KEY,
    ENV_SSL_TRUSTED_CA,
    ENV_USER,
    ENV_USERNAME_CLAIM,
    get_bucketfs_location,
    get_common_kwargs,
    get_connection_factory,
    get_local_kwargs,
    get_oidc_user,
    get_saas_kwargs,
    get_ssl_options,
    local_env_complete,
    log_connection,
    oidc_env_complete,
    optional_bool_from_env,
    saas_env_complete,
)
from exasol.ai.mcp.server.main import (
    ENV_LOG_FILE,
    ENV_LOG_FORMATTER,
    ENV_LOG_LEVEL,
    setup_logger,
)


def _get_test_env(keys: list[str]) -> dict[str, Any]:
    return {key: "value" for key in keys}


@pytest.mark.parametrize(
    ["keys", "is_complete"],
    [
        ([ENV_USER, ENV_PASSWORD], False),
        ([ENV_DSN, ENV_PASSWORD], False),
        ([ENV_DSN, ENV_USER], False),
        ([ENV_DSN, ENV_USER, ENV_PASSWORD], True),
        ([ENV_DSN, ENV_USER, ENV_ACCESS_TOKEN], True),
        ([ENV_DSN, ENV_USER, ENV_REFRESH_TOKEN], True),
    ],
    ids=[
        "no-dsn",
        "no-user",
        "no-auth",
        "complete, password",
        "complete, access-token",
        "complete, refresh-token",
    ],
)
def test_local_env_complete(keys, is_complete) -> None:
    assert local_env_complete(_get_test_env(keys)) == is_complete


@pytest.mark.parametrize(
    ["keys", "is_complete"],
    [
        ([ENV_DSN], False),
        ([ENV_USERNAME_CLAIM], False),
        ([ENV_DSN, ENV_USERNAME_CLAIM], True),
    ],
    ids=["no-dsn", "no-claim", "complete"],
)
def test_oidc_env_complete(keys, is_complete) -> None:
    assert oidc_env_complete(_get_test_env(keys)) == is_complete


@pytest.mark.parametrize(
    ["keys", "is_complete"],
    [
        ([ENV_SAAS_ACCOUNT_ID, ENV_SAAS_PAT, ENV_SAAS_DATABASE_ID], True),
        ([ENV_SAAS_HOST, ENV_SAAS_PAT, ENV_SAAS_DATABASE_ID], False),
        ([ENV_SAAS_HOST, ENV_SAAS_ACCOUNT_ID, ENV_SAAS_DATABASE_ID], False),
        ([ENV_SAAS_HOST, ENV_SAAS_ACCOUNT_ID, ENV_SAAS_PAT], False),
        (
            [ENV_SAAS_HOST, ENV_SAAS_ACCOUNT_ID, ENV_SAAS_PAT, ENV_SAAS_DATABASE_ID],
            True,
        ),
        (
            [
                ENV_SAAS_HOST,
                ENV_SAAS_ACCOUNT_ID,
                ENV_SAAS_PAT_HEADER,
                ENV_SAAS_DATABASE_NAME,
            ],
            True,
        ),
    ],
    ids=[
        "no-host",
        "no-account-id",
        "no-pat",
        "no-db",
        "complete, pat, db-id",
        "complete, pat-header, db-name",
    ],
)
def test_saas_env_complete(keys, is_complete) -> None:
    assert saas_env_complete(_get_test_env(keys)) == is_complete


def test_get_local_kwargs():
    kwargs = get_local_kwargs(
        {
            ENV_DSN: "my_dsn",
            ENV_USER: "the_user",
            ENV_PASSWORD: "the_password",
            ENV_ACCESS_TOKEN: "the_access_token",
            ENV_REFRESH_TOKEN: "the_refresh_token",
        }
    )
    assert kwargs == {
        "dsn": "my_dsn",
        "user": "the_user",
        "password": "the_password",
        "access_token": "the_access_token",
        "refresh_token": "the_refresh_token",
    }


@pytest.mark.parametrize(
    ["env", "expected_kwargs"],
    [
        (
            {
                ENV_SAAS_HOST: "my_saas_host",
                ENV_SAAS_ACCOUNT_ID: "my_saas_account_id",
                ENV_SAAS_PAT: "my_pat",
                ENV_SAAS_DATABASE_ID: "my_saas_database_id",
            },
            {
                "host": "my_saas_host",
                "account_id": "my_saas_account_id",
                "pat": "my_pat",
                "database_id": "my_saas_database_id",
            },
        ),
        (
            {
                ENV_SAAS_ACCOUNT_ID: "my_saas_account_id",
                ENV_SAAS_PAT: "my_pat",
                ENV_SAAS_DATABASE_ID: "my_saas_database_id",
            },
            {
                "host": DEFAULT_SAAS_HOST,
                "account_id": "my_saas_account_id",
                "pat": "my_pat",
                "database_id": "my_saas_database_id",
            },
        ),
        (
            {
                ENV_SAAS_HOST: "my_saas_host",
                ENV_SAAS_ACCOUNT_ID: "my_saas_account_id",
                ENV_SAAS_PAT_HEADER: "pat_header",
                ENV_SAAS_DATABASE_NAME: "my_saas_database_name",
            },
            {
                "host": "my_saas_host",
                "account_id": "my_saas_account_id",
                "pat": "my_pat_from_header",
                "database_name": "my_saas_database_name",
            },
        ),
    ],
    ids=["pre-configured-pat, db-id", "default-saas-host", "pat-in-header, db-name"],
)
@patch("fastmcp.server.dependencies.get_http_headers")
@patch("exasol.saas.client.api_access.get_connection_params")
def test_get_saas_kwargs(
    mock_connection_params, mock_http_headers, env, expected_kwargs
) -> None:
    mock_http_headers.return_value = {"pat_header": "my_pat_from_header"}
    get_saas_kwargs(env)
    mock_connection_params.assert_called_once_with(**expected_kwargs)


@pytest.mark.parametrize(
    ["e_vars", "expected"],
    [
        (["yes", "y", "true", "YES", "Y", "TRUE"], True),
        (["no", "n", "false", "NO", "N", "FALSE"], False),
        ([None], None),
    ],
    ids=["True", "False", "None"],
)
def test_optional_bool_from_env(e_vars, expected) -> None:
    for e_var in e_vars:
        env = {} if e_var is None else {"name": e_var}
        assert optional_bool_from_env(env, "name") == expected


def test_optional_bool_from_env_error() -> None:
    with pytest.raises(ValueError, match="boolean"):
        assert optional_bool_from_env({"name": "maybe"}, "name")


@dataclass
class SslTestOpt:
    """
    A setup for testing one SSL option.
    """

    env_name: str
    """ Name of the environment variable to pass the SSL option in. """
    ssl_name: str | None = None
    """ Name of the SSL option in a dictionary returned by `get_ssl_options`. """
    env_value: str | None = None
    """ Value of the environment variable, or a part of it. Will not be set if None. """
    ssl_value: Any | None = None
    """ Expected value of the SSL option, or a part of it. """
    is_file: bool = False
    """ The option is a path to a file, that must exist. """
    is_dir: bool = False
    """ The option is a path to a directory, that must exist. """
    requires: Optional["SslTestOpt"] = None
    """ Another option that must be provided in order to test this one. """

    def get_test_case(self, tmp_path: Path) -> tuple[dict[str, str], Any | None]:
        """
        Returns the dictionary with environment variables and the expected value
        of the SSL option. The test will verify that this value is present in the
        dictionary returned by `get_ssl_options`.
        In case the env value is a path to a file or directory, the full path is
        constructed using the provided temporary path. The file or directory is created.
        """
        if not (self.is_file or self.is_dir):
            env_value, ssl_value = self.env_value, self.ssl_value
        else:
            pth = tmp_path / self.env_value
            if self.is_dir:
                pth.mkdir()
            else:
                pth.write_text("content", encoding="utf-8")
            env_value = str(pth)
            ssl_value = env_value
        env = {} if env_value is None else {self.env_name: env_value}
        if self.requires is not None:
            required_env, _ = self.requires.get_test_case(tmp_path)
            env.update(required_env)
        return env, ssl_value


@pytest.mark.parametrize(
    "test_opt",
    [
        SslTestOpt(
            env_name=ENV_SSL_CERT_VALIDATION,
            env_value="true",
            ssl_name="cert_reqs",
            ssl_value=ssl.CERT_REQUIRED,
        ),
        SslTestOpt(
            env_name=ENV_SSL_CERT_VALIDATION,
            env_value="false",
            ssl_name="cert_reqs",
            ssl_value=ssl.CERT_NONE,
        ),
        SslTestOpt(env_name=ENV_SSL_CERT_VALIDATION, ssl_name="cert_reqs"),
        SslTestOpt(
            env_name=ENV_SSL_TRUSTED_CA,
            env_value="my_ca_dir",
            is_dir=True,
            ssl_name="ca_cert_path",
        ),
        SslTestOpt(
            env_name=ENV_SSL_TRUSTED_CA,
            env_value="my_ca_file.cert",
            is_file=True,
            ssl_name="ca_certs",
        ),
        SslTestOpt(
            env_name=ENV_SSL_CLIENT_CERT,
            env_value="my_cli_file.cert",
            is_file=True,
            ssl_name="certfile",
        ),
        SslTestOpt(
            env_name=ENV_SSL_PRIVATE_KEY,
            env_value="my_private.key",
            is_file=True,
            ssl_name="keyfile",
            requires=SslTestOpt(
                env_name=ENV_SSL_CLIENT_CERT,
                env_value="my_cli_file.cert",
                is_file=True,
            ),
        ),
    ],
    ids=lambda opt: f"{opt.env_name}={opt.env_value}",
)
def test_get_ssl_options(test_opt, tmp_path) -> None:
    """Tests all SSL options individually, one by one."""
    env, ssl_value = test_opt.get_test_case(tmp_path)
    ssl_opt = get_ssl_options(env)
    assert ssl_opt.get(test_opt.ssl_name) == ssl_value


@pytest.mark.parametrize(
    "test_opt",
    [
        SslTestOpt(env_name=ENV_SSL_TRUSTED_CA, env_value="my_ca_dir", is_dir=True),
        SslTestOpt(
            env_name=ENV_SSL_TRUSTED_CA, env_value="my_ca_file.cert", is_file=True
        ),
        SslTestOpt(
            env_name=ENV_SSL_CLIENT_CERT, env_value="my_cli_file.cert", is_file=True
        ),
        SslTestOpt(
            env_name=ENV_SSL_PRIVATE_KEY,
            env_value="my_private.key",
            is_file=True,
            requires=SslTestOpt(
                env_name=ENV_SSL_CLIENT_CERT, env_value="my_cli_file.cert", is_file=True
            ),
        ),
    ],
    ids=lambda opt: f"{opt.env_name}={opt.env_value}",
)
def test_get_ssl_options_invalid(test_opt, tmp_path) -> None:
    """
    Verifies that get_ssl_options checks that specified files and directories actually
    exist, and raises a ValueError if they aren't.
    """
    env, _ = test_opt.get_test_case(tmp_path)
    env[test_opt.env_name] = str(tmp_path / "non-existent")
    with pytest.raises(ValueError, match="doesn't exist"):
        get_ssl_options(env)


def test_get_common_kwargs_sslopt() -> None:
    common_args = get_common_kwargs({})
    assert "websocket_sslopt" not in common_args
    common_args = get_common_kwargs({ENV_SSL_CERT_VALIDATION: "false"})
    assert "websocket_sslopt" in common_args


def test_get_oidc_user_none() -> None:
    assert get_oidc_user(None) == (None, None)


@patch("exasol.ai.mcp.server.connection_factory.get_oidc_user")
def test_get_connection_factory_oidc_multi_user(mock_oidc_user, mock_connect) -> None:
    """
    This test validates the behaviour of the connection factory in a multi-user case.
    The connection factory is expected to create a different connection for every user.
    The username should be extracted from the MCP Auth context.
    """
    env = {ENV_DSN: "my.db.dsn", ENV_USERNAME_CLAIM: "username"}
    factory = get_connection_factory(env)
    num_users = 3
    for i in range(1, num_users + 1):
        user_name = f"my_user{i}"
        mock_oidc_user.return_value = (user_name, "xyz")
        with factory():
            pass
        assert mock_connect.call_count == i
        _, connect_kwargs = mock_connect.call_args
        assert "password" not in connect_kwargs
        assert connect_kwargs["user"] == user_name
        assert connect_kwargs["access_token"] == "xyz"
    # Try, to get the connection for the first user again,
    # it should be pulled from cache.
    mock_oidc_user.return_value = ("my_user1", "xyz")
    with factory():
        pass
    assert mock_connect.call_count == num_users


@patch("exasol.ai.mcp.server.connection_factory.get_oidc_user")
def test_get_connection_factory_oidc_default_user(mock_oidc_user, mock_connect) -> None:
    """
    This is a variation of the `test_get_connection_factory_oidc_multi_user` test for
    the case when the server connects to the database using the default credentials but
    impersonating the actual user.
    """
    env = {
        ENV_DSN: "my.db.dsn",
        ENV_USER: "my_user_name",
        ENV_PASSWORD: "my_password",
        ENV_USERNAME_CLAIM: "username",
    }
    factory = get_connection_factory(env)
    num_users = 3
    for i in range(1, num_users + 1):
        user_name = f"my_user{i}"
        mock_oidc_user.return_value = (user_name, "xyz")
        with factory():
            pass
        assert mock_connect.call_count == i
        _, connect_kwargs = mock_connect.call_args
        assert "access_token" not in connect_kwargs
        assert connect_kwargs["user"] == "my_user_name"
        assert connect_kwargs["password"] == "my_password"
        execute_args = mock_connect.return_value.execute.call_args.args
        assert execute_args == (f'IMPERSONATE "{user_name}"',)
    # Try, to get the connection for the first user again,
    # it should be pulled from cache.
    mock_oidc_user.return_value = ("my_user1", "xyz")
    with factory():
        pass
    assert mock_connect.call_count == num_users


@pytest.mark.parametrize(
    ["auth_env", "auth_arg"],
    [
        (ENV_PASSWORD, "password"),
        (ENV_ACCESS_TOKEN, "access_token"),
        (ENV_REFRESH_TOKEN, "refresh_token"),
    ],
    ids=["password", "access_token", "refresh_token"],
)
def test_get_connection_factory_single_user(mock_connect, auth_env, auth_arg) -> None:
    """
    This test validates the behaviour of the connection factory in a single-user case,
    using the default credentials.
    """
    env = {ENV_DSN: "my.db.dsn", ENV_USER: "my_user_name", auth_env: "secret"}
    factory = get_connection_factory(env)
    with factory():
        pass
    assert mock_connect.call_count == 1
    _, connect_kwargs = mock_connect.call_args
    assert connect_kwargs["user"] == "my_user_name"
    assert connect_kwargs[auth_arg] == "secret"
    # The connection should be cached.
    with factory():
        pass
    assert mock_connect.call_count == 1


def test_get_connection_factory_early_error() -> None:
    env = {ENV_DSN: "my.db.dsn"}
    with pytest.raises(
        ValueError,
        match="Insufficient",
    ):
        get_connection_factory(env)


@patch("exasol.ai.mcp.server.connection_factory.get_oidc_user")
def test_get_connection_factory_late_error(mock_oidc_user, mock_connect) -> None:
    env = {ENV_DSN: "my.db.dsn", ENV_USERNAME_CLAIM: "username"}
    mock_oidc_user.return_value = ("my_user_name", "")
    factory = get_connection_factory(env)
    with pytest.raises(RuntimeError, match="Cannot extract"):
        with factory():
            pass


def test_get_connection_factory_no_claim(mock_connect) -> None:
    env = {ENV_DSN: "my.db.dsn", ENV_USERNAME_CLAIM: "username"}
    factory = get_connection_factory(env)
    with pytest.raises(RuntimeError, match="Username not found"):
        with factory():
            pass


@patch("exasol.saas.client.api_access.get_connection_params")
def test_get_connection_factory_sass(mock_connection_params, mock_connect) -> None:
    """
    This test validates the behaviour of the connection factory in case,
    of SaaS backend.
    """
    env = _get_test_env(
        [
            ENV_SAAS_HOST,
            ENV_SAAS_ACCOUNT_ID,
            ENV_SAAS_PAT,
            ENV_SAAS_DATABASE_ID,
        ]
    )
    mock_connection_params.return_value = {
        "dsn": "my.db.dsn",
        "user": "my_user_name",
        "password": "my_password",
    }
    factory = get_connection_factory(env)
    with factory():
        pass
    assert mock_connect.call_count == 1
    _, connect_kwargs = mock_connect.call_args
    assert connect_kwargs["dsn"] == "my.db.dsn"
    assert connect_kwargs["user"] == "my_user_name"
    assert connect_kwargs["password"] == "my_password"
    # The connection should be cached.
    with factory():
        pass
    assert mock_connect.call_count == 1


@patch("fastmcp.server.dependencies.get_access_token")
@patch("fastmcp.server.dependencies.get_http_headers")
def test_log_connection(mock_http_headers, mock_access_token, tmp_path) -> None:
    log_file = str(tmp_path / "log_dir/log_file.log")
    log_format = "%(message)s"
    env = {
        ENV_LOG_FILE: log_file,
        ENV_LOG_LEVEL: "INFO",
        ENV_LOG_FORMATTER: log_format,
        ENV_LOG_CLAIMS: "true",
        ENV_LOG_HTTP_HEADERS: "true",
    }
    setup_logger(env)

    access_token = AccessToken(
        token="my_token",
        client_id="my_client_id",
        scopes=["my_scope"],
        claims={"claim1": "carnivore", "claim2": "nocturnal"},
    )
    mock_access_token.return_value = access_token
    mock_http_headers.return_value = {
        "my_header_name": "my_header_value",
    }
    conn_kwargs = {
        "dsn": "my.db.dsn",
        "user": "server-user-name",
        "password": "my-password",
        "non-json-arg": access_token,
    }
    user = "user-user-name"
    log_connection(conn_kwargs, user, env)
    expected_json = {
        "db-connection": {
            "conn-kwargs": {
                "dsn": "my.db.dsn",
                "user": "server-user-name",
                "password": "***",
                "non-json-arg": "AccessToken",
            },
            "user": "user-user-name",
            "oauth-claims": {"claim1": "carnivore", "claim2": "nocturnal"},
            "http-headers": {"my_header_name": "my_header_value"},
        }
    }
    with open(log_file) as f:
        actual_json = json.load(f)
    assert actual_json == expected_json


@pytest.mark.parametrize(
    ["env", "expected_kwargs"],
    [
        (
            {
                ENV_BUCKETFS_URL: "https://my_bfs_host:4321",
                ENV_BUCKETFS_SERVICE: "my_bfs_service",
                ENV_BUCKETFS_BUCKET: "my_bucket",
                ENV_BUCKETFS_USER: "me",
                ENV_BUCKETFS_PASSWORD: "my_password",
                ENV_BUCKETFS_PATH: "my_path_in_bucket",
                ENV_SSL_CERT_VALIDATION: "yes",
            },
            {
                "backend": "onprem",
                "url": "https://my_bfs_host:4321",
                "service_name": "my_bfs_service",
                "bucket_name": "my_bucket",
                "username": "me",
                "password": "my_password",
                "path": "my_path_in_bucket",
                "verify": True,
            },
        ),
        (
            {
                ENV_SAAS_HOST: "the_saas_url",
                ENV_SAAS_ACCOUNT_ID: "my_saas_account_id",
                ENV_SAAS_PAT: "my_saas_pat",
                ENV_SAAS_DATABASE_ID: "my_saas_db_id",
                ENV_BUCKETFS_PATH: "my_path_in_bucket",
            },
            {
                "backend": "saas",
                "url": "the_saas_url",
                "account_id": "my_saas_account_id",
                "pat": "my_saas_pat",
                "database_id": "my_saas_db_id",
                "path": "my_path_in_bucket",
            },
        ),
    ],
)
@patch("exasol.bucketfs.path.build_path")
def test_get_bucketfs_location(mock_infer_path, env, expected_kwargs) -> None:
    get_bucketfs_location(env)
    mock_infer_path.assert_called_with(**expected_kwargs)


def test_get_bucketfs_location_failure() -> None:
    with pytest.raises(Exception):
        get_bucketfs_location(
            {
                ENV_BUCKETFS_URL: "https://my_bfs_host:4321",
                ENV_BUCKETFS_USER: "me",
                # No password, that should trigger an exception before
                # attempting to access the BucketFS.
            }
        )
