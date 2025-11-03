from typing import Any
from unittest.mock import patch

import pytest

from exasol.ai.mcp.server.connection_factory import (
    ENV_ACCESS_TOKEN,
    ENV_DSN,
    ENV_PASSWORD,
    ENV_REFRESH_TOKEN,
    ENV_SAAS_ACCOUNT_ID,
    ENV_SAAS_DATABASE_ID,
    ENV_SAAS_DATABASE_NAME,
    ENV_SAAS_HOST,
    ENV_SAAS_PAT,
    ENV_SAAS_PAT_HEADER,
    ENV_USER,
    ENV_USERNAME_CLAIM,
    get_connection_factory,
    get_local_kwargs,
    get_oidc_user,
    get_saas_kwargs,
    local_env_complete,
    oidc_env_complete,
    saas_env_complete,
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
        ([ENV_SAAS_ACCOUNT_ID, ENV_SAAS_PAT, ENV_SAAS_DATABASE_ID], False),
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
    ids=["pre-configured-pat, db-id", "pat-in-header, db-name"],
)
@patch("fastmcp.server.dependencies.get_http_headers")
@patch("exasol.saas.client.api_access.get_connection_params")
def test_get_saas_kwargs(
    mock_connection_params, mock_http_headers, env, expected_kwargs
) -> None:
    mock_http_headers.return_value = {"pat_header": "my_pat_from_header"}
    get_saas_kwargs(env)
    mock_connection_params.assert_called_once_with(**expected_kwargs)


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
    This is a variation of the previous test for the case when the server connects to
    the database using the default credentials but impersonating the actual user.
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


def test_get_connection_factory_late_error(mock_connect) -> None:
    env = {ENV_DSN: "my.db.dsn", ENV_USERNAME_CLAIM: "username"}
    factory = get_connection_factory(env)
    with pytest.raises(RuntimeError, match="Cannot extract"):
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
