import json
from typing import Any
from unittest.mock import (
    MagicMock,
    create_autospec,
    patch,
)

import pytest
from pyexasol import ExaConnection

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import (
    ENV_ACCESS_TOKEN,
    ENV_DSN,
    ENV_PASSWORD,
    ENV_REFRESH_TOKEN,
    ENV_SETTINGS,
    ENV_USER,
    ENV_USERNAME_CLAIM,
    get_connection_factory,
    get_mcp_settings,
    mcp_server,
)
from exasol.ai.mcp.server.mcp_server import ExasolMCPServer
from exasol.ai.mcp.server.server_settings import McpServerSettings


@pytest.fixture
def settings_json() -> dict[str, Any]:
    return {
        "schemas": {"enable": True, "like_pattern": "my_schema"},
        "tables": {"enable": True, "like_pattern": "my_tables%"},
        "views": {"enable": False},
        "language": "english",
    }


@pytest.fixture
def mock_connect():
    with patch("pyexasol.connect") as mock_pyconn:
        mock_connection = MagicMock(spec=ExaConnection)
        mock_connection.execute = MagicMock()
        mock_connection.configure_mock(is_closed=False)
        mock_connection.options = {}
        mock_pyconn.return_value = mock_connection
        yield mock_pyconn


def test_get_mcp_settings_empty() -> None:
    assert get_mcp_settings({}) == McpServerSettings()


def test_get_mcp_settings_json_str(settings_json) -> None:
    env = {ENV_SETTINGS: json.dumps(settings_json)}
    result = get_mcp_settings(env)
    assert result == McpServerSettings.model_validate(settings_json)


def test_get_mcp_settings_file(settings_json, tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    with open(json_path, "w") as f:
        json.dump(settings_json, f)
    env = {ENV_SETTINGS: str(json_path)}
    result = get_mcp_settings(env)
    assert result == McpServerSettings.model_validate(settings_json)


def test_get_mcp_settings_invalid_json_str(tmp_path) -> None:
    env = {ENV_SETTINGS: '{"abc"=123}'}
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings(env)


def test_get_mcp_settings_invalid_json_file(tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    with open(json_path, "w") as f:
        f.write('{"abc"=123}')
    env = {ENV_SETTINGS: str(json_path)}
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings(env)


def test_get_mcp_settings_no_file(tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    env = {ENV_SETTINGS: str(json_path)}
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings(env)


@pytest.fixture
def oidc_user_none() -> None:
    with patch("exasol.ai.mcp.server.main.get_oidc_user") as mock_oidc_user:
        mock_oidc_user.return_value = (None, None)
        yield


@patch("exasol.ai.mcp.server.main.get_oidc_user")
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


@patch("exasol.ai.mcp.server.main.get_oidc_user")
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
def test_get_connection_factory_single_user(
    oidc_user_none, mock_connect, auth_env, auth_arg
) -> None:
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
    with pytest.raises(ValueError, match="database username"):
        get_connection_factory(env)


def test_get_connection_factory_late_error(oidc_user_none, mock_connect) -> None:
    env = {ENV_DSN: "my.db.dsn", ENV_USERNAME_CLAIM: "username"}
    factory = get_connection_factory(env)
    with pytest.raises(RuntimeError, match="database username"):
        with factory():
            pass


@patch("exasol.ai.mcp.server.main.create_mcp_server")
@patch("exasol.ai.mcp.server.main.get_env")
def test_mcp_server(
    mock_get_env, mock_create_server, oidc_user_none, mock_connect, settings_json
) -> None:
    """
    This test validates the creation of an MCP Server in a single-user mode,
    using password.
    """
    mock_server = create_autospec(ExasolMCPServer)
    mock_create_server.return_value = mock_server
    mock_get_env.return_value = {
        ENV_DSN: "my.db.dsn",
        ENV_USER: "my_user_name",
        ENV_PASSWORD: "my_password",
        ENV_SETTINGS: json.dumps(settings_json),
    }
    server = mcp_server()
    assert isinstance(server, ExasolMCPServer)
    _, create_server_kwargs = mock_create_server.call_args
    assert create_server_kwargs["config"] == McpServerSettings.model_validate(
        settings_json
    )
    assert isinstance(create_server_kwargs["connection"], DbConnection)
    create_server_kwargs["connection"].execute_query("SELECT 1", snapshot=False)
    _, connect_kwargs = mock_connect.call_args
    assert connect_kwargs["dsn"] == "my.db.dsn"
    assert connect_kwargs["user"] == "my_user_name"
    assert connect_kwargs["password"] == "my_password"
