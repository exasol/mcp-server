import json
from typing import Any
from unittest.mock import (
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
    get_connection_factory,
    get_mcp_settings,
    main,
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


@patch("pyexasol.connect")
@patch("exasol.ai.mcp.server.main.get_access_token_string")
def test_get_connection_factory_oauth(mock_get_token_str, mock_connect) -> None:
    """
    This test validates the behaviour of the connection factory in case
    when MCP OpenID is used.
    """
    env = {ENV_DSN: "my.db.dsn", ENV_USER: "my_user_name"}
    mock_get_token_str.return_value = "xyz"
    factory = get_connection_factory(env)
    factory()
    _, connect_kwargs = mock_connect.call_args
    assert "password" not in connect_kwargs
    assert connect_kwargs["access_token"] == "xyz"


@pytest.mark.parametrize(
    ["token_env", "token_arg"],
    [(ENV_ACCESS_TOKEN, "access_token"), (ENV_REFRESH_TOKEN, "refresh_token")],
    ids=["access_token", "refresh_token"],
)
@patch("pyexasol.connect")
def test_get_connection_factory_bearer_token(
    mock_connect, token_env, token_arg
) -> None:
    """
    This test validates the use of an access or refresh token instead of the password.
    """
    env = {ENV_DSN: "my.db.dsn", ENV_USER: "my_user_name", token_env: "my_token"}
    factory = get_connection_factory(env)
    factory()
    _, connect_kwargs = mock_connect.call_args
    assert "password" not in connect_kwargs
    assert connect_kwargs[token_arg] == "my_token"


@patch("pyexasol.connect")
@patch("exasol.ai.mcp.server.main.create_mcp_server")
@patch("exasol.ai.mcp.server.main.get_env")
def test_main_with_json_str(
    mock_get_env, mock_create_server, mock_connect, settings_json
) -> None:
    """
    This test validates the creation of an MCP Server without authentication,
    using password.
    """
    mock_connection = create_autospec(ExaConnection)
    mock_connection.options = {}
    mock_connect.return_value = mock_connection
    mock_server = create_autospec(ExasolMCPServer)
    mock_create_server.return_value = mock_server
    mock_get_env.return_value = {
        ENV_DSN: "my.db.dsn",
        ENV_USER: "my_user_name",
        ENV_PASSWORD: "my_password",
        ENV_SETTINGS: json.dumps(settings_json),
    }
    main()
    _, create_server_kwargs = mock_create_server.call_args
    assert create_server_kwargs["config"] == McpServerSettings.model_validate(
        settings_json
    )
    assert isinstance(create_server_kwargs["connection"], DbConnection)
    mock_server.run.assert_called_once()
    # Test the connection factory
    create_server_kwargs["connection"].execute_query("SELECT 1", snapshot=False)
    _, connect_kwargs = mock_connect.call_args
    assert connect_kwargs["dsn"] == "my.db.dsn"
    assert connect_kwargs["user"] == "my_user_name"
    assert connect_kwargs["password"] == "my_password"
