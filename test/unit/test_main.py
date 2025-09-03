import json
import os
from typing import Any
from unittest.mock import (
    create_autospec,
    patch,
)

import pytest
from pyexasol import ExaConnection

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import (
    ENV_DSN,
    ENV_PASSWORD,
    ENV_SETTINGS,
    ENV_USER,
    get_mcp_settings,
    main,
)
from exasol.ai.mcp.server.mcp_server import ExasolMCPServer
from exasol.ai.mcp.server.server_settings import McpServerSettings


@pytest.fixture
def clear_settings() -> None:
    envar = os.environ.pop(ENV_SETTINGS, None)
    yield
    if envar:
        os.environ[ENV_SETTINGS] = envar


@pytest.fixture
def settings_json() -> dict[str, Any]:
    return {
        "schemas": {"enable": True, "like_pattern": "my_schema"},
        "tables": {"enable": True, "like_pattern": "my_tables%"},
        "views": {"enable": False},
        "language": "english",
    }


def test_get_mcp_settings_empty(clear_settings) -> None:
    assert get_mcp_settings() == McpServerSettings()


def test_get_mcp_settings_json_str(clear_settings, settings_json) -> None:
    os.environ[ENV_SETTINGS] = json.dumps(settings_json)
    result = get_mcp_settings()
    assert result == McpServerSettings.model_validate(settings_json)


def test_get_mcp_settings_file(clear_settings, settings_json, tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    with open(json_path, "w") as f:
        json.dump(settings_json, f)
    os.environ[ENV_SETTINGS] = str(json_path)
    result = get_mcp_settings()
    assert result == McpServerSettings.model_validate(settings_json)


def test_get_mcp_settings_invalid_json_str(clear_settings, tmp_path) -> None:
    os.environ[ENV_SETTINGS] = '{"abc"=123}'
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings()


def test_get_mcp_settings_invalid_json_file(clear_settings, tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    with open(json_path, "w") as f:
        f.write('{"abc"=123}')
    os.environ[ENV_SETTINGS] = str(json_path)
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings()


def test_get_mcp_settings_no_file(clear_settings, tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    os.environ[ENV_SETTINGS] = str(json_path)
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings()


@patch("pyexasol.connect")
@patch("exasol.ai.mcp.server.main.create_mcp_server")
def test_main_with_json_str(
    mock_create_server, mock_connect, clear_settings, settings_json
) -> None:
    mock_connection = create_autospec(ExaConnection)
    mock_connect.return_value = mock_connection
    mock_server = create_autospec(ExasolMCPServer)
    mock_create_server.return_value = mock_server
    os.environ[ENV_DSN] = "my.db.dsn"
    os.environ[ENV_USER] = "my_user_name"
    os.environ[ENV_PASSWORD] = "my_password"
    os.environ[ENV_SETTINGS] = json.dumps(settings_json)
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
