import json
import os
from typing import Any
from unittest.mock import (
    create_autospec,
    patch,
)

import pytest
from pyexasol import ExaConnection

from exasol.ai.mcp.server.mcp_server import (
    ENVAR_DSN,
    ENVAR_PASSWORD,
    ENVAR_SETTINGS,
    ENVAR_USER,
    get_mcp_settings,
    main,
)
from exasol.ai.mcp.server.server_settings import McpServerSettings


@pytest.fixture
def clear_settings() -> None:
    envar = os.environ.pop(ENVAR_SETTINGS, None)
    yield
    if envar:
        os.environ[ENVAR_SETTINGS] = envar


@pytest.fixture
def settings_json() -> dict[str, Any]:
    return {
        "schemas": {"enable": True, "like_pattern": "my_schema"},
        "tables": {"enable": True, "like_pattern": "my_tables%"},
        "views": {"enable": False},
    }


def test_get_mcp_settings_empty(clear_settings) -> None:
    assert get_mcp_settings() == McpServerSettings()


def test_get_mcp_settings_json_str(clear_settings, settings_json) -> None:
    os.environ[ENVAR_SETTINGS] = json.dumps(settings_json)
    result = get_mcp_settings()
    assert result == McpServerSettings.model_validate(settings_json)


def test_get_mcp_settings_file(clear_settings, settings_json, tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    with open(json_path, "w") as f:
        json.dump(settings_json, f)
    os.environ[ENVAR_SETTINGS] = str(json_path)
    result = get_mcp_settings()
    assert result == McpServerSettings.model_validate(settings_json)


def test_get_mcp_settings_invalid_json_str(clear_settings, tmp_path) -> None:
    os.environ[ENVAR_SETTINGS] = '{"abc"=123}'
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings()


def test_get_mcp_settings_invalid_json_file(clear_settings, tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    with open(json_path, "w") as f:
        f.write('{"abc"=123}')
    os.environ[ENVAR_SETTINGS] = str(json_path)
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings()


def test_get_mcp_settings_no_file(clear_settings, tmp_path) -> None:
    json_path = tmp_path / "mcp_settings.json"
    os.environ[ENVAR_SETTINGS] = str(json_path)
    with pytest.raises(ValueError, match="Invalid MCP Server configuration"):
        get_mcp_settings()


@patch("pyexasol.connect")
@patch("exasol.ai.mcp.server.mcp_server.ExasolMCPServer")
def test_main_with_json_str(
    mock_server, mock_connect, clear_settings, settings_json
) -> None:
    mock_connect.return_value = create_autospec(ExaConnection)
    os.environ[ENVAR_DSN] = "my.db.dsn"
    os.environ[ENVAR_USER] = "my_user_name"
    os.environ[ENVAR_PASSWORD] = "my_password"
    os.environ[ENVAR_SETTINGS] = json.dumps(settings_json)
    main()
    _, kwargs = mock_connect.call_args
    assert kwargs["dsn"] == "my.db.dsn"
    assert kwargs["user"] == "my_user_name"
    assert kwargs["password"] == "my_password"
    _, kwargs = mock_server.call_args
    assert kwargs["config"] == McpServerSettings.model_validate(settings_json)
