import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Any
from unittest.mock import (
    create_autospec,
    patch,
)

from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
import pytest
from fastmcp.server.auth import RemoteAuthProvider

from exasol.ai.mcp.server.connection_factory import (
    ENV_DSN,
    ENV_PASSWORD,
    ENV_USER,
)
from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import (
    ENV_LOG_FILE,
    ENV_LOG_FORMATTER,
    ENV_LOG_LEVEL,
    ENV_LOG_TO_CONSOLE,
    ENV_SETTINGS,
    get_mcp_settings,
    mcp_server,
    setup_logger,
    main_http
)
from exasol.ai.mcp.server.generic_auth import (
    ENV_PROVIDER_TYPE,
    AuthParameter,
    exa_parameter_env_name,
    exa_provider_name,
)
from exasol.ai.mcp.server.mcp_server import ExasolMCPServer
from exasol.ai.mcp.server.server_settings import McpServerSettings


def _set_fake_conn(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_DSN, "my.db.dsn")
    monkeypatch.setenv(ENV_USER, "my_user_name")
    monkeypatch.setenv(ENV_PASSWORD, "my_password")


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


def test_setup_logger(tmp_path) -> None:
    log_file = tmp_path / "log_dir/log_file.log"
    log_format = "%(name)s - %(levelname)s - %(message)s"
    env = {
        ENV_LOG_FILE: str(log_file),
        ENV_LOG_LEVEL: "INFO",
        ENV_LOG_FORMATTER: log_format,
    }
    setup_logger(env)
    logger = logging.getLogger("test_logger")
    logger.info("Test message")
    with open(log_file) as f:
        assert f.read().strip() == "test_logger - INFO - Test message"


def test_setup_logger_to_console(caplog) -> None:
    log_format = "%(name)s - %(levelname)s - %(message)s"
    env = {
        ENV_LOG_TO_CONSOLE: "true",
        ENV_LOG_LEVEL: "INFO",
        ENV_LOG_FORMATTER: log_format,
    }
    setup_logger(env)
    logger = logging.getLogger("test_logger")

    caplog.clear()
    logger.info("Test message")
    assert len(caplog.records) == 1
    assert caplog.records[0].message == "Test message"
    assert caplog.records[0].levelname == "INFO"


@patch("exasol.ai.mcp.server.main.create_mcp_server")
@patch("exasol.ai.mcp.server.main.get_env")
def test_mcp_server(
    mock_get_env, mock_create_server, mock_connect, settings_json
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


@patch("exasol.ai.mcp.server.main.create_mcp_server")
def test_mcp_server_logger(
    mock_create_server, mock_connect, monkeypatch, tmp_path
) -> None:
    """
    This test validates that the root logger is configured during the
    McpServer creation.
    """
    mock_server = create_autospec(ExasolMCPServer)
    mock_create_server.return_value = mock_server
    _set_fake_conn(monkeypatch)
    log_file = str(tmp_path / "log_dir/log_file.log")
    monkeypatch.setenv(ENV_LOG_FILE, log_file)

    mcp_server()

    root_logger = logging.getLogger()
    assert log_file in [
        handler.baseFilename
        for handler in root_logger.handlers
        if isinstance(handler, RotatingFileHandler)
    ]


@patch("fastmcp.FastMCP.run")
def test_main_http(mock_run, monkeypatch) -> None:
    """
    Verifies that the HTTP server will run if the Auth is configured.
    """
    monkeypatch.setenv(ENV_PROVIDER_TYPE, exa_provider_name(RemoteAuthProvider))
    monkeypatch.setenv(exa_parameter_env_name(AuthParameter("jwks_uri")), "https://my_oidc.com/jwks")
    monkeypatch.setenv(exa_parameter_env_name(AuthParameter("authorization_servers")), "https://my_oidc.com")
    monkeypatch.setenv(exa_parameter_env_name(AuthParameter("base_url")), f"https://my_mpc.com")
    _set_fake_conn(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(main_http)
    assert result.exit_code == 0
    assert result.exception is None


@patch("fastmcp.FastMCP.run")
def test_main_http_error(mock_run, monkeypatch) -> None:
    """
    Verifies that the HTTP server will not run if the Auth is not configured.
    """
    _set_fake_conn(monkeypatch)
    runner = CliRunner()
    result = runner.invoke(main_http)
    assert result.exit_code > 0
    assert result.exception is not None


@patch("fastmcp.FastMCP.run")
def test_main_http_no_auth(mock_run, monkeypatch, caplog) -> None:
    """
    Verifies that the HTTP server will run if the Auth is not configured,
    but an exemption is given. A warning message should be logged.
    """
    _set_fake_conn(monkeypatch)
    monkeypatch.setenv(ENV_LOG_TO_CONSOLE, "true")
    caplog.clear()
    runner = CliRunner()
    result = runner.invoke(main_http, ['--no-auth'])
    assert result.exit_code == 0
    assert result.exception is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
