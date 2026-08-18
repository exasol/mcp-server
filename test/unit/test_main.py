import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from unittest.mock import (
    MagicMock,
    create_autospec,
    patch,
)

import pytest
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from fastmcp.server.auth import RemoteAuthProvider

import exasol.ai.mcp.server.main as main_module
from exasol.ai.mcp.server.connection.connection_factory import (
    ENV_DSN,
    ENV_PASSWORD,
    ENV_USER,
)
from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.main import (
    ENV_LOG_FILE,
    ENV_LOG_FORMATTER,
    ENV_LOG_IGNORE,
    ENV_LOG_LEVEL,
    ENV_LOG_TO_CONSOLE,
    ENV_SETTINGS,
    _find_error_code_config,
    _register_execute_query,
    _register_profile_query,
    _register_summarize_table,
    get_mcp_settings,
    get_project_short_tag,
    main_http,
    mcp_server,
    register_tools,
    setup_logger,
    setup_telemetry,
)
from exasol.ai.mcp.server.setup.generic_auth import (
    ENV_PROVIDER_TYPE,
    AuthParameter,
    exa_parameter_env_name,
    exa_provider_name,
)
from exasol.ai.mcp.server.setup.server_settings import McpServerSettings
from exasol.ai.mcp.server.tools.mcp_server import ExasolMCPServer


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


def test_setup_logger_ignore(tmp_path) -> None:
    log_file = tmp_path / "log_dir/log_file.log"
    env = {
        ENV_LOG_FILE: str(log_file),
        ENV_LOG_LEVEL: "INFO",
        ENV_LOG_IGNORE: "ignored_lib, another_lib",
    }
    setup_logger(env)
    logging.getLogger("ignored_lib").info("should be suppressed")
    logging.getLogger("another_lib").info("also suppressed")
    logging.getLogger("normal_logger").info("should appear")
    with open(log_file) as f:
        content = f.read()
    assert "should be suppressed" not in content
    assert "also suppressed" not in content
    assert "should appear" in content


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
    monkeypatch.setenv(
        exa_parameter_env_name(AuthParameter("jwks_uri")), "https://my_oidc.com/jwks"
    )
    monkeypatch.setenv(
        exa_parameter_env_name(AuthParameter("authorization_servers")),
        "https://my_oidc.com",
    )
    monkeypatch.setenv(
        exa_parameter_env_name(AuthParameter("base_url")), f"https://my_mpc.com"
    )
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


def test_register_tools_list_disabled() -> None:
    config = McpServerSettings(enable_list_tools=False)
    with (
        patch("exasol.ai.mcp.server.main._register_list_schemas") as p_ls,
        patch("exasol.ai.mcp.server.main._register_find_schemas") as p_fs,
        patch("exasol.ai.mcp.server.main._register_list_tables") as p_lt,
        patch("exasol.ai.mcp.server.main._register_find_tables") as p_ft,
        patch("exasol.ai.mcp.server.main._register_list_functions") as p_lf,
        patch("exasol.ai.mcp.server.main._register_find_functions") as p_ff,
        patch("exasol.ai.mcp.server.main._register_list_scripts") as p_lsc,
        patch("exasol.ai.mcp.server.main._register_find_scripts") as p_fsc,
    ):
        register_tools(MagicMock(), config)
        p_ls.assert_not_called()
        p_lt.assert_not_called()
        p_lf.assert_not_called()
        p_lsc.assert_not_called()
        p_fs.assert_called_once()
        p_ft.assert_called_once()
        p_ff.assert_called_once()
        p_fsc.assert_called_once()


def test_register_tools_find_disabled() -> None:
    config = McpServerSettings(enable_find_tools=False)
    with (
        patch("exasol.ai.mcp.server.main._register_list_schemas") as p_ls,
        patch("exasol.ai.mcp.server.main._register_find_schemas") as p_fs,
        patch("exasol.ai.mcp.server.main._register_list_tables") as p_lt,
        patch("exasol.ai.mcp.server.main._register_find_tables") as p_ft,
        patch("exasol.ai.mcp.server.main._register_list_functions") as p_lf,
        patch("exasol.ai.mcp.server.main._register_find_functions") as p_ff,
        patch("exasol.ai.mcp.server.main._register_list_scripts") as p_lsc,
        patch("exasol.ai.mcp.server.main._register_find_scripts") as p_fsc,
    ):
        register_tools(MagicMock(), config)
        p_ls.assert_called_once()
        p_lt.assert_called_once()
        p_lf.assert_called_once()
        p_lsc.assert_called_once()
        p_fs.assert_not_called()
        p_ft.assert_not_called()
        p_ff.assert_not_called()
        p_fsc.assert_not_called()


def test_register_tools_dialect_tools_enabled_by_default() -> None:
    config = McpServerSettings()
    with (
        patch("exasol.ai.mcp.server.main._register_list_sql_types") as p_lst,
        patch("exasol.ai.mcp.server.main._register_list_system_tables") as p_lsy,
        patch("exasol.ai.mcp.server.main._register_describe_system_table") as p_dsy,
        patch("exasol.ai.mcp.server.main._register_list_statistics_tables") as p_lst2,
        patch("exasol.ai.mcp.server.main._register_describe_statistics_table") as p_dst,
        patch("exasol.ai.mcp.server.main._register_list_keywords") as p_lk,
        patch(
            "exasol.ai.mcp.server.main._register_builtin_function_categories"
        ) as p_bfc,
        patch("exasol.ai.mcp.server.main._register_list_builtin_functions") as p_lbf,
        patch("exasol.ai.mcp.server.main._register_describe_builtin_function") as p_dbf,
    ):
        register_tools(MagicMock(), config)
        p_lst.assert_called_once()
        p_lsy.assert_called_once()
        p_dsy.assert_called_once()
        p_lst2.assert_called_once()
        p_dst.assert_called_once()
        p_lk.assert_called_once()
        p_bfc.assert_called_once()
        p_lbf.assert_called_once()
        p_dbf.assert_called_once()


def test_register_tools_dialect_tools_disabled() -> None:
    config = McpServerSettings(enable_dialect_tools=False)
    with (
        patch("exasol.ai.mcp.server.main._register_list_sql_types") as p_lst,
        patch("exasol.ai.mcp.server.main._register_list_system_tables") as p_lsy,
        patch("exasol.ai.mcp.server.main._register_describe_system_table") as p_dsy,
        patch("exasol.ai.mcp.server.main._register_list_statistics_tables") as p_lst2,
        patch("exasol.ai.mcp.server.main._register_describe_statistics_table") as p_dst,
        patch("exasol.ai.mcp.server.main._register_list_keywords") as p_lk,
        patch(
            "exasol.ai.mcp.server.main._register_builtin_function_categories"
        ) as p_bfc,
        patch("exasol.ai.mcp.server.main._register_list_builtin_functions") as p_lbf,
        patch("exasol.ai.mcp.server.main._register_describe_builtin_function") as p_dbf,
    ):
        register_tools(MagicMock(), config)
        p_lst.assert_not_called()
        p_lsy.assert_not_called()
        p_dsy.assert_not_called()
        p_lst2.assert_not_called()
        p_dst.assert_not_called()
        p_lk.assert_not_called()
        p_bfc.assert_not_called()
        p_lbf.assert_not_called()
        p_dbf.assert_not_called()


@pytest.mark.parametrize("query_result_format", ["tabular", "dict"])
@pytest.mark.parametrize(
    "register, tabular_fn, dict_fn",
    [
        (_register_execute_query, "execute_query_tabular", "execute_query"),
        (_register_profile_query, "profile_query_tabular", "profile_query"),
        (
            _register_summarize_table,
            "summarize_table_tabular",
            "summarize_table",
        ),
    ],
)
def test_register_respects_query_result_format(
    register, tabular_fn, dict_fn, query_result_format
) -> None:
    mcp_server = MagicMock()
    mcp_server.config = McpServerSettings(query_result_format=query_result_format)
    register(mcp_server)
    registered_fn = mcp_server.tool.call_args[0][0]
    expected_fn = getattr(
        mcp_server, tabular_fn if query_result_format == "tabular" else dict_fn
    )
    assert registered_fn is expected_fn


def _set_fake_module_location(
    monkeypatch: MonkeyPatch, module_file: Path, module_name: str
) -> None:
    monkeypatch.setattr(main_module, "__file__", str(module_file))
    monkeypatch.setattr(main_module, "__name__", module_name)


def test_find_error_code_config_found(tmp_path, monkeypatch) -> None:
    (tmp_path / "error_code_config.yml").write_text(
        "error-tags:\n  ABC:\n    highest-index: 0\n"
    )
    module_path = tmp_path / "exasol" / "ai" / "mcp" / "server" / "main.py"
    module_path.parent.mkdir(parents=True)
    _set_fake_module_location(monkeypatch, module_path, "exasol.ai.mcp.server.main")
    assert _find_error_code_config() == tmp_path / "error_code_config.yml"


def test_find_error_code_config_missing_next_to_package_root(
    tmp_path, monkeypatch
) -> None:
    module_path = tmp_path / "exasol" / "ai" / "mcp" / "server" / "main.py"
    module_path.parent.mkdir(parents=True)
    _set_fake_module_location(monkeypatch, module_path, "exasol.ai.mcp.server.main")
    assert _find_error_code_config() is None


def test_find_error_code_config_ignores_ancestor_files(tmp_path, monkeypatch) -> None:
    """
    A file further up the tree (above the directory containing the
    top-level package) must not be picked up.
    """
    (tmp_path / "error_code_config.yml").write_text(
        "error-tags:\n  WRONG:\n    highest-index: 0\n"
    )
    package_root = tmp_path / "site-packages"
    module_path = package_root / "exasol" / "ai" / "mcp" / "server" / "main.py"
    module_path.parent.mkdir(parents=True)
    _set_fake_module_location(monkeypatch, module_path, "exasol.ai.mcp.server.main")
    assert _find_error_code_config() is None


def test_find_error_code_config_module_name_too_deep(monkeypatch) -> None:
    # "/main.py" only has one parent ("/"), which is fewer than the five
    # directory levels implied by the dotted module name.
    _set_fake_module_location(
        monkeypatch, Path("/main.py"), "exasol.ai.mcp.server.main"
    )
    assert _find_error_code_config() is None


def test_get_project_short_tag_found(tmp_path, monkeypatch) -> None:
    config_file = tmp_path / "error_code_config.yml"
    config_file.write_text("error-tags:\n  ABC:\n    highest-index: 0\n")
    monkeypatch.setattr(
        "exasol.ai.mcp.server.main._find_error_code_config",
        lambda: config_file,
    )
    assert get_project_short_tag() == "ABC"


def test_get_project_short_tag_missing_file(monkeypatch) -> None:
    monkeypatch.setattr(
        "exasol.ai.mcp.server.main._find_error_code_config",
        lambda: None,
    )
    assert get_project_short_tag() is None


def test_get_project_short_tag_malformed(tmp_path, monkeypatch) -> None:
    config_file = tmp_path / "error_code_config.yml"
    config_file.write_text("not-error-tags: {}\n")
    monkeypatch.setattr(
        "exasol.ai.mcp.server.main._find_error_code_config",
        lambda: config_file,
    )
    assert get_project_short_tag() is None


def test_setup_telemetry_uses_project_short_tag() -> None:
    logger = logging.getLogger("test_setup_telemetry_ok")
    with (
        patch("exasol.ai.mcp.server.main.telemetry.was_setup", return_value=False),
        patch("exasol.ai.mcp.server.main.telemetry.setup"),
        patch("exasol.ai.mcp.server.main.telemetry.track") as mock_track,
        patch("exasol.ai.mcp.server.main.get_project_short_tag", return_value="EMCP"),
    ):
        setup_telemetry(logger)
        mock_track.assert_called_once_with("EMCP.started")


def test_setup_telemetry_warns_when_tag_missing(caplog) -> None:
    logger = logging.getLogger("test_setup_telemetry_warn")
    caplog.set_level(logging.WARNING, logger="test_setup_telemetry_warn")
    with (
        patch("exasol.ai.mcp.server.main.telemetry.was_setup", return_value=False),
        patch("exasol.ai.mcp.server.main.telemetry.setup"),
        patch("exasol.ai.mcp.server.main.telemetry.track") as mock_track,
        patch("exasol.ai.mcp.server.main.get_project_short_tag", return_value=None),
    ):
        setup_telemetry(logger)
        mock_track.assert_called_once_with("mcp-server.started")
        assert any(rec.levelname == "WARNING" for rec in caplog.records)


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
    result = runner.invoke(main_http, ["--no-auth"])
    assert result.exit_code == 0
    assert result.exception is None
    assert any(
        (rec.levelname == "WARNING") and ("authentication" in rec.message.lower())
        for rec in caplog.records
    )
