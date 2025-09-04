from unittest import mock

import pytest
from pyexasol import ExaConnection

from exasol.ai.mcp.server.parameter_parser import (
    FuncParameterParser,
    ScriptParameterParser,
)
from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaParameterSettings,
)


@pytest.fixture
def parameter_config() -> McpServerSettings:
    return McpServerSettings(
        parameters=MetaParameterSettings(
            enable=True,
            name_field="name",
            comment_field="function_comment",
            type_field="type",
            input_field="inputs",
            emit_field="emits",
            return_field="returns",
            usage_field="usage",
        ),
        case_sensitive=False,
    )


@pytest.fixture
def func_parameter_parser(parameter_config) -> FuncParameterParser:
    return FuncParameterParser(
        connection=mock.create_autospec(ExaConnection), settings=parameter_config
    )


@pytest.fixture
def script_parameter_parser(parameter_config) -> ScriptParameterParser:
    return ScriptParameterParser(
        connection=mock.create_autospec(ExaConnection), settings=parameter_config
    )
