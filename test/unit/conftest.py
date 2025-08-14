from unittest import mock

import pytest
from pyexasol import ExaConnection

from exasol.ai.mcp.server.parameter_parser import (
    FuncParameterParser,
    ScriptParameterParser,
)
from exasol.ai.mcp.server.server_settings import MetaParameterSettings


@pytest.fixture
def parameter_config() -> MetaParameterSettings:
    return MetaParameterSettings(
        enable=True,
        name_field="name",
        comment_field="function_comment",
        type_field="type",
        input_field="inputs",
        emit_field="emits",
        return_field="returns",
        usage_field="usage",
    )


@pytest.fixture
def func_parameter_parser(parameter_config) -> FuncParameterParser:
    return FuncParameterParser(
        connection=mock.create_autospec(ExaConnection), conf=parameter_config
    )


@pytest.fixture
def script_parameter_parser(parameter_config) -> ScriptParameterParser:
    return ScriptParameterParser(
        connection=mock.create_autospec(ExaConnection), conf=parameter_config
    )
