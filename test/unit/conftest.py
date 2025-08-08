from unittest import mock

import pytest
from pyexasol import ExaConnection

from exasol.ai.mcp.server.parameter_parser import (
    FuncParameterParser,
    ScriptParameterParser,
)
from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaListSettings,
    MetaParameterSettings,
)


@pytest.fixture
def server_config() -> McpServerSettings:
    return McpServerSettings(
        scripts=MetaListSettings(comment_field="udf_comment"),
        parameters=MetaParameterSettings(
            enable=True,
            name_field="name",
            type_field="type",
            input_field="inputs",
            emit_field="emits",
            return_field="returns",
        ),
    )


@pytest.fixture
def func_parameter_parser(server_config) -> FuncParameterParser:
    return FuncParameterParser(
        connection=mock.create_autospec(ExaConnection), settings=server_config
    )


@pytest.fixture
def script_parameter_parser(server_config) -> ScriptParameterParser:
    return ScriptParameterParser(
        connection=mock.create_autospec(ExaConnection), settings=server_config
    )
