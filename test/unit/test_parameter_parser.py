import json
import re
from textwrap import dedent
from unittest import mock

import pytest
from pyexasol import ExaConnection

from exasol.ai.mcp.server.parameter_parser import (
    FuncParameterParser,
    ScriptParameterParser,
    parameter_list_pattern,
)
from exasol.ai.mcp.server.parameter_pattern import regex_flags
from exasol.ai.mcp.server.server_settings import MetaParameterSettings


@pytest.fixture
def param_config() -> MetaParameterSettings:
    return MetaParameterSettings(
        enable=True,
        name_field="name",
        type_field="type",
        input_field="inputs",
        emit_field="emits",
        return_field="returns",
    )


@pytest.fixture
def func_parameter_parser(param_config) -> FuncParameterParser:
    return FuncParameterParser(
        connection=mock.create_autospec(ExaConnection), conf=param_config
    )


@pytest.fixture
def script_parameter_parser(param_config) -> ScriptParameterParser:
    return ScriptParameterParser(
        connection=mock.create_autospec(ExaConnection), conf=param_config
    )


@pytest.mark.parametrize(
    "params_list",
    ["( abc DOUBLE, xZZ varchar(200) )", '("my_real" REAL, "my_int" INTEGER)'],
    ids=["non-quoted-names", "quoted-names"],
)
def test_parameter_list_pattern(params_list):
    assert (
        re.match(rf"\({parameter_list_pattern}\)", params_list, flags=regex_flags)
        is not None
    )


@pytest.mark.parametrize(
    "params_list",
    ["(abc, xZZ varchar(200))", '("my_real" REAL, "my_complex" COMPLEX)'],
    ids=["no-type", "bad-type"],
)
def test_parameter_list_pattern_error(params_list):
    assert (
        re.match(rf"\({parameter_list_pattern}\)", params_list, flags=regex_flags)
        is None
    )


@pytest.mark.parametrize(
    ["params", "expected_result"],
    [
        (
            " Pa1 INT , _var_PAR  varchar( 100 ) ",
            [
                {"name": "Pa1", "type": "INT"},
                {"name": "_var_PAR", "type": "varchar( 100 )"},
            ],
        ),
        (
            '"param1"  DECIMAL(3,2),"PARAM2" decimal(10, 0)',
            [
                {"name": "param1", "type": "DECIMAL(3,2)"},
                {"name": "PARAM2", "type": "decimal(10, 0)"},
            ],
        ),
        (
            "p1 varchar(3), ts2 timestamp(6 ) with local time zone, d3 decimal(10,5), "
            "h4 hashtype(4 byte),i5 interval year ( 5) to month",
            [
                {"name": "p1", "type": "varchar(3)"},
                {"name": "ts2", "type": "timestamp(6 ) with local time zone"},
                {"name": "d3", "type": "decimal(10,5)"},
                {"name": "h4", "type": "hashtype(4 byte)"},
                {"name": "i5", "type": "interval year ( 5) to month"},
            ],
        ),
        ('"P_1" INT', [{"name": "P_1", "type": "INT"}]),
        ('"1_P" INT', [{"name": "1_P", "type": "INT"}]),
        ("...", "..."),
    ],
    ids=[
        "non-quoted-names",
        "quoted-names",
        "complex-types",
        "single-parameter",
        "strange-name",
        "variadic",
    ],
)
def test_parse_parameter_list(func_parameter_parser, params, expected_result):
    result = func_parameter_parser.parse_parameter_list(params)
    assert result == expected_result


@pytest.mark.parametrize(
    ["info", "expected_result"],
    [
        (
            {
                "FUNCTION_SCHEMA": "MySchema",
                "FUNCTION_NAME": "SPAN_DISTANCE",
                "FUNCTION_TEXT": dedent(
                    """
                    FUNCTION SPAN_DISTANCE(begin1 INTEGER, end1 INTEGER, begin2 INTEGER, end2 INTEGER)
                    RETURN INTEGER
                    res INTEGER;
                    BEGIN
                        IF begin2 >= end1 THEN
                            res := begin2 - end1;
                        ELSE
                            res := NULL;
                        END IF;
                        RETURN res;
                    END;
                    /
                """
                ),
            },
            {
                "inputs": [
                    {"name": "begin1", "type": "INTEGER"},
                    {"name": "end1", "type": "INTEGER"},
                    {"name": "begin2", "type": "INTEGER"},
                    {"name": "end2", "type": "INTEGER"},
                ],
                "returns": {"type": "INTEGER"},
            },
        ),
        (
            {
                "FUNCTION_SCHEMA": "MySchema",
                "FUNCTION_NAME": "ValidateCredentials",
                "FUNCTION_TEXT": dedent(
                    """
                    FUNCTION "ValidateCredentials" (user_name VARCHAR(100), password VARCHAR(100))
                    RETURN BOOL
                    BEGIN ... END;
                """
                ),
            },
            {
                "inputs": [
                    {"name": "user_name", "type": "VARCHAR(100)"},
                    {"name": "password", "type": "VARCHAR(100)"},
                ],
                "returns": {"type": "BOOL"},
            },
        ),
        (
            {
                "FUNCTION_SCHEMA": "MySchema",
                "FUNCTION_NAME": "CIRCLE_AREA",
                "FUNCTION_TEXT": dedent(
                    """
                    FUNCTION "MySchema"."CIRCLE_AREA"(radius DOUBLE)
                    RETURN DOUBLE
                    BEGIN ... END;
                """
                ),
            },
            {
                "inputs": [{"name": "radius", "type": "DOUBLE"}],
                "returns": {"type": "DOUBLE"},
            },
        ),
        (
            {
                "FUNCTION_SCHEMA": "MySchema",
                "FUNCTION_NAME": "GetTimestamp",
                "FUNCTION_TEXT": dedent(
                    """
                    function MySchema.GetTimestamp()
                    return TIMESTAMP(8) WITH LOCAL TIME ZONE
                    begin ... end;
                """
                ),
            },
            {"inputs": [], "returns": {"type": "TIMESTAMP(8) WITH LOCAL TIME ZONE"}},
        ),
    ],
    ids=[
        "no-schema,no-quotes",
        "no-schema,quoted name",
        "schema,quotes",
        "schema,no-quotes,no-input",
    ],
)
def test_func_extract_parameters(func_parameter_parser, info, expected_result):
    result = func_parameter_parser.extract_parameters(info)
    assert result == expected_result


@pytest.mark.parametrize(
    "invalid_text",
    [
        dedent(
            """
            function MySchema.GetSkyHooks
            return VARCHAR(1000)
            begin ... end;
        """
        ),
        dedent(
            """
            function MySchema.GetSkyHooks()
            return COMPLEX
            begin ... end;
    """
        ),
    ],
    ids=["ino-inputs", "invalid-return-type"],
)
def test_func_extract_parameters_error(func_parameter_parser, invalid_text):
    info = {
        "FUNCTION_SCHEMA": "MySchema",
        "FUNCTION_NAME": "GetSkyHooks",
        "FUNCTION_TEXT": invalid_text,
    }
    with pytest.raises(ValueError, match="Failed to parse"):
        func_parameter_parser.extract_parameters(info)


@pytest.mark.parametrize(
    ["info", "expected_result"],
    [
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "TOTAL_LENGTH",
                "SCRIPT_LANGUAGE": "PYTHON3",
                "SCRIPT_INPUT_TYPE": "SET",
                "SCRIPT_RESULT_TYPE": "RETURNS",
                "SCRIPT_TEXT": dedent(
                    """
                    CREATE PYTHON3 SET SCRIPT "TOTAL_LENGTH" ("text" VARCHAR(100000) UTF8)
                    RETURNS INTEGER AS
                    def run(ctx):
                        more_data = True
                        result = 0
                        while more_data:
                            result += len(ctx.text)
                            more_data = ctx.next()
                        return result
                """
                ),
            },
            {
                "inputs": [{"name": "text", "type": "VARCHAR(100000) UTF8"}],
                "returns": {"type": "INTEGER"},
            },
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "COMBINED_SPAN",
                "SCRIPT_LANGUAGE": "LUA",
                "SCRIPT_INPUT_TYPE": "SCALAR",
                "SCRIPT_RESULT_TYPE": "EMITS",
                "SCRIPT_TEXT": dedent(
                    """
                    CREATE LUA SCALAR SCRIPT COMBINED_SPAN(
                        "begin1" DECIMAL(18,0), "end1" DECIMAL(18,0),
                        "begin2" DECIMAL(18,0), "end2" DECIMAL(18,0)
                    ) EMITS ("BEGIN" DECIMAL(18,0), "END" DECIMAL(18,0)) AS
                    function run(ctx)
                        ctx.emit(math.min(ctx.begin1, ctx.begin2), math.max(ctx.end1, ctx.end2))
                    end
                """
                ),
            },
            {
                "inputs": [
                    {"name": "begin1", "type": "DECIMAL(18,0)"},
                    {"name": "end1", "type": "DECIMAL(18,0)"},
                    {"name": "begin2", "type": "DECIMAL(18,0)"},
                    {"name": "end2", "type": "DECIMAL(18,0)"},
                ],
                "emits": [
                    {"name": "BEGIN", "type": "DECIMAL(18,0)"},
                    {"name": "END", "type": "DECIMAL(18,0)"},
                ],
            },
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "EXTRACT_PRODUCT_NAMES",
                "SCRIPT_LANGUAGE": "PYTHON3",
                "SCRIPT_INPUT_TYPE": "SET",
                "SCRIPT_RESULT_TYPE": "EMITS",
                "SCRIPT_TEXT": dedent(
                    """
                    CREATE PYTHON3 SET SCRIPT "MySchema"."Extract_Product_Names"(
                        "text" VARCHAR(100000)
                    ) EMITS (
                        "ProductName" VARCHAR(1000),
                        "Begins" DECIMAL(10,0),
                        "Ends" DECIMAL(10,0)
                    ) AS
                    def run(ctx):
                        ...
                """
                ),
            },
            {
                "inputs": [{"name": "text", "type": "VARCHAR(100000)"}],
                "emits": [
                    {"name": "ProductName", "type": "VARCHAR(1000)"},
                    {"name": "Begins", "type": "DECIMAL(10,0)"},
                    {"name": "Ends", "type": "DECIMAL(10,0)"},
                ],
            },
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "Bessel",
                "SCRIPT_LANGUAGE": "LUA",
                "SCRIPT_INPUT_TYPE": "SCALAR",
                "SCRIPT_RESULT_TYPE": "RETURNS",
                "SCRIPT_TEXT": dedent(
                    """
                    create lua scalar script MySchema.Bessel(n int, x double)
                    returns double as
                    function run(ctx)
                        ...
                    end
                """
                ),
            },
            {
                "inputs": [
                    {"name": "n", "type": "int"},
                    {"name": "x", "type": "double"},
                ],
                "returns": {"type": "double"},
            },
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "RANDOM_DAY_OF_WEEK",
                "SCRIPT_LANGUAGE": "PYTHON3",
                "SCRIPT_INPUT_TYPE": "SCALAR",
                "SCRIPT_RESULT_TYPE": "RETURNS",
                "SCRIPT_TEXT": dedent(
                    """
                    CREATE PYTHON3 SCALAR SCRIPT RANDOM_DAY_OF_WEEK ()
                    RETURNS VARCHAR(10) AS
                    def run(ctx):
                        ...
                """
                ),
            },
            {"inputs": [], "returns": {"type": "VARCHAR(10)"}},
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "COUNT_SHEEP",
                "SCRIPT_LANGUAGE": "PYTHON3",
                "SCRIPT_INPUT_TYPE": "SET",
                "SCRIPT_RESULT_TYPE": "RETURNS",
                "SCRIPT_TEXT": dedent(
                    """
                    CREATE PYTHON3 SET SCRIPT COUNT_SHEEP(...)
                    RETURNS INTEGER AS
                    def run(ctx):
                        ...
                """
                ),
            },
            {"inputs": "...", "returns": {"type": "INTEGER"}},
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "Greetings",
                "SCRIPT_LANGUAGE": "PYTHON3",
                "SCRIPT_INPUT_TYPE": "SCALAR",
                "SCRIPT_RESULT_TYPE": "EMITS",
                "SCRIPT_TEXT": dedent(
                    """
                    CREATE PYTHON3 SCALAR SCRIPT Greetings(name VARCHAR(100))
                    EMITS (...) AS
                    def run(ctx):
                        ...
                """
                ),
            },
            {
                "inputs": [
                    {"name": "name", "type": "VARCHAR(100)"},
                ],
                "emits": "...",
            },
        ),
    ],
    ids=[
        "set,returns",
        "scalar,emits",
        "set-emits",
        "scalar-returns",
        "no-input",
        "variadic-input",
        "variadic-emit",
    ],
)
def test_script_extract_parameters(script_parameter_parser, info, expected_result):
    result = script_parameter_parser.extract_parameters(info)
    assert result == expected_result


@pytest.mark.parametrize(
    ["result_type", "invalid_text"],
    [
        (
            "RETURNS",
            dedent(
                """
                CREATE PYTHON3 SCALAR SCRIPT MySchema.GetSkyHooks
                RETURNS VARCHAR(1000) AS
                def run(ctx):
                    ...
            """
            ),
        ),
        (
            "RETURNS",
            dedent(
                """
                CREATE PYTHON3 SCALAR SCRIPT MySchema.GetSkyHooks()
                RETURNS AS
                def run(ctx):
                    ...
            """
            ),
        ),
        (
            "EMITS",
            dedent(
                """
                CREATE PYTHON3 SCALAR SCRIPT MySchema.GetSkyHooks()
                EMITS AS
                def run(ctx):
                    ...
        """
            ),
        ),
    ],
    ids=["no-input", "no-return", "no-emit"],
)
def test_script_extract_parameters_error(
    script_parameter_parser, result_type, invalid_text
):
    info = {
        "SCRIPT_SCHEMA": "MySchema",
        "SCRIPT_NAME": "GetSkyHooks",
        "SCRIPT_LANGUAGE": "PYTHON3",
        "SCRIPT_INPUT_TYPE": "SCALAR",
        "SCRIPT_RESULT_TYPE": result_type,
        "SCRIPT_TEXT": invalid_text,
    }
    with pytest.raises(ValueError, match="Failed to parse"):
        script_parameter_parser.extract_parameters(info)


@mock.patch("exasol.ai.mcp.server.parameter_parser.ParameterParser._execute_query")
def test_describe(mock_execute_query, func_parameter_parser):
    mock_execute_query.return_value = [
        {
            "FUNCTION_SCHEMA": "MySchema",
            "FUNCTION_NAME": "Validate",
            "FUNCTION_TEXT": dedent(
                """
                FUNCTION "Validate"(user_name VARCHAR(100), password VARCHAR(100))
                RETURN BOOL
                BEGIN ... END;
            """
            ),
        },
    ]
    result = func_parameter_parser.describe(
        schema_name="MySchema", func_name="Validate"
    )
    expected_result = {
        "inputs": [
            {"name": "user_name", "type": "VARCHAR(100)"},
            {"name": "password", "type": "VARCHAR(100)"},
        ],
        "returns": {"type": "BOOL"},
    }
    assert result == expected_result


@mock.patch("exasol.ai.mcp.server.parameter_parser.ParameterParser._execute_query")
def test_describe_not_found(mock_execute_query, func_parameter_parser):
    mock_execute_query.return_value = []
    with pytest.raises(ValueError, match="not found"):
        func_parameter_parser.describe(schema_name="MySchema", func_name="Validate")


@mock.patch("exasol.ai.mcp.server.parameter_parser.ParameterParser._execute_query")
def test_describe_invalid_data(mock_execute_query, func_parameter_parser):
    mock_execute_query.return_value = [
        {
            "FUNCTION_SCHEMA": "MySchema",
            "FUNCTION_NAME": "Func1",
            "FUNCTION_TEXT": "FUNCTION Func1 RETURN BOOL BEGIN ... END;",
        }
    ]
    with pytest.raises(ValueError, match="Failed to parse"):
        func_parameter_parser.describe(schema_name="MySchema", func_name="Func1")
