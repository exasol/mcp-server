from textwrap import dedent
from unittest import mock

import pytest

from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    DBColumn,
    DBEmitFunction,
    DBReturnFunction,
)


@pytest.mark.parametrize(
    ["params", "expected_result"],
    [
        (
            " Pa1 INT , _var_PAR  varchar( 100 ) ",
            [
                DBColumn(name="Pa1", type="INT"),
                DBColumn(name="_var_PAR", type="varchar( 100 )"),
            ],
        ),
        (
            '"param1"  DECIMAL(3,2),"PARAM2" decimal(10, 0)',
            [
                DBColumn(name="param1", type="DECIMAL(3,2)"),
                DBColumn(name="PARAM2", type="decimal(10, 0)"),
            ],
        ),
        (
            "p1 varchar(3), ts2 timestamp(6 ) with local time zone, d3 decimal(10,5), "
            "h4 hashtype(4 byte),i5 interval year ( 5) to month",
            [
                DBColumn(name="p1", type="varchar(3)"),
                DBColumn(name="ts2", type="timestamp(6 ) with local time zone"),
                DBColumn(name="d3", type="decimal(10,5)"),
                DBColumn(name="h4", type="hashtype(4 byte)"),
                DBColumn(name="i5", type="interval year ( 5) to month"),
            ],
        ),
        ('"P_1" INT', [DBColumn(name="P_1", type="INT")]),
        ('"1_P" INT', [DBColumn(name="1_P", type="INT")]),
    ],
    ids=[
        "non-quoted-names",
        "quoted-names",
        "complex-types",
        "single-parameter",
        "strange-name",
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
                "FUNCTION_COMMENT": "Function comment",
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
            DBReturnFunction(
                schema="MySchema",
                name="SPAN_DISTANCE",
                comment="Function comment",
                input=[
                    DBColumn(name="begin1", type="INTEGER"),
                    DBColumn(name="end1", type="INTEGER"),
                    DBColumn(name="begin2", type="INTEGER"),
                    DBColumn(name="end2", type="INTEGER"),
                ],
                returns="INTEGER",
            ),
        ),
        (
            {
                "FUNCTION_SCHEMA": "MySchema",
                "FUNCTION_NAME": "ValidateCredentials",
                "FUNCTION_COMMENT": "Function comment",
                "FUNCTION_TEXT": dedent(
                    """
                    FUNCTION "ValidateCredentials" (user_name VARCHAR(100), password VARCHAR(100))
                    RETURN BOOL
                    BEGIN ... END;
                """
                ),
            },
            DBReturnFunction(
                schema="MySchema",
                name="ValidateCredentials",
                comment="Function comment",
                input=[
                    DBColumn(name="user_name", type="VARCHAR(100)"),
                    DBColumn(name="password", type="VARCHAR(100)"),
                ],
                returns="BOOL",
            ),
        ),
        (
            {
                "FUNCTION_SCHEMA": "MySchema",
                "FUNCTION_NAME": "CIRCLE_AREA",
                "FUNCTION_COMMENT": "Function comment",
                "FUNCTION_TEXT": dedent(
                    """
                    FUNCTION "MySchema"."CIRCLE_AREA"(radius DOUBLE)
                    RETURN DOUBLE
                    BEGIN ... END;
                """
                ),
            },
            DBReturnFunction(
                schema="MySchema",
                name="CIRCLE_AREA",
                comment="Function comment",
                input=[DBColumn(name="radius", type="DOUBLE")],
                returns="DOUBLE",
            ),
        ),
        (
            {
                "FUNCTION_SCHEMA": "MySchema",
                "FUNCTION_NAME": "GetTimestamp",
                "FUNCTION_COMMENT": "Function comment",
                "FUNCTION_TEXT": dedent(
                    """
                    function MySchema.GetTimestamp()
                    return TIMESTAMP(8) WITH LOCAL TIME ZONE
                    begin ... end;
                """
                ),
            },
            DBReturnFunction(
                schema="MySchema",
                name="GetTimestamp",
                comment="Function comment",
                input=[],
                returns="TIMESTAMP(8) WITH LOCAL TIME ZONE",
            ),
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
                "SCRIPT_COMMENT": "Script comment",
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
            DBReturnFunction(
                schema="MySchema",
                name="TOTAL_LENGTH",
                comment="Script comment",
                input=[DBColumn(name="text", type="VARCHAR(100000) UTF8")],
                returns="INTEGER",
            ),
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "COMBINED_SPAN",
                "SCRIPT_COMMENT": "Script comment",
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
            DBEmitFunction(
                schema="MySchema",
                name="COMBINED_SPAN",
                comment="Script comment",
                input=[
                    DBColumn(name="begin1", type="DECIMAL(18,0)"),
                    DBColumn(name="end1", type="DECIMAL(18,0)"),
                    DBColumn(name="begin2", type="DECIMAL(18,0)"),
                    DBColumn(name="end2", type="DECIMAL(18,0)"),
                ],
                emits=[
                    DBColumn(name="BEGIN", type="DECIMAL(18,0)"),
                    DBColumn(name="END", type="DECIMAL(18,0)"),
                ],
            ),
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "EXTRACT_PRODUCT_NAMES",
                "SCRIPT_COMMENT": "Script comment",
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
            DBEmitFunction(
                schema="MySchema",
                name="EXTRACT_PRODUCT_NAMES",
                comment="Script comment",
                input=[DBColumn(name="text", type="VARCHAR(100000)")],
                emits=[
                    DBColumn(name="ProductName", type="VARCHAR(1000)"),
                    DBColumn(name="Begins", type="DECIMAL(10,0)"),
                    DBColumn(name="Ends", type="DECIMAL(10,0)"),
                ],
            ),
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "Bessel",
                "SCRIPT_COMMENT": "Script comment",
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
            DBReturnFunction(
                schema="MySchema",
                name="Bessel",
                comment="Script comment",
                input=[
                    DBColumn(name="n", type="int"),
                    DBColumn(name="x", type="double"),
                ],
                returns="double",
            ),
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "RANDOM_DAY_OF_WEEK",
                "SCRIPT_COMMENT": "Script comment",
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
            DBReturnFunction(
                schema="MySchema",
                name="RANDOM_DAY_OF_WEEK",
                comment="Script comment",
                input=[],
                returns="VARCHAR(10)",
            ),
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "COUNT_SHEEP",
                "SCRIPT_COMMENT": "Script comment",
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
            DBReturnFunction(
                schema="MySchema",
                name="COUNT_SHEEP",
                comment="Script comment",
                input=[],
                dynamic_input=True,
                returns="INTEGER",
            ),
        ),
        (
            {
                "SCRIPT_SCHEMA": "MySchema",
                "SCRIPT_NAME": "Greetings",
                "SCRIPT_COMMENT": "Script comment",
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
            DBEmitFunction(
                schema="MySchema",
                name="Greetings",
                comment="Script comment",
                input=[
                    DBColumn(name="name", type="VARCHAR(100)"),
                ],
                emits=[],
                dynamic_output=True,
            ),
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
    assert result.model_dump(exclude={"usage"}) == expected_result.model_dump(
        exclude={"usage"}
    )
    assert result.usage


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


@mock.patch(
    "exasol.ai.mcp.server.tools.parameter_parser.ParameterParser._execute_query"
)
def test_describe(mock_execute_query, func_parameter_parser):
    mock_execute_query.return_value = [
        {
            "FUNCTION_SCHEMA": "MySchema",
            "FUNCTION_NAME": "Validate",
            "FUNCTION_COMMENT": "Credentials validation function",
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
    expected_result = DBReturnFunction(
        schema="MySchema",
        name="Validate",
        comment="Credentials validation function",
        input=[
            DBColumn(name="user_name", type="VARCHAR(100)"),
            DBColumn(name="password", type="VARCHAR(100)"),
        ],
        returns="BOOL",
    )
    assert result == expected_result


@mock.patch(
    "exasol.ai.mcp.server.tools.parameter_parser.ParameterParser._execute_query"
)
def test_describe_not_found(mock_execute_query, func_parameter_parser):
    mock_execute_query.return_value = []
    with pytest.raises(ValueError, match="not found"):
        func_parameter_parser.describe(schema_name="MySchema", func_name="Validate")


@mock.patch(
    "exasol.ai.mcp.server.tools.parameter_parser.ParameterParser._execute_query"
)
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
