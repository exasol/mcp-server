import pytest

from exasol.ai.mcp.server.parameter_parser import VARIADIC_MARKER


@pytest.mark.parametrize(
    ["input_type", "result", "expected_text"],
    [
        (
            "SET",
            {
                "inputs": [
                    {"name": "xx", "type": "CHAR(10)"},
                    {"name": "yy", "type": "INT"},
                ],
                "returns": {"type": "DOUBLE"},
            },
            (
                "In most cases, an Exasol SET User Defined Function (UDF) can be "
                "called just like a normal aggregate function. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("xx", "yy") AS "RETURN_VALUE" '
                'FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "xx", "yy".'
            ),
        ),
        (
            "SCALAR",
            {
                "inputs": [
                    {"name": "xx", "type": "CHAR(10)"},
                    {"name": "yy", "type": "INT"},
                ],
                "emits": [
                    {"name": "abc", "type": "DOUBLE"},
                    {"name": "efg", "type": "VARCHAR(100)"},
                ],
            },
            (
                "In most cases, an Exasol SCALAR User Defined Function (UDF) can be "
                "called just like a normal scalar function. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("xx", "yy") FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "xx", "yy". '
                'The query produces a result set with the columns ("abc", "efg"), '
                "similar to what is returned by a SELECT query."
            ),
        ),
        (
            "SCALAR",
            {"inputs": [], "returns": {"type": "DOUBLE"}},
            (
                "In most cases, an Exasol SCALAR User Defined Function (UDF) can be "
                "called just like a normal scalar function. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"() AS "RETURN_VALUE" ``` '
            ),
        ),
        (
            "SET",
            {"inputs": VARIADIC_MARKER, "returns": {"type": "DOUBLE"}},
            (
                "In most cases, an Exasol SET User Defined Function (UDF) can be called "
                "just like a normal aggregate function. However, this particular UDF "
                "has dynamic input parameters. The function_comment may give a hint on "
                "what parameters are expected to be provided in a specific use case. "
                "Note that in the following example the input parameters are given "
                "only for illustration. They shall not be used as a guide on how to "
                "call this UDF. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("INPUT_1", "INPUT_2") AS "RETURN_VALUE" '
                'FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "INPUT_1", "INPUT_2".'
            ),
        ),
        (
            "SCALAR",
            {
                "inputs": [
                    {"name": "xx", "type": "CHAR(10)"},
                    {"name": "yy", "type": "INT"},
                ],
                "emits": VARIADIC_MARKER,
            },
            (
                "In most cases, an Exasol SCALAR User Defined Function (UDF) can "
                "be called just like a normal scalar function. However, this "
                "particular UDF has dynamic output parameters. The function_comment "
                "may give a hint on what parameters are expected to be emitted in a "
                "specific use case. When calling a UDF with dynamic output "
                "parameters, the EMITS clause should be provided in the call, as "
                "demonstrated in the example below. Note that in the following "
                "example the output parameters are given only for illustration. "
                "They shall not be used as a guide on how to call this UDF. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("xx", "yy") EMITS ("OUTPUT_1" VARCHAR(100), '
                '"OUTPUT_2" DOUBLE) FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the "
                'table "MY_SOURCE_TABLE" with the following columns: "xx", "yy". '
                'The query produces a result set with the columns ("OUTPUT_1", '
                '"OUTPUT_2"), similar to what is returned by a SELECT query.'
            ),
        ),
        (
            "SET",
            {"inputs": VARIADIC_MARKER, "emits": VARIADIC_MARKER},
            (
                "In most cases, an Exasol SET User Defined Function (UDF) can be "
                "called just like a normal aggregate function. However, this "
                "particular UDF has dynamic input and output parameters. The "
                "function_comment may give a hint on what parameters are expected to be "
                "provided and emitted in a specific use case. When calling a UDF with "
                "dynamic output parameters, the EMITS clause should be provided in the "
                "call, as demonstrated in the example below. Note that in the following "
                "example the input and output parameters are given only for illustration. "
                "They shall not be used as a guide on how to call this UDF. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("INPUT_1", "INPUT_2") EMITS ("OUTPUT_1" VARCHAR(100), '
                '"OUTPUT_2" DOUBLE) FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "INPUT_1", "INPUT_2". '
                'The query produces a result set with the columns ("OUTPUT_1", "OUTPUT_2"), '
                "similar to what is returned by a SELECT query."
            ),
        ),
    ],
    ids=[
        "set-returns",
        "scalar-emits",
        "no-input",
        "variadic-input",
        "variadic-emit",
        "variadic-both",
    ],
)
def test_get_udf_call_example(
    script_parameter_parser, input_type, result, expected_text
):
    example = script_parameter_parser.get_udf_call_example(
        result, input_type=input_type, func_name="my_udf"
    )
    example = example.replace("\n", " ")
    assert example == expected_text
