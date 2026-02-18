from dataclasses import dataclass

import pytest

from exasol.ai.mcp.server.tools.schema.db_output_schema import DBColumn


@dataclass
class _TestCase:
    input_type: str
    input_params: list[DBColumn]
    dynamic_input: bool = False
    output_params: list[DBColumn] | None = None
    dynamic_output: bool = False
    expected_text: str = ""


@pytest.mark.parametrize(
    "test_case",
    [
        _TestCase(
            input_type="SET",
            input_params=[
                DBColumn(name="xx", type="CHAR(10)"),
                DBColumn(name="yy", type="INT"),
            ],
            expected_text=(
                "In most cases, an Exasol SET User Defined Function (UDF) can be "
                "called just like a normal aggregate function. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("xx", "yy") AS "RETURN_VALUE" '
                'FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "xx", "yy". '
                "Note that in an SQL query, the names of database objects, such as "
                "schemas, tables, UDFs, and columns should be enclosed in double quotes. "
                "A reference to a UDF should include a reference to its schema."
            ),
        ),
        _TestCase(
            input_type="SCALAR",
            input_params=[
                DBColumn(name="xx", type="CHAR(10)"),
                DBColumn(name="yy", type="INT"),
            ],
            output_params=[
                DBColumn(name="abc", type="DOUBLE"),
                DBColumn(name="efg", type="VARCHAR(100)"),
            ],
            expected_text=(
                "In most cases, an Exasol SCALAR User Defined Function (UDF) can be "
                "called just like a normal scalar function. Unlike normal scalar "
                "functions that return a single value for every input row, this UDF "
                "can emit multiple output rows per input row, each with 2 columns. "
                "An SQL SELECT statement calling a UDF that emits output columns, "
                "such as this one, cannot include any additional columns. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("xx", "yy") FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "xx", "yy". '
                'The query produces a result set with the columns ("abc", "efg"), '
                "similar to what is returned by a SELECT query. "
                "Note that in an SQL query, the names of database objects, such as "
                "schemas, tables, UDFs, and columns, including columns returned by "
                "the UDF, should be enclosed in double quotes. "
                "A reference to a UDF should include a reference to its schema."
            ),
        ),
        _TestCase(
            input_type="SCALAR",
            input_params=[],
            expected_text=(
                "In most cases, an Exasol SCALAR User Defined Function (UDF) can be "
                "called just like a normal scalar function. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"() AS "RETURN_VALUE" ```  '
                "Note that in an SQL query, the names of database objects, such as "
                "schemas, tables, UDFs, and columns should be enclosed in double quotes. "
                "A reference to a UDF should include a reference to its schema."
            ),
        ),
        _TestCase(
            input_type="SET",
            input_params=[],
            dynamic_input=True,
            expected_text=(
                "In most cases, an Exasol SET User Defined Function (UDF) can be called "
                "just like a normal aggregate function. This particular UDF has "
                "dynamic input parameters. The function comment may give a hint on "
                "what parameters are expected to be provided in a specific use case. "
                "Note that in the following example the input parameters are given "
                "only for illustration. They shall not be used as a guide on how to "
                "call this UDF. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("INPUT_1", "INPUT_2") AS "RETURN_VALUE" '
                'FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "INPUT_1", "INPUT_2". '
                "Note that in an SQL query, the names of database objects, such as "
                "schemas, tables, UDFs, and columns should be enclosed in double quotes. "
                "A reference to a UDF should include a reference to its schema."
            ),
        ),
        _TestCase(
            input_type="SCALAR",
            input_params=[
                DBColumn(name="xx", type="CHAR(10)"),
                DBColumn(name="yy", type="INT"),
            ],
            dynamic_output=True,
            expected_text=(
                "In most cases, an Exasol SCALAR User Defined Function (UDF) can "
                "be called just like a normal scalar function. Unlike normal scalar "
                "functions that return a single value for every input row, this UDF "
                "can emit multiple output rows per input row. An SQL SELECT statement "
                "calling a UDF that emits output columns, such as this one, cannot "
                "include any additional columns. This particular UDF has "
                "dynamic output parameters. The function comment may give a hint on "
                "what parameters are expected to be emitted in a specific use case. "
                "When calling a UDF with dynamic output parameters, the EMITS clause "
                "should be provided in the call, as demonstrated in the example below. "
                "Note that in the following example the output parameters are given "
                "only for illustration. They shall not be used as a guide on how to "
                "call this UDF. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("xx", "yy") EMITS ("OUTPUT_1" VARCHAR(100), '
                '"OUTPUT_2" DOUBLE) FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the "
                'table "MY_SOURCE_TABLE" with the following columns: "xx", "yy". '
                'The query produces a result set with the columns ("OUTPUT_1", '
                '"OUTPUT_2"), similar to what is returned by a SELECT query. '
                "Note that in an SQL query, the names of database objects, such as "
                "schemas, tables, UDFs, and columns, including columns returned by "
                "the UDF, should be enclosed in double quotes. "
                "A reference to a UDF should include a reference to its schema."
            ),
        ),
        _TestCase(
            input_type="SET",
            input_params=[],
            dynamic_input=True,
            dynamic_output=True,
            expected_text=(
                "In most cases, an Exasol SET User Defined Function (UDF) can be "
                "called just like a normal aggregate function. Unlike normal aggregate "
                "functions that return a single value for every input group, this UDF "
                "can emit multiple output rows per input group. An SQL SELECT statement "
                "calling a UDF that emits output columns, such as this one, cannot "
                "include any additional columns. This particular UDF "
                "has dynamic input and output parameters. The function comment may give "
                "a hint on what parameters are expected to be provided and emitted in a "
                "specific use case. When calling a UDF with dynamic output parameters, "
                "the EMITS clause should be provided in the call, as demonstrated in "
                "the example below. Note that in the following example the input and "
                "output parameters are given only for illustration. They shall not be "
                "used as a guide on how to call this UDF. "
                "Here is a usage example for this particular UDF: "
                '``` SELECT "my_udf"("INPUT_1", "INPUT_2") EMITS ("OUTPUT_1" VARCHAR(100), '
                '"OUTPUT_2" DOUBLE) FROM "MY_SOURCE_TABLE" ``` '
                "This example assumes that the currently opened schema has the table "
                '"MY_SOURCE_TABLE" with the following columns: "INPUT_1", "INPUT_2". '
                'The query produces a result set with the columns ("OUTPUT_1", "OUTPUT_2"), '
                "similar to what is returned by a SELECT query. "
                "Note that in an SQL query, the names of database objects, such as "
                "schemas, tables, UDFs, and columns, including columns returned by "
                "the UDF, should be enclosed in double quotes. "
                "A reference to a UDF should include a reference to its schema."
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
def test_get_udf_call_example(script_parameter_parser, test_case):
    example = script_parameter_parser.get_udf_call_example(
        input_type=test_case.input_type,
        func_name="my_udf",
        input_params=test_case.input_params,
        variadic_input=test_case.dynamic_input,
        output_params=test_case.output_params,
        variadic_emit=test_case.dynamic_output,
    )
    example = example.replace("\n", " ")
    assert example == test_case.expected_text
