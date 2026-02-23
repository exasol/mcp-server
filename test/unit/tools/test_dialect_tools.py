from test.utils.result_utils import verify_result_table

from exasol.ai.mcp.server.tools.dialect_tools import (
    builtin_function_categories,
    describe_builtin_function,
    list_builtin_functions,
)
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    CATEGORIES_FIELD,
    DESCRIPTION_FIELD,
    EXAMPLE_FIELD,
    NAME_FIELD,
    USAGE_FIELD,
)


def test_builtin_function_categories() -> None:
    result = builtin_function_categories()
    assert len(result) == len(set(result))
    assert all(
        expected_name in result for expected_name in ["numeric", "string", "analytic"]
    )


def test_list_builtin_functions() -> None:
    result = list_builtin_functions("numeric")
    assert all(expected_name in result for expected_name in ["CEILING", "DEGREES"])


def test_describe_builtin_functions() -> None:
    result = describe_builtin_function("to_date")
    result_json = [row.model_dump() for row in result]
    verify_result_table(
        result_json,
        key_column=NAME_FIELD,
        other_columns=[DESCRIPTION_FIELD, CATEGORIES_FIELD, USAGE_FIELD, EXAMPLE_FIELD],
        expected_keys=["TO_DATE"],
    )
