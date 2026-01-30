from test.utils.result_utils import verify_result_table

from exasol.ai.mcp.server.mcp_resources import (
    builtin_function_categories,
    describe_builtin_function,
    list_builtin_functions,
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
    verify_result_table(
        result,
        key_column="name",
        other_columns=["description", "types", "usage-notes", "example"],
        expected_keys=["TO_DATE"],
    )
