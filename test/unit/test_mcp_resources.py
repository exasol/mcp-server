from test.utils.result_utils import verify_result_table

from exasol.ai.mcp.server.mcp_resources import (
    describe_builtin_function,
    list_builtin_functions,
)


def test_list_builtin_functions() -> None:
    result = list_builtin_functions("numeric")
    verify_result_table(
        result,
        key_column="name",
        other_columns=["description"],
        expected_keys=["CEILING", "DEGREES"],
    )


def test_describe_builtin_functions() -> None:
    result = describe_builtin_function("to_date")
    verify_result_table(
        result,
        key_column="name",
        other_columns=["description", "types", "usage-notes", "example"],
        expected_keys=["TO_DATE"],
    )
