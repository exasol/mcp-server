"""
Integration tests verifying that execute_exasol_query (and other DB-backed tools,
which all route through DbConnection.execute_query) never leak pyexasol/Exasol-
server internals (DSN, DB/OS username, OS name, driver/client version, session id)
to the MCP client. See GitHub issue #256.
"""

from test.utils.tool_utils import run_tool

import pytest
from fastmcp.exceptions import ToolError

from exasol.ai.mcp.server.setup.server_settings import McpServerSettings

# Substrings that must never appear in a client-facing error message, regardless
# of which pyexasol error triggered it.
_SENSITIVE_SUBSTRINGS = ("os_user", "os_name", "session_id", "dsn", "pyexasol")


def _assert_no_sensitive_details(message: str, pyexasol_connection) -> None:
    lower = message.lower()
    for substring in _SENSITIVE_SUBSTRINGS:
        assert substring not in lower, f"leaked {substring!r} in: {message}"
    assert pyexasol_connection.options["dsn"] not in message
    assert pyexasol_connection.options["user"] not in message


def test_execute_query_object_not_found_is_sanitized(pyexasol_connection):
    """
    SELECT against a schema/table that doesn't exist raises an ExaQueryError.
    The client should see useful, query-specific detail but no connection/session
    internals.
    """
    config = McpServerSettings(enable_read_query=True)
    with pytest.raises(ToolError) as exc_info:
        run_tool(
            pyexasol_connection,
            config,
            tool_name="execute_exasol_query",
            query="SELECT * FROM NONEXISTENT_SCHEMA.NONEXISTENT_TABLE",
        )
    message = str(exc_info.value)
    _assert_no_sensitive_details(message, pyexasol_connection)
    assert "NONEXISTENT_TABLE" in message


def test_execute_query_syntax_error_is_sanitized(pyexasol_connection):
    """
    A malformed SELECT that sqlglot's lenient parser still accepts (so it is not
    rejected by verify_query) but that the Exasol server itself rejects with a
    genuine syntax error: an ExaQueryError.
    """
    config = McpServerSettings(enable_read_query=True)
    with pytest.raises(ToolError) as exc_info:
        run_tool(
            pyexasol_connection,
            config,
            tool_name="execute_exasol_query",
            query="SELECT 1,",
        )
    message = str(exc_info.value)
    _assert_no_sensitive_details(message, pyexasol_connection)


def test_execute_query_multi_statement_is_sanitized(pyexasol_connection):
    """
    Regression test for the original issue repro: "SELECT 1; SELECT 2" passes
    verify_query's sqlglot-based check (parse_one silently parses only the first
    statement) but is rejected by the Exasol server itself, since a single
    EXECUTE call may not contain multiple statements.
    """
    config = McpServerSettings(enable_read_query=True)
    with pytest.raises(ToolError) as exc_info:
        run_tool(
            pyexasol_connection,
            config,
            tool_name="execute_exasol_query",
            query="SELECT 1; SELECT 2",
        )
    message = str(exc_info.value)
    _assert_no_sensitive_details(message, pyexasol_connection)
