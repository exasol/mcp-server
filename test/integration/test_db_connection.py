import pyexasol
import pytest

from exasol.ai.mcp.server.db_connection import DbConnection


@pytest.mark.parametrize("snapshot", [True, False])
def test_db_connection_execute(backend_aware_database_params, snapshot):
    db_connection = DbConnection(
        lambda: pyexasol.connect(**backend_aware_database_params)
    )
    result = db_connection.execute_query("SELECT 1", snapshot=snapshot).fetchval()
    assert result == 1


def test_db_connection_reconnect(backend_aware_database_params):
    """
    The test validates that if the connection the wrapper keeps gets closed
    the wrapper will request another one.
    """
    connection: pyexasol.ExaConnection | None = None
    call_count = 0

    def connection_factory() -> pyexasol.ExaConnection:
        nonlocal connection, call_count
        connection = pyexasol.connect(**backend_aware_database_params)
        call_count += 1
        return connection

    db_connection = DbConnection(connection_factory)

    result = db_connection.execute_query("SELECT 1").fetchval()
    assert result == 1
    assert call_count == 1
    assert connection is not None
    connection.close()
    result = db_connection.execute_query("SELECT 2").fetchval()
    assert result == 2
    assert call_count == 2


def test_db_connection_error(backend_aware_database_params):
    """
    The test validates that a request to execute an invalid query
    results in exception, but does not close the connection.
    """
    call_count = 0

    def connection_factory() -> pyexasol.ExaConnection:
        nonlocal call_count
        call_count += 1
        return pyexasol.connect(**backend_aware_database_params)

    db_connection = DbConnection(connection_factory)

    with pytest.raises(pyexasol.ExaRequestError):
        db_connection.execute_query("SELECT 1 2")

    assert call_count == 1
    result = db_connection.execute_query("SELECT 1").fetchval()
    assert result == 1
    assert call_count == 1
