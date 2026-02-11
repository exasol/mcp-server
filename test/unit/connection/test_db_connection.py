from contextlib import contextmanager
from typing import (
    Any,
    ContextManager,
)
from unittest.mock import MagicMock

import pyexasol
import pytest

from exasol.ai.mcp.server.connection.db_connection import DbConnection


def _create_mock_statement(val: Any) -> pyexasol.ExaStatement:
    statement = MagicMock(spec=pyexasol.ExaStatement)
    statement.fetchval.return_value = val
    return statement


def _create_mock_exception(
    ex_type: type[pyexasol.ExaError], connection: pyexasol.ExaConnection
) -> pyexasol.ExaError:
    if issubclass(ex_type, pyexasol.ExaRequestError):
        return ex_type(connection, code=666, message="error")
    return ex_type(connection, message="error")


class FakeConnectionFactory:
    def __init__(self, results: list[Any], snapshot: bool):
        self.connection = MagicMock(spec=pyexasol.ExaConnection)
        self.connection.options = {}
        self.connection.is_closed = False
        self.connection.close.side_effect = self._close
        side_effect = [
            (
                _create_mock_exception(res, self.connection)
                if isinstance(res, type) and issubclass(res, pyexasol.ExaError)
                else _create_mock_statement(res)
            )
            for res in results
        ]
        if snapshot:
            self.connection.meta = MagicMock(spec=pyexasol.ExaMetaData)
            self.connection.meta.execute_snapshot.side_effect = side_effect
        else:
            self.connection.execute.side_effect = side_effect
        self.conn_state = []

    def _close(self):
        self.connection.is_closed = True

    @contextmanager
    def __call__(self) -> ContextManager[pyexasol.ExaConnection]:
        self.connection.is_closed = False
        yield self.connection
        self.conn_state.append(self.connection.is_closed)


@pytest.fixture(params=[True, False])
def snapshot(request) -> bool:
    return request.param


def test_db_connection_execute_success(snapshot):
    """
    Tests the successful execution of a query first time.
    """
    factory = FakeConnectionFactory(results=[1], snapshot=snapshot)
    db_connection = DbConnection(factory)
    result = db_connection.execute_query("SELECT 1", snapshot=snapshot).fetchval()
    assert result == 1
    assert factory.conn_state == [False]


def test_db_connection_execute_failure(snapshot):
    """
    Tests the failure of a query execution first time.
    """
    results = [pyexasol.ExaRequestError, 1]
    factory = FakeConnectionFactory(results=results, snapshot=snapshot)
    db_connection = DbConnection(factory, num_retries=2)
    with pytest.raises(pyexasol.ExaRequestError):
        db_connection.execute_query("SELECT 1", snapshot=snapshot).fetchval()


def test_db_connection_execute_retry_success(snapshot):
    """
    Tests the successful execution of a query after a number of retries.
    """
    results = [
        pyexasol.ExaCommunicationError,
        pyexasol.ExaRuntimeError,
        pyexasol.ExaAuthError,
        1,
    ]
    factory = FakeConnectionFactory(results=results, snapshot=snapshot)
    db_connection = DbConnection(factory, num_retries=4)
    result = db_connection.execute_query("SELECT 1", snapshot=snapshot).fetchval()
    assert result == 1
    assert factory.conn_state == [True, True, True, False]


def test_db_connection_execute_retry_failure(snapshot):
    """
    Tests the failure after a number of retries.
    """
    results = [
        pyexasol.ExaCommunicationError,
        pyexasol.ExaRuntimeError,
        pyexasol.ExaAuthError,
        1,
    ]
    factory = FakeConnectionFactory(results=results, snapshot=snapshot)
    db_connection = DbConnection(factory, num_retries=3)
    with pytest.raises(pyexasol.ExaAuthError):
        db_connection.execute_query("SELECT 1", snapshot=snapshot)
