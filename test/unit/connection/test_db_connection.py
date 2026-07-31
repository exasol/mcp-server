import logging
from contextlib import contextmanager
from typing import (
    Any,
    ContextManager,
)
from unittest.mock import MagicMock

import pyexasol
import pytest

from exasol.ai.mcp.server.connection.db_connection import (
    GENERIC_DB_ERROR_MESSAGE,
    QUERY_ERROR_PREFIX,
    DbConnection,
)


def _create_mock_statement(val: Any) -> pyexasol.ExaStatement:
    statement = MagicMock(spec=pyexasol.ExaStatement)
    statement.fetchval.return_value = val
    return statement


def _create_mock_exception(
    ex_type: type[pyexasol.ExaError], connection: pyexasol.ExaConnection
) -> pyexasol.ExaError:
    if issubclass(ex_type, pyexasol.ExaQueryError):
        return ex_type(connection, query="SELECT 1", code=666, message="error")
    if issubclass(ex_type, pyexasol.ExaRequestError):
        return ex_type(connection, code=666, message="error")
    return ex_type(connection, message="error")


class FakeConnectionFactory:
    def __init__(self, results: list[Any], snapshot: bool):
        self.connection = MagicMock(spec=pyexasol.ExaConnection)
        # A real ExaConnection always carries these keys (verbose_error defaults to
        # True in pyexasol); ExaError.__str__() needs them to render, which the
        # sanitizing log call in db_connection.py relies on.
        self.connection.options = {
            "verbose_error": True,
            "dsn": "mock-dsn",
            "user": "mock-user",
        }
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
    def __call__(self, no_auth: bool = False) -> ContextManager[pyexasol.ExaConnection]:
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
    Tests that a non-retryable, non-query pyexasol error (ExaRequestError) is
    sanitized to a generic connection-tier RuntimeError, with the original
    exception preserved as __cause__.
    """
    results = [pyexasol.ExaRequestError, 1]
    factory = FakeConnectionFactory(results=results, snapshot=snapshot)
    db_connection = DbConnection(factory, num_retries=2)
    with pytest.raises(RuntimeError) as exc_info:
        db_connection.execute_query("SELECT 1", snapshot=snapshot)
    assert str(exc_info.value) == GENERIC_DB_ERROR_MESSAGE
    assert isinstance(exc_info.value.__cause__, pyexasol.ExaRequestError)


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
    Tests that after retries are exhausted, the last error (ExaAuthError) is
    sanitized to a generic connection-tier RuntimeError, with the original
    exception preserved as __cause__.
    """
    results = [
        pyexasol.ExaCommunicationError,
        pyexasol.ExaRuntimeError,
        pyexasol.ExaAuthError,
        1,
    ]
    factory = FakeConnectionFactory(results=results, snapshot=snapshot)
    db_connection = DbConnection(factory, num_retries=3)
    with pytest.raises(RuntimeError) as exc_info:
        db_connection.execute_query("SELECT 1", snapshot=snapshot)
    assert str(exc_info.value) == GENERIC_DB_ERROR_MESSAGE
    assert isinstance(exc_info.value.__cause__, pyexasol.ExaAuthError)


@pytest.mark.parametrize(
    "ex_type",
    [
        pyexasol.ExaQueryError,
        pyexasol.ExaQueryTimeoutError,
        pyexasol.ExaQueryAbortError,
    ],
)
def test_db_connection_execute_query_error_detail_preserved(snapshot, ex_type):
    """
    Tests that ExaQueryError (and subclasses) surface the original `.message` to
    the caller: this exception family's `.message` does not carry
    dsn/user/schema/session_id info (unlike the generic ExaError `__str__` output).
    """
    factory = FakeConnectionFactory(results=[ex_type], snapshot=snapshot)
    db_connection = DbConnection(factory, num_retries=2)
    with pytest.raises(RuntimeError) as exc_info:
        db_connection.execute_query("SELECT 1", snapshot=snapshot)
    assert str(exc_info.value) == f"{QUERY_ERROR_PREFIX}error"
    assert isinstance(exc_info.value.__cause__, ex_type)


def test_db_connection_execute_non_pyexasol_error_propagates_unmodified(snapshot):
    """
    Tests that a bug in our own code (a non-pyexasol exception) is not caught or
    sanitized by execute_query, and propagates unmodified.
    """
    factory = FakeConnectionFactory(results=[1], snapshot=snapshot)
    if snapshot:
        factory.connection.meta.execute_snapshot.side_effect = TypeError("boom")
    else:
        factory.connection.execute.side_effect = TypeError("boom")
    db_connection = DbConnection(factory, num_retries=2)
    with pytest.raises(TypeError, match="boom"):
        db_connection.execute_query("SELECT 1", snapshot=snapshot)


def test_db_connection_execute_error_is_logged_with_full_detail(snapshot, caplog):
    """
    Tests that the original exception's full detail is logged server-side at
    WARNING level (not just the bare fact that something failed), while the
    exception raised to the caller only carries the sanitized message.
    """
    factory = FakeConnectionFactory(results=[pyexasol.ExaQueryError], snapshot=snapshot)
    db_connection = DbConnection(factory, num_retries=2)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(RuntimeError):
            db_connection.execute_query("SELECT 1", snapshot=snapshot)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    assert "error" in record.getMessage()
