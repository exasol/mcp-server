import logging
from collections.abc import Callable
from contextlib import suppress
from typing import (
    Any,
    ContextManager,
    TypeVar,
)

from pyexasol import (
    ExaAuthError,
    ExaCommunicationError,
    ExaConnection,
    ExaError,
    ExaQueryError,
    ExaRuntimeError,
    ExaStatement,
)

logger = logging.getLogger(__name__)

GENERIC_DB_ERROR_TEMPLATE = (
    "A database error occurred ({error_type}). Contact your administrator if the "
    "problem persists."
)

QUERY_ERROR_PREFIX = "The database rejected the query: "


def generic_db_error_message(error_type: type[ExaError]) -> str:
    """
    The sanitized message returned to the client for a non-query pyexasol error.
    It names the error type but carries no other detail.
    """
    return GENERIC_DB_ERROR_TEMPLATE.format(error_type=error_type.__name__)


T = TypeVar("T")


def _identity(statement: ExaStatement) -> ExaStatement:
    return statement


def fetchall(statement: ExaStatement) -> list[Any]:
    """`fetch` callable for `DbConnection.execute_query`: fetches all rows."""
    return statement.fetchall()


def fetchcol(statement: ExaStatement) -> list[Any]:
    """`fetch` callable for `DbConnection.execute_query`: fetches the first column."""
    return statement.fetchcol()


def fetchone(statement: ExaStatement) -> Any:
    """`fetch` callable for `DbConnection.execute_query`: fetches one row."""
    return statement.fetchone()


def fetchval(statement: ExaStatement) -> Any:
    """`fetch` callable for `DbConnection.execute_query`: fetches one value."""
    return statement.fetchval()


class DbConnection:
    """
    This is a pyexasol connection wrapper. It requests a new connection for each query
    it executes. The returned connections can be cached but this class doesn't need to
    know about this.

    Any pyexasol error (`ExaError` subclass) that would otherwise propagate is
    sanitized before it reaches the caller - see `execute_query` for details - so
    that internal connection/session details are not exposed to an MCP client,
    unless `verbose_errors` is enabled.

    Args:
        connection_factory:
            Supplied factory that creates a connection. The connection should be created
            with `fetch_dict`=True. The wrapper sets this option to True anyway. The
            dictionary option is required in order to present the result in a json form.
            This is what FastMCP expects from a tool.
        num_retries:
            Number of attempts to execute a query before raising an exception.
        verbose_errors:
            If True, pyexasol errors are not sanitized: the caller receives the
            full error text, as rendered by pyexasol, which (with the pyexasol
            `verbose_error` option on) includes the DSN, user, schema, session id
            and, for query errors, the query text.
    """

    def __init__(
        self,
        connection_factory: Callable[..., ContextManager[ExaConnection]],
        num_retries: int = 2,
        verbose_errors: bool = False,
    ) -> None:
        self._conn_factory = connection_factory
        self._num_retries = num_retries
        self._verbose_errors = verbose_errors

    def execute_query(
        self,
        query: str | list[str],
        snapshot: bool = True,
        no_auth: bool = False,
        fetch: Callable[[ExaStatement], T] = _identity,
    ) -> T:
        """
        Will make the set number of attempts to execute the provided query. A repeated
        attempt may follow an `ExaCommunicationError` or `ExaAuthError` raised during
        the query execution, since these can be transient (a network blip, a briefly
        stale token) and may succeed on a fresh connection. `ExaRuntimeError` is never
        retried: pyexasol only raises it for client-side precondition failures (e.g.
        duplicate column names in the result set, an already-closed connection) that
        are a function of the query/arguments themselves, not of connection state -
        retrying would only reproduce the identical error. The connection is still
        discarded, out of caution.

        `fetch` is called on the resulting statement while the connection is still
        checked out of the pool, and inside this method's sanitizing try/except, so
        callers should retrieve rows through it (e.g. `fetch=fetchall`, using the
        module-level helper above) rather than calling `.fetchall()`/`.fetchval()`/etc.
        on the returned value afterwards. A large result set is fetched in chunks, each
        a separate request over the connection; once the connection is back in the pool
        another, concurrent, request may be using it.

        An `ExaError` raised while fetching - for example a connection failure while
        pulling a later chunk of a large result set - is sanitized like one raised
        during query execution, but it's never retried: the query has already run, and
        a retry would run it again. The connection is discarded.

        The statement is closed after `fetch`, also while the connection is checked
        out, whether `fetch` succeeds or not. A failure to close it is ignored. The
        default `fetch` returns the statement unchanged, already closed, for callers
        that only execute a statement for its side effect (e.g. DDL/DML) and never
        fetch from it.

        Any `ExaError` that ultimately reaches this method (whether from establishing
        the connection, from an unretried/retry-exhausted query failure, or from the
        `fetch` call) is sanitized before being raised to the caller:

        - The full original exception is always logged server-side at WARNING level
          first (including the DSN, database user, schema, session id and, for
          query errors, the query text, which pyexasol adds to the exception text
          when its `verbose_error` option is on), then a `RuntimeError` is raised
          `from` it, so the original exception remains available as `__cause__` for
          server-side debugging, even though it is never shown to the client.
        - For `ExaQueryError` (and its subclasses `ExaQueryTimeoutError`,
          `ExaQueryAbortError`) - raised for actual SQL EXECUTE failures such as
          syntax errors or references to non-existent objects - the `RuntimeError`
          message includes the original `.message` text, which is safe to expose.
        - For every other `ExaError` subtype (authentication, connection,
          communication or concurrency errors) a generic message is used instead,
          which only names the exception type, since these categories can embed
          connection/session details directly in `.message`.
        - If `verbose_errors` was set in the constructor, the `RuntimeError`
          message is instead the exception type name followed by the full `str()`
          of the original exception, for every `ExaError` subtype. Query errors
          keep the `QUERY_ERROR_PREFIX`, so that the client can still tell them
          apart from other errors.

        Non-pyexasol exceptions (i.e. bugs in our own code, such as a `TypeError`)
        are not caught here and propagate unmodified.

        If snapshot is True, which should be the mode of choice for querying metadata,
        the `meta.execute_snapshot` method will be called. Otherwise, it will use the
        normal `execute` method.

        If a list of queries is provided, all statements are executed in a single
        session and the result of the last one is returned. The statement of each
        earlier query is closed before the next query runs.

        If no_auth is True, the connection factory will skip the OAuth username claim
        check. This is intended for unauthenticated internal callers such as the health
        check endpoint.
        """
        try:
            return self._execute_with_retries(
                query, snapshot=snapshot, no_auth=no_auth, fetch=fetch
            )
        except ExaError as ex:
            raise RuntimeError(self._client_error_message(ex)) from ex

    def _client_error_message(self, ex: ExaError) -> str:
        """
        Logs a pyexasol error server-side with full detail, and builds the error
        message returned to the caller (and ultimately to the MCP client). See
        `execute_query` for the rules.
        """
        is_query_error = isinstance(ex, ExaQueryError)
        logger.warning(
            (
                "Query execution failed with a database error"
                if is_query_error
                else "A pyexasol error occurred while executing a query"
            ),
            exc_info=ex,
        )
        prefix = QUERY_ERROR_PREFIX if is_query_error else ""
        if self._verbose_errors:
            # pyexasol's verbose text is wrapped in newlines; strip them.
            return f"{prefix}{type(ex).__name__}: {str(ex).strip()}"
        if is_query_error:
            return f"{prefix}{ex.message}"
        return generic_db_error_message(type(ex))

    def _execute_with_retries(
        self,
        query: str | list[str],
        snapshot: bool,
        no_auth: bool,
        fetch: Callable[[ExaStatement], T],
    ) -> T:
        queries = [query] if isinstance(query, str) else query
        attempt = 1
        while True:
            with self._conn_factory(no_auth=no_auth) as connection:
                connection.options["fetch_dict"] = True
                try:
                    statement = self._run_queries(
                        connection, queries, snapshot=snapshot
                    )
                except ExaRuntimeError:
                    # A client-side precondition failure, not a connection problem.
                    # The same query would fail with the same error on any
                    # connection, so it's not retried; the connection is discarded
                    # out of caution, since the error may reflect it being in an
                    # unexpected state. close() itself is not expected to fail here,
                    # but it must not be allowed to mask the original error if it does.
                    try:
                        connection.close()
                    except ExaError:
                        pass
                    raise
                except (ExaCommunicationError, ExaAuthError):
                    connection.close()
                    if attempt == self._num_retries:
                        raise
                    attempt += 1
                    continue

                # Fetch and close the statement before leaving the `with` block,
                # which returns the connection to the pool.
                try:
                    return fetch(statement)
                except ExaError:
                    # The query has run, so it's not retried: that would run it
                    # again, repeating any side effects. The connection may be in
                    # an unexpected state, so it's discarded.
                    with suppress(ExaError):
                        connection.close()
                    raise
                finally:
                    self._close_statement(statement)

    @staticmethod
    def _close_statement(statement: ExaStatement) -> None:
        """
        Closes the statement, releasing its result set on the server, if it is still
        open. Otherwise, `ExaStatement.__del__` would do it later, possibly after the
        connection has been returned to the pool and checked out by another request.
        This is only cleanup: a failure is ignored, so that it cannot discard a
        result that has already been fetched, stop the remaining queries in a list
        from running, or mask the original error.
        """
        with suppress(ExaError):
            statement.close()

    @staticmethod
    def _run_queries(
        connection: ExaConnection, queries: list[str], snapshot: bool
    ) -> ExaStatement:
        statement = None
        for q in queries:
            # Only the last statement is returned. Close the previous one now, so
            # that it is not left open if the next query fails.
            if statement is not None:
                DbConnection._close_statement(statement)
            statement = (
                connection.meta.execute_snapshot(query=q)
                if snapshot
                else connection.execute(query=q)
            )
        return statement
