import logging
from collections.abc import Callable
from typing import (
    Any,
    ContextManager,
    TypeVar,
)

from pyexasol import (
    ExaAuthError,
    ExaCommunicationError,
    ExaConnection,
    ExaConnectionError,
    ExaError,
    ExaQueryError,
    ExaRuntimeError,
    ExaStatement,
)

logger = logging.getLogger(__name__)

GENERIC_DB_ERROR_MESSAGE = (
    "A database error occurred. Please try again later or contact "
    "your administrator if the problem persists."
)

# The two failure categories a client can actually act on get a message that
# names the category - and nothing else. Neither carries a host, a port, a user
# name or a session id, so the sanitization contract of `execute_query` holds:
# the original exception stays server-side, in the log and as `__cause__`.
#
# Without this, a database that is simply not running was reported with the
# generic message above, and the agent driving the tool had no better move than
# "try again later" against a server that was never going to answer.
CONNECTION_ERROR_MESSAGE = (
    "Could not connect to the database: it is not running or not reachable. "
    "Start it or check that it is reachable, then try again."
)

AUTH_ERROR_MESSAGE = (
    "The database rejected the login of the configured MCP user. "
    "Check the credentials the MCP server was started with."
)

QUERY_ERROR_PREFIX = "The database rejected the query: "

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
    that internal connection/session details are never exposed to an MCP client.

    Args:
        connection_factory:
            Supplied factory that creates a connection. The connection should be created
            with `fetch_dict`=True. The wrapper sets this option to True anyway. The
            dictionary option is required in order to present the result in a json form.
            This is what FastMCP expects from a tool.
        num_retries:
            Number of attempts to execute a query before raising an exception.
    """

    def __init__(
        self,
        connection_factory: Callable[..., ContextManager[ExaConnection]],
        num_retries: int = 2,
    ) -> None:
        self._conn_factory = connection_factory
        self._num_retries = num_retries

    def execute_query(
        self,
        query: str | list[str],
        snapshot: bool = True,
        no_auth: bool = False,
        fetch: Callable[[ExaStatement], T] = _identity,
    ) -> T:
        """
        Will make the set number of attempts to execute the provided query. A repeated
        attempt may follow an `ExaCommunicationError` or `ExaAuthError`, since these can
        be transient (a network blip, a briefly stale token) and may succeed on a fresh
        connection. `ExaRuntimeError` is never retried: pyexasol only raises it for
        client-side precondition failures (e.g. duplicate column names in the result
        set, an already-closed connection) that are a function of the query/arguments
        themselves, not of connection state - retrying would reproduce the identical
        error while needlessly discarding a connection.

        `fetch` is called on the resulting statement while still inside this method's
        sanitizing try/except, so callers should retrieve rows through it (e.g.
        `fetch=fetchall`, using the module-level helper below) rather than calling
        `.fetchall()`/`.fetchval()`/etc. on the returned value afterwards. An `ExaError`
        raised while fetching - for example a connection failure while pulling a later
        chunk of a large result set - is sanitized exactly like one raised during query
        execution; fetching outside this method would let it through unsanitized. The
        default `fetch` returns the statement unchanged, for callers that only execute
        a statement for its side effect (e.g. DDL/DML) and never fetch from it.

        Any `ExaError` that ultimately reaches this method (whether from establishing
        the connection, from an unretried/retry-exhausted query failure, or from the
        `fetch` call) is sanitized before being raised to the caller:

        - The full original exception is always logged server-side at WARNING level
          first (including any DSN, DB/OS username, OS name, driver/client version
          or session id it may carry), then a `RuntimeError` is raised `from` it, so
          the original exception remains available as `__cause__` for server-side
          debugging, even though it is never shown to the client.
        - For `ExaQueryError` (and its subclasses `ExaQueryTimeoutError`,
          `ExaQueryAbortError`) - raised for actual SQL EXECUTE failures such as
          syntax errors or references to non-existent objects - the `RuntimeError`
          message includes the original `.message` text, which is safe to expose.
        - For `ExaConnectionError`/`ExaCommunicationError` (the database cannot be
          reached) and `ExaAuthError` (the login was rejected) a fixed message that
          names only the CATEGORY is used, so a client can tell "start the
          database" from "fix the credentials" without seeing the connection/session
          details these exceptions embed in `.message`.
        - For every other `ExaError` subtype (concurrency, client-side runtime
          errors) a fixed, fully generic message is used instead.

        Non-pyexasol exceptions (i.e. bugs in our own code, such as a `TypeError`)
        are not caught here and propagate unmodified.

        If snapshot is True, which should be the mode of choice for querying metadata,
        the `meta.execute_snapshot` method will be called. Otherwise, it will use the
        normal `execute` method.

        If a list of queries is provided, all statements are executed in a single
        session and the result of the last one is returned.

        If no_auth is True, the connection factory will skip the OAuth username claim
        check. This is intended for unauthenticated internal callers such as the health
        check endpoint.
        """
        try:
            statement = self._execute_with_retries(
                query, snapshot=snapshot, no_auth=no_auth
            )
            return fetch(statement)
        except ExaQueryError as ex:
            logger.warning(
                "Query execution failed with a database error", exc_info=True
            )
            raise RuntimeError(f"{QUERY_ERROR_PREFIX}{ex.message}") from ex
        except (ExaConnectionError, ExaCommunicationError) as ex:
            logger.warning(
                "Could not connect to or communicate with the database", exc_info=True
            )
            raise RuntimeError(CONNECTION_ERROR_MESSAGE) from ex
        except ExaAuthError as ex:
            logger.warning("The database rejected the login", exc_info=True)
            raise RuntimeError(AUTH_ERROR_MESSAGE) from ex
        except ExaError as ex:
            logger.warning(
                "A pyexasol error occurred while executing a query", exc_info=True
            )
            raise RuntimeError(GENERIC_DB_ERROR_MESSAGE) from ex

    def _execute_with_retries(
        self, query: str | list[str], snapshot: bool, no_auth: bool
    ) -> ExaStatement:
        queries = [query] if isinstance(query, str) else query
        attempt = 1
        while True:
            with self._conn_factory(no_auth=no_auth) as connection:
                connection.options["fetch_dict"] = True
                try:
                    return self._run_queries(connection, queries, snapshot=snapshot)
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

    @staticmethod
    def _run_queries(
        connection: ExaConnection, queries: list[str], snapshot: bool
    ) -> ExaStatement:
        result = None
        for q in queries:
            result = (
                connection.meta.execute_snapshot(query=q)
                if snapshot
                else connection.execute(query=q)
            )
        return result
