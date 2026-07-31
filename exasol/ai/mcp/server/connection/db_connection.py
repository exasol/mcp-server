import logging
from collections.abc import Callable
from typing import ContextManager

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

GENERIC_DB_ERROR_MESSAGE = (
    "A database error occurred. Please try again later or contact "
    "your administrator if the problem persists."
)

QUERY_ERROR_PREFIX = "The database rejected the query: "


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
        self, query: str | list[str], snapshot: bool = True, no_auth: bool = False
    ) -> ExaStatement:
        """
        Will make the set number of attempts to execute the provided query. A repeated
        attempt may follow a CommunicationError, ExaRuntimeError or ExaAuthError.

        Any `ExaError` that ultimately reaches this method (whether from establishing
        the connection or from an unretried/retry-exhausted query failure) is
        sanitized before being raised to the caller:

        - The full original exception is always logged server-side at WARNING level
          first (including any DSN, DB/OS username, OS name, driver/client version
          or session id it may carry), then a `RuntimeError` is raised `from` it, so
          the original exception remains available as `__cause__` for server-side
          debugging, even though it is never shown to the client.
        - For `ExaQueryError` (and its subclasses `ExaQueryTimeoutError`,
          `ExaQueryAbortError`) - raised for actual SQL EXECUTE failures such as
          syntax errors or references to non-existent objects - the `RuntimeError`
          message includes the original `.message` text, which is safe to expose.
        - For every other `ExaError` subtype (authentication, connection,
          communication or concurrency errors) a fixed, fully generic message is
          used instead, since these categories can embed connection/session details
          directly in `.message`.

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
        queries = [query] if isinstance(query, str) else query
        attempt = 1
        try:
            while True:
                with self._conn_factory(no_auth=no_auth) as connection:
                    connection.options["fetch_dict"] = True
                    try:
                        result = None
                        for q in queries:
                            result = (
                                connection.meta.execute_snapshot(query=q)
                                if snapshot
                                else connection.execute(query=q)
                            )
                        return result

                    except (ExaCommunicationError, ExaRuntimeError, ExaAuthError):
                        connection.close()
                        if attempt == self._num_retries:
                            raise
                        attempt += 1
        except ExaQueryError as ex:
            logger.warning(
                "Query execution failed with a database error", exc_info=True
            )
            raise RuntimeError(f"{QUERY_ERROR_PREFIX}{ex.message}") from ex
        except ExaError as ex:
            logger.warning(
                "A pyexasol error occurred while executing a query", exc_info=True
            )
            raise RuntimeError(GENERIC_DB_ERROR_MESSAGE) from ex
