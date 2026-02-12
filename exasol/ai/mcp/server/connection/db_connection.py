from collections.abc import Callable
from typing import ContextManager

from pyexasol import (
    ExaAuthError,
    ExaCommunicationError,
    ExaConnection,
    ExaRuntimeError,
    ExaStatement,
)


class DbConnection:
    """
    This is a pyexasol connection wrapper. It requests a new connection for each query
    it executes. The returned connections can be cached but this class doesn't need to
    know about this.

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
        connection_factory: Callable[[], ContextManager[ExaConnection]],
        num_retries: int = 2,
    ) -> None:
        self._conn_factory = connection_factory
        self._num_retries = num_retries

    def execute_query(self, query: str, snapshot: bool = True) -> ExaStatement:
        """
        Will make the set number of attempts to execute the provided query. A repeated
        attempt may follow a CommunicationError, ExaRuntimeError or ExaAuthError. All
        other errors are considered unrecoverable and therefore will be propagated to
        the caller.

        If snapshot is True, which should be the mode of choice for querying metadata,
        the `meta.execute_snapshot` method will be called. Otherwise, it will use the
        normal `execute` method.
        """
        attempt = 1
        while True:
            with self._conn_factory() as connection:
                connection.options["fetch_dict"] = True
                try:
                    if snapshot:
                        return connection.meta.execute_snapshot(query=query)
                    return connection.execute(query=query)

                except (ExaCommunicationError, ExaRuntimeError, ExaAuthError):
                    connection.close()
                    if attempt == self._num_retries:
                        raise
                    attempt += 1
