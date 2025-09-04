from collections.abc import Callable

from pyexasol import (
    ExaCommunicationError,
    ExaConnection,
    ExaRuntimeError,
    ExaStatement,
)


class DbConnection:
    """
    This is a pyexasol connection wrapper. It keeps the connection and requests a new
    one when the existing connection gets broken.

    Args:
        connection_factory:
            Supplied factory that creates a connection. The connection should be created
            with `fetch_dict`=True. The wrapper sets this option to True anyway. The
            dictionary option is required in order to present the result in a json form.
            This is what FastMCP expects from a tool.
    """

    def __init__(
        self, connection_factory: Callable[[], ExaConnection], num_retries: int = 2
    ) -> None:
        self._conn_factory = connection_factory
        self._num_retries = num_retries
        self._connection: ExaConnection | None = None

    def execute_query(self, query: str, snapshot: bool = True) -> ExaStatement:
        """
        Will make the set number of attempts to open or re-open the connection and
        execute the provided query. A repeated attempt may follow a CommunicationError
        or ExaRuntimeError. All other errors are considered unrecoverable and therefore
        will be propagated to the caller.

        If snapshot is True, which should be the mode of choice for querying metadata,
        the `meta.execute_snapshot` method will be called. Otherwise, it will use the
        normal `execute` method.
        """
        attempt = 1
        while True:
            if self._connection is None or self._connection.is_closed:
                self._connection = self._conn_factory()
                self._connection.options["fetch_dict"] = True
            try:
                if snapshot:
                    return self._connection.meta.execute_snapshot(query=query)
                return self._connection.execute(query=query)

            except (ExaCommunicationError, ExaRuntimeError):
                if not self._connection.is_closed:
                    self._connection.close()
                if attempt == self._num_retries:
                    raise
                attempt += 1
