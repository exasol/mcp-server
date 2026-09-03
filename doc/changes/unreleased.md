# Unreleased

## Summary

## Bug Fixes

* #293: Error messages a client can act on. A database that cannot be reached was reported with the fully generic "A database error occurred. Please try again later ..." text, so the agent driving the tool had no better move than to retry against a server that was never going to answer; it is now reported as "Could not connect to the database: it is not running or not reachable", and a rejected login as such. Neither message carries a host, port, user or session id, so the sanitization contract of `DbConnection.execute_query` is unchanged; every other `ExaError` keeps the generic text. A refused query says why it was refused (`query_rejection_reason`): a non-SELECT names the statement kind, a `SELECT INTO` is named, and a query the Exasol parser cannot read carries the parser's position and, for `TOP` / `FETCH FIRST`, the hint that Exasol pages with `LIMIT`. Before, `SELECT TOP 3 * FROM T` was answered with "The query is invalid or not a SELECT statement." `verify_query` keeps its boolean contract.
