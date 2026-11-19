# Unreleased

## Summary

Fixed a number of small issues in the server setup. The telemetry no longer delays
the server startup, and the server reports its own version during the handshake.

Database errors are easier to troubleshoot. The generic error message now names the
error type, and the new `verbose_db_errors` setting returns the full error text to
the client.

## Features

* #334: Made the database error output configurable and more informative:
  * The generic database error message now includes the error type
  * Added the `verbose_db_errors` setting, which returns the full database error text to the client

## Bug Fixes

* #330: Fixed a number of small issues in the server setup:
  * The telemetry no longer blocks the server startup
  * The server reports its own version instead of the FastMCP version
  * Accepted multi-line JSON in `EXA_MCP_SETTINGS`
  * The error for a missing settings file now shows the resolved file path
  * A lowercase or invalid `EXA_MCP_LOG_LEVEL` no longer crashes the server
  * Corrected the default HTTP host in the README
* #337: Fixed the `find_exasol_schemas` tool:
  * The tool no longer fails when fewer than two of the tables, views, functions and scripts are enabled
  * Schemas without visible objects of an enabled type can now be found by their name and comment
* #341: Fixed a database connection being returned to the pool before its query result was fetched
