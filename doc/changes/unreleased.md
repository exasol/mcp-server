# Unreleased

## Summary

Fixed a number of small issues in the server setup. The telemetry no longer delays
the server startup, and the server reports its own version during the handshake.

## Bug Fixes

* #330: Fixed a number of small issues in the server setup:
  * The telemetry no longer blocks the server startup
  * The server reports its own version instead of the FastMCP version
  * Accepted multi-line JSON in `EXA_MCP_SETTINGS`
  * The error for a missing settings file now shows the resolved file path
  * A lowercase or invalid `EXA_MCP_LOG_LEVEL` no longer crashes the server
  * Corrected the default HTTP host in the README
