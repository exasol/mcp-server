# Unreleased

## Summary

Fixed a number of small issues in the server setup. The telemetry no longer delays
the server startup, and the server reports its own version during the handshake.

## Bug Fixes

* #330: The telemetry no longer blocks the server startup
* #330: The server reports its own version instead of the FastMCP version
* #330: Accepted multi-line JSON in `EXA_MCP_SETTINGS`
* #330: The error for a missing settings file now shows the resolved file path
* #330: A lowercase or invalid `EXA_MCP_LOG_LEVEL` no longer crashes the server
* #330: Corrected the default HTTP host in the README
