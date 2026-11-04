# 2.4.0 - 2026-11-04

## Summary

Upgraded to FastMCP 4. OAuth proxy deployments with `EXA_AUTH_ISSUER_URL` different
from `EXA_AUTH_BASE_URL` require all clients to re-authorize once. The write tools
support elicitation on the MCP `2026-07-28` protocol.

## Features

* #324: Supported elicitation on the MCP `2026-07-28` protocol

## Bug Fixes

* #309: A pooled connection is no longer discarded when the database rejects a
  query
* #303: Fixed BucketFS tools on Windows and the download of binary files
* #322: Declining an elicitation is now reported as declined rather than cancelled

## Refactorings

* #323: Upgraded to FastMCP 4 and switched the tool annotations to snake_case field
  names
* #322: Prepared the elicitation code for the FastMCP 4 migration and added an
  explicit `httpx` dependency

## Dependency Updates

### `main`

* Updated dependency `fastmcp:3.4.11` to `4.1.5`
* Added dependency `httpx:0.28.1`
