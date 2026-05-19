# 1.8.0 - 2026-05-19

## Summary

Improved OAuth2 configuration. Made docker image multi-arch.

## Features

* #209: Added building docker image for the ARM arhitecture.
* #212: Restored the FastMCP v2 OAuth2 environment variable based configuration for the remaining and future providers.
* #184: Added `EXA_MCP_OAUTH_STORAGE_BACKEND` environment variable to select the OAuth state storage backend (`filetree` default, `memory` for stateless deployments).

## Refactoring

* #210: Changed the org name in docker hub from "exadockerci4" to "exasol".
