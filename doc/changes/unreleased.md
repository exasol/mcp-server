# Unreleased

## Summary

This release adds the exasol telemetry tracking -- anonymized usage tracking of the MCP server.
This release upgrades FastMCP to 3.2.4 to fix CVE-2026-32871. It restores FastMCP v2-compatible environment variable configuration for Auth0, AuthKit, AWS Cognito, Azure, and Google OAuth providers, but not for all availabe providers.

## Features

* #165: Telemetry integration.
* #200: Restored FastMCP v2-compatible environment variable configuration for Auth0, AuthKit, AWS Cognito, Azure, and Google OAuth providers.

## Security

* #192: Upgrade FastMCP to 3.2.4 to fix CVE-2026-32871
