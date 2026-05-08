# 1.6.2 - 2026-04-10

## Summary

This release adds the exasol telemetry tracking -- anonymized usage tracking of the MCP server.
This release upgrades FastMCP to 3.2.4 to fix CVE-2026-32871. It restores FastMCP v2-compatible environment variable configuration for Auth0, AuthKit, AWS Cognito, Azure, and Google OAuth providers, but not for all availabe providers.

## Features

* #165: Telemetry integration.
* #200: Restored FastMCP v2-compatible environment variable configuration for Auth0, AuthKit, AWS Cognito, Azure, and Google OAuth providers.

## Security

* #192: Upgrade FastMCP to 3.2.4 to fix CVE-2026-32871
* #203: Fixed the following CVEs:
    - CVE-2026-42215 (GitPython)
    - CVE-2026-42284 (GitPython)
    - CVE-2025-71176 (pytest)
    - CVE-2026-41425 (Authlib)
    - CVE-2026-40347 (python-multipart)
    - CVE-2026-25645 (requests)

## Bug fixing

* #185: Made the health check uneffected by authentication.
