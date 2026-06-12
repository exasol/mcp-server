# 1.10.0 - 2026-06-04

## Summary

Added more control over logging. Added CLI to install skills.

## Features

* #228: Added `EXA_MCP_LOG_IGNORE` environment variable to suppress log messages from specific dependency components (e.g. FastMCP, uvicorn) that would otherwise flood the log file.
* #232: Added `exasol-install-skills` CLI command that copies bundled Exasol skills to a local
  directory for use with MCP clients that do not support the FastMCP skills protocol (e.g. Open
  Code). Pass `--server-url` to download the latest skills from a remote Exasol MCP server
  instead. Added `name` field to all skill frontmatter for compatibility with Open Code's native
  skills format. Added client integration guide with Open Code plugin examples, including a
  Python-free option that fetches skills directly from GitHub.
