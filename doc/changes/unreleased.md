# Unreleased

## Features

* #277: Added the `enable_dialect_tools` setting to allow hiding the tools that
  provide information about the Exasol SQL dialect.
* #274: Renamed the `describe_exasol_*` tools (plural forms) and changed them to
  accept a list of names, describing multiple tables, views or functions per call.
* #272: Changed `execute_exasol_query`, `profile_exasol_query` and the `sample`
  field of `summarize_exasol_table` to return a tabular `{columns, rows}` shape by
  default.

## Bug Fixes

* #253: Changed the telemetry "started" event to use the project short tag
  (e.g. `EMCP`) read from `error_code_config.yml`.
* #272: Stopped retrying `ExaRuntimeError` in `DbConnection`. pyexasol only raises
  it for client-side precondition failures (e.g. duplicate column names in a result
  set), which reproduce identically on any connection, so retrying only discarded
  connections and added latency without ever succeeding.
* #281: Fixed `find_exasol_*` tools returning all objects instead of none when no
  keyword matched anything.

## Security

* #256: Sanitized errors from `execute_exasol_query` and other DB-backed tools.
* #266: Added scheduled Trivy CVE scan for the Docker image.
