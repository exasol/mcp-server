# Unreleased

## Features

* #272: Changed `execute_exasol_query`, `profile_exasol_query` and the `sample`
  field of `summarize_exasol_table` to return a columnar `{columns, rows}` shape by
  default, reducing token usage for wide or long results. Added `query_result_format`
  setting to opt back into the previous row-of-dicts shape.

## Security

* #256: Sanitized errors from `execute_exasol_query` and other DB-backed tools.
* #266: Added scheduled Trivy CVE scan for the Docker image.
