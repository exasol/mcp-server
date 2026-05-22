# Unreleased

## Features

* #217: Added `summarize_exasol_table` tool that returns per-column statistics
  (distinct value count, min/max for numeric columns) and a sample of rows.
  Enabled via `enable_summarize_table` configuration flag.
* #219: Added `profile_exasol_query` tool that runs a query with profiling enabled
  and returns the execution plan breakdown from `EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY`.
  Enabled via `enable_query_profiling` configuration flag.
* #219: Added optional `row_limit` parameter to `execute_exasol_query` for previewing
  query results without fetching all rows.
