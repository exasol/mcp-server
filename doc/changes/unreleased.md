# Unreleased

## Bug Fixes

* #223: Fixed `profile_exasol_query` to be side-effect free: the tool now checks whether
  session profiling is already enabled via `SYS.EXA_PARAMETERS` and skips the
  `ALTER SESSION SET PROFILE` statements if it was already `ON`.

## Features

* #217: Added `summarize_exasol_table` tool that returns per-column statistics
  (distinct value count, min/max for numeric columns) and a sample of rows.
  Enabled via `enable_summarize_table` configuration flag.
* #219: Added `profile_exasol_query` tool that runs a query with profiling enabled
  and returns the execution plan breakdown from `EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY`.
  Enabled via `enable_query_profiling` configuration flag.
* #219: Added optional `row_limit` parameter to `execute_exasol_query` for previewing
  query results without fetching all rows.
* #221: Added `list_exasol_preprocessors` tool that lists available SQL preprocessor
  scripts and reports the currently active one. Added `set_exasol_preprocessor` tool
  that activates a preprocessor at the session level. Both tools are enabled by default
  and can be disabled via the `enable_preprocessor_tools` configuration flag.
