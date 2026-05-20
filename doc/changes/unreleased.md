# Unreleased

## Features

* #217: Added `summarize_exasol_table` tool that returns per-column statistics
  (distinct value count, min/max for numeric columns) and a sample of rows.
  Enabled via `enable_summarize_table` configuration flag.
