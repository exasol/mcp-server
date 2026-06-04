---
name: exasol-mcp-server
description: "Exasol MCP Server usage guide: common workflows for database exploration, query debugging, BucketFS management, and working with UDFs and preprocessors."
tags: ["exasol", "mcp", "workflows"]
---

# Exasol MCP Server

## Optional Tools

Several tools — including `execute_exasol_query`, `profile_exasol_query`, `execute_exasol_write_query`, `summarize_exasol_table`, and all BucketFS tools — are disabled by default and may not appear in the tool list. If a workflow below requires a tool that is not available, do not retry or attempt workarounds: inform the user that the tool is disabled and needs to be enabled by the server administrator.

## Common Workflows

### Schema Exploration

Start broad, then narrow down:

```
1. list_exasol_schemas
   → pick a schema

2. list_exasol_tables_and_views(schema_name=...)
   → pick a table

3. describe_exasol_table_or_view(schema_name=..., table_name=...)
   → see columns, types, constraints

4. summarize_exasol_table(schema_name=..., table_name=...)
   → understand data distribution (if enabled)
```

If you already know roughly what you are looking for, use the `find_*` variants to keyword-search across all schemas at once.

### Writing and Debugging a Query

```
1. execute_exasol_query(query="SELECT ...")
   → verify the query works and returns expected data

2. profile_exasol_query(query="SELECT ...")
   → check the execution plan if the query is slow
   → look at the EXECUTION_MODE column: COMPILE, EXECUTE, WAIT
   → high COMPILE time → query rewrite or simplification may help
   → high EXECUTE time → check JOIN strategies and data distribution
```

### Discovering and Using UDFs

```
1. list_exasol_user_defined_functions(schema_name=...)
   → see all UDF scripts in a schema

2. describe_exasol_user_defined_function(schema_name=..., script_name=...)
   → read source code and parameter declarations

3. execute_exasol_query(query="SELECT schema.script_name(col) FROM table")
   → invoke the script on data
```

### BucketFS Workflow

```
1. list_bucketfs_directories(path="/")
   → see top-level buckets

2. list_bucketfs_files(path="/my_bucket/models/")
   → list model files

3. read_bucketfs_text_file(path="/my_bucket/config.json")
   → read a configuration file

4. write_text_to_bucketfs_file(path="/my_bucket/output.txt", content="...")
   → save results or configuration
```

### Checking Built-in Function Behavior

```
1. list_exasol_built_in_function_categories()
   → see function groups (string, numeric, date, ...)

2. list_exasol_built_in_functions(category="string")
   → list functions in a category

3. describe_exasol_built_in_function(name="REGEXP_REPLACE")
   → get the full signature and description
```

---

## Preprocessors

A preprocessor is a special UDF script that runs before each query, transforming the SQL before Exasol executes it. Use cases include macro expansion, automatic schema qualification, and audit logging.

```
1. list_exasol_preprocessors()
   → see available preprocessor scripts

2. set_exasol_preprocessor(schema_name=..., script_name=...)
   → activate for the current session

3. Verify with list_exasol_preprocessors() before running dependent queries
   (the setting may reset if the server reconnects)
```
