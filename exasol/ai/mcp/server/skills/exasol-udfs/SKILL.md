---
description: "Exasol User-Defined Functions (UDFs) and Scripts: CREATE SCRIPT syntax, language options, ExaIterator API, BucketFS access, and Script Language Containers."
tags: ["exasol", "udf", "scripts", "bucketfs"]
---

# Exasol UDFs and Scripts

## Overview

Exasol lets you define custom functions and scripts in Python, Java, Lua, or R. These run inside the Exasol cluster nodes. Use them when:

- A computation is too complex to express in SQL.
- You need to call an external library or model.
- You want to process data in bulk without transferring it to the client.

The CREATE SCRIPT statement creates a UDF (also called "script").

## Script Types

| Type | Input | Output | Typical use |
|---|---|---|---|
| `SCALAR` | One row per call | One row per call | Transform a single value |
| `SET` | All rows of a group | One or more rows | Aggregate or reshape a group |
| `AGGREGATE` | All rows (no grouping needed) | One row | Custom aggregation |

**SET with EMITS** can return a different number of rows than it receives (e.g., splitting one row into many).

## CREATE SCRIPT Syntax

```sql
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT my_schema.my_script(
    input_col VARCHAR(100),
    factor DOUBLE
)
RETURNS DOUBLE AS

import math

def run(ctx):
    return math.sqrt(ctx.input_col) * ctx.factor

/
```

The script body ends with a bare `/` on its own line.

### Language Tokens

| Token | Language |
|---|---|
| `PYTHON3` | Python 3 (recommended) |
| `PYTHON3_SME` | Python 3 with Script Management Extensions |
| `JAVA` | Java |
| `LUA` | Lua |
| `R` | R |

### Return Declaration

- `RETURNS type` — scalar output (one value per call for SCALAR scripts).
- `EMITS (col1 type1, col2 type2, ...)` — tabular output (SET and SCALAR scripts that emit multiple rows).

## Python SCALAR Script

```sql
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT my_schema.upper_words(text VARCHAR(2000))
RETURNS VARCHAR(2000) AS

def run(ctx):
    return ctx.text.upper() if ctx.text else None

/
```

Call it like a regular SQL function:

```sql
SELECT my_schema.upper_words(description) FROM products;
```

## Python SET Script with EMITS

```sql
CREATE OR REPLACE PYTHON3 SET SCRIPT my_schema.tokenize(text VARCHAR(2000))
EMITS (token VARCHAR(200)) AS

def run(ctx):
    while True:
        if ctx.text:
            for word in ctx.text.split():
                ctx.emit(word)
        if not ctx.next():
            break

/
```

Call it in a `GROUP BY` context (group determines which rows form a set):

```sql
SELECT my_schema.tokenize(description) FROM products GROUP BY product_id;
```

## ExaIterator API (`ctx`)

| Method / Attribute | Description |
|---|---|
| `ctx.<column_name>` | Access the current row's input value |
| `ctx.next()` | Advance to the next row in the group; returns `False` at end (SET only) |
| `ctx.emit(val1, val2, ...)` | Output a result row (SET/EMITS) |
| `ctx.size()` | Number of rows in the current group (SET only) |
| `ctx.reset()` | Reset iterator to the start of the group (SET only) |

For SCALAR scripts, `ctx` holds exactly one row; no `next()` or `emit()` needed unless you use EMITS.

## Accessing BucketFS from Scripts

Files uploaded to BucketFS are available at a mounted path inside the script environment:

```
/buckets/<bucket_service>/<bucket_name>/<path>
```

Example — load a pickled model:

```python
import pickle

def run(ctx):
    with open('/buckets/bfsdefault/mymodels/model.pkl', 'rb') as f:
        model = pickle.load(f)
    return float(model.predict([[ctx.feature1, ctx.feature2]])[0])
```

The exact path depends on the BucketFS service name and bucket name configured in the database.

## Importing Other Scripts

```python
# In the script body, import another script in the same or different schema:
import exa
exa.import_script('my_schema.helper_script')
```

`exa.import_script` executes the referenced script's module-level code, making its definitions available. Use this instead of Python `import` for scripts stored in Exasol.

## Java Script Example

```sql
CREATE OR REPLACE JAVA SCALAR SCRIPT my_schema.java_upper(text VARCHAR(200))
RETURNS VARCHAR(200) AS

class JAVA_UPPER {
    static String run(ExaMetadata meta, ExaIterator ctx) throws Exception {
        String val = ctx.getString("text");
        return val != null ? val.toUpperCase() : null;
    }
}
/
```

In Java, the class name must match the script name (uppercase, underscores for hyphens).

## Script Language Containers (SLCs)

The built-in Python/Java/Lua/R environments have limited packages. To use additional packages (e.g., pandas, scikit-learn, torch), you need a **Script Language Container**.

An SLC is a Docker-based archive (`.tar.gz`) that bundles a complete language runtime with the packages you need. It is:

1. Built with [exaslct](https://github.com/exasol/script-languages-release).
2. Uploaded to BucketFS.
3. Activated in the session or database via `ALTER SESSION SET SCRIPT_LANGUAGES = '...'`.

```sql
-- Activate a custom SLC (example)
ALTER SESSION SET SCRIPT_LANGUAGES =
    'PYTHON3=localzmq+protobuf:///bfsdefault/myslc/release/current/exasol_python3?lang=python';
```

After activation, scripts using `PYTHON3` will use the custom container.

## Debugging Tips

- **No stdout**: `print()` output is not visible. Use `ctx.emit()` or raise exceptions to surface information.
- **Exceptions**: unhandled exceptions abort the statement and show the traceback in the error message.
- **Check registration**: use `list_exasol_scripts` MCP tool or query `EXA_ALL_SCRIPTS` to verify a script exists.
- **View script source**: use `describe_exasol_script` MCP tool or query `EXA_ALL_SCRIPTS.SCRIPT_TEXT`.
- **Test incrementally**: write a minimal script first, verify it works, then add complexity.

## Common Errors

| Error | Likely cause |
|---|---|
| `syntax error` | Missing `/` terminator, wrong language keyword |
| `column not found` | Typo in `ctx.column_name`; names are case-insensitive in Exasol but Python attribute access is not |
| `module not found` | Package not in the built-in environment; needs an SLC |
| `data exception` | Return type mismatch; ensure the Python return value is compatible with `RETURNS` type |
