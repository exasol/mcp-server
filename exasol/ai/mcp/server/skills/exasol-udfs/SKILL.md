---
description: "Exasol User-Defined Functions (UDFs) and Scripts: CREATE SCRIPT syntax, language options, SQL-to-language data type mappings, ExaIterator API, BucketFS access, and Script Language Containers."
tags: ["exasol", "udf", "scripts", "bucketfs"]
---

# Exasol UDFs and Scripts

## Overview

Exasol lets you define custom functions and scripts in Python, Java, Lua, or R. These run inside the Exasol cluster nodes. Use them when:

- A computation is too complex to express in SQL.
- You need to call an external library, model or service.
- You want to process data in bulk without transferring it to the client.

The CREATE SCRIPT statement creates a UDF (also called "script").

## Script Types

| Type | Input | Output | Typical use |
|---|---|---|---|
| `SCALAR` | One row per call | One row (or multiple with `EMITS`) | Transform a single value |
| `SET` | All rows of a group | One row (or multiple with `EMITS`) | Aggregate or reshape a group |

**SET with EMITS** can return a different number of rows than it receives (e.g., a custom window function).
A **SET script with RETURNS** (not `EMITS`) acts as a custom aggregate — it receives all rows of a group and returns a single value.
A **SCALAR with EMITS** can be used for splitting one row into many.

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

## Variadic Scripts (Dynamic Parameters)

Use `...` as the parameter list to accept any number of input columns, output columns, or both.

### Dynamic Input

```sql
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT my_schema.to_json(...) RETURNS VARCHAR(2000000) AS
import json
def run(ctx):
    obj = {}
    for i in range(0, exa.meta.input_column_count, 2):
        obj[ctx[i]] = ctx[i + 1]   -- caller passes: name, value, name, value, ...
    return json.dumps(obj)
/

SELECT my_schema.to_json('fruit', fruit, 'price', price) FROM products;
```

- Access columns by index: `ctx[i]` — **0-based in Python/Java, 1-based in Lua/R**
- `exa.meta.input_column_count` — total number of input columns
- `exa.meta.input_columns[i].name` / `.sql_type` — per-column metadata

### Dynamic Output (`EMITS(...)`)

Declare `EMITS(...)` in `CREATE SCRIPT`. At call time, columns must be specified one of two ways:

| Method | Where specified | Use when |
|---|---|---|
| `EMITS` in `SELECT` | Caller's SQL query | Output structure depends on data values |
| `default_output_columns()` | Script body | Output structure derivable from input column count/types |

```sql
-- EMITS in SELECT (required when output depends on data content)
SELECT my_schema.split_csv(line) EMITS (a VARCHAR(100), b VARCHAR(100)) FROM t;
```

```python
# default_output_columns() — called before run(); no ctx/data access available
def default_output_columns():
    parts = []
    for i in range(exa.meta.input_column_count):
        parts.append("c" + exa.meta.input_columns[i].name + " " + exa.meta.input_columns[i].sql_type)
    return ", ".join(parts)
```

If neither is provided, the query fails with:
> *The script has dynamic return arguments. Either specify the return arguments in the query via EMITS or implement the method default_output_columns in the UDF.*

## ExaIterator API (`ctx`)

| Method / Attribute | Description |
|---|---|
| `ctx.<column_name>` | Access the current row's input value |
| `ctx.next()` | Advance to the next row in the group; returns `False` at end (SET only) |
| `ctx.emit(val1, val2, ...)` | Output a result row (SET/EMITS) |
| `ctx.size()` | Number of rows in the current group (SET only) |
| `ctx.reset()` | Reset iterator to the start of the group (SET only) |
| `ctx.get_dataframe(num_rows)` | Read up to `num_rows` rows as a pandas DataFrame (SET only) |
| `ctx.emit(dataframe)` | Emit all rows of a pandas DataFrame as output (SET/EMITS only) |

For SCALAR scripts, `ctx` holds exactly one row; no `next()` or `emit()` needed unless you use EMITS.

**Note:** There is no `emit_dataframe()` method — use `ctx.emit(dataframe)` to emit a DataFrame.

**R difference:** In R, use `ctx$next_row(n)` instead of `ctx.next()` to advance the iterator; it reads up to `n` rows at a time.

## `exa.meta` Object

The `exa.meta` object is available in all script languages and provides metadata about the current script invocation:

| Property | Description |
|---|---|
| `exa.meta.input_column_count` | Number of input columns passed to the script |
| `exa.meta.input_columns[i].name` | Name of input column `i` (lowercase) |
| `exa.meta.input_columns[i].sql_type` | SQL type of input column `i` (e.g. `VARCHAR(100)`, `DOUBLE`) |
| `exa.meta.output_column_count` | Number of output columns declared |
| `exa.meta.output_columns[i].name` | Name of output column `i` |
| `exa.meta.script_name` | Fully qualified name of the current script |
| `exa.meta.session_id` | Current session ID |

`exa.meta.input_columns` is especially useful in variadic scripts to inspect what columns were passed at call time.

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

## Sharing Code Between Scripts

Exasol does not provide a built-in `exa.import_script` mechanism in the default Python environment. The standard approach for sharing code is to bundle shared modules in a Script Language Container (SLC) — install them as regular Python packages accessible via normal `import` statements inside any script.

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

### Java with External JARs

Use `%jar` to load a JAR from BucketFS and `%scriptclass` to specify the entry-point class:

```sql
CREATE OR REPLACE JAVA SCALAR SCRIPT my_schema.custom_transform(input VARCHAR(2000))
RETURNS VARCHAR(2000) AS
  %scriptclass com.mycompany.MyTransformer;
  %jar /buckets/bfsdefault/default/jars/my-lib.jar;
/
```

- `%jar` can appear multiple times to load several JARs.
- `%scriptclass` is required when the class name differs from the script name.

## Data Types

SQL types are mapped to language types when passed into scripts:

| SQL Type | Python 3 | Java | Lua |
|---|---|---|---|
| `DECIMAL(p,0)` | `int` | `Integer` / `Long` / `BigDecimal` | `decimal` |
| `DECIMAL(p,s)` | `decimal.Decimal` | `BigDecimal` | `decimal` |
| `DOUBLE` | `float` | `Double` | `number` |
| `DATE` | `datetime.date` | `java.sql.Date` | `string` |
| `TIMESTAMP` | `datetime.datetime` | `java.sql.Timestamp` | `string` |
| `BOOLEAN` | `bool` | `Boolean` | `boolean` |
| `VARCHAR` / `CHAR` | `str` | `String` | `string` |

SQL `NULL` maps to `None` (Python), `null` (Java), or `NULL` (Lua).

**Caveats:**
- Python `datetime.datetime` supports only 6 fractional digits; `TIMESTAMP(7/8/9)` values are truncated.
- Use `DOUBLE` instead of `DECIMAL` where precision allows — better performance.
- For the return value, the Python/Java type must be compatible with the declared `RETURNS` type.

## Script Language Containers (SLCs)

The built-in environments have a fixed set of packages. If a script raises `module not found`, the package is not available and an SLC is required. An SLC bundles a complete language runtime with additional packages and is installed by a database administrator.

Once an SLC is uploaded to BucketFS, activate it for the session:

```sql
ALTER SESSION SET SCRIPT_LANGUAGES =
    'PYTHON3=localzmq+protobuf:///bfsdefault/myslc/release/current/exasol_python3?lang=python';
```

After activation, scripts using `PYTHON3` will use the custom container. The `PYTHON3_SME` language token refers to a pre-built SLC provided by Exasol.

## Performance Tips

- **Load once, use many**: Load models, configuration, or large resources at module level (outside `run()`), not per-row. The module is initialized once per worker process.
- **Batch with SET**: Collect rows into a list or DataFrame, process in bulk, then emit results. Avoids per-row Python/Java overhead.
- **Use `get_dataframe()`**: For pandas-based batch processing, `ctx.get_dataframe(n)` is faster than looping with `ctx.next()`.
- **Lua for low latency**: Lua starts in under 10 ms (no JVM/Python startup). Use it for row-level transforms where latency matters.
- **Parallelism is automatic**: UDFs run on all cluster nodes simultaneously — no manual partitioning needed.

## Debugging Tips

- **No stdout**: `print()` output is not visible. Use `ctx.emit()` or raise exceptions to surface information.
- **Exceptions**: unhandled exceptions abort the statement and show the traceback in the error message.
- **Check registration**: use `list_exasol_scripts` MCP tool or query `EXA_ALL_SCRIPTS` to verify a script exists.
- **View script source**: use `describe_exasol_script` MCP tool or query `EXA_ALL_SCRIPTS.SCRIPT_TEXT`.

## Common Errors

| Error | Likely cause |
|---|---|
| `syntax error` | Missing `/` terminator, wrong language keyword |
| `column not found` | Typo in `ctx.column_name`; names are case-insensitive in Exasol but Python attribute access is not |
| `module not found` | Package not in the built-in environment; needs an SLC |
| `data exception` | Return type mismatch; ensure the Python return value is compatible with `RETURNS` type |
