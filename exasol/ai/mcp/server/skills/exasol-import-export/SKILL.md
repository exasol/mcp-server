---
description: "Exasol IMPORT and EXPORT SQL statements: syntax, file formats (CSV, FBV, Parquet), cloud storage (S3, Azure, GCS), connection objects, error handling, and ETL staging patterns."
tags: ["exasol", "import", "export", "etl", "csv", "parquet", "cloud-storage"]
---

# Exasol IMPORT and EXPORT

## When to Use What

| Scenario | Tool |
|----------|------|
| Files on your local machine | **exapump CLI** (`upload` / `export`) |
| Remote or cloud files (S3, Azure, GCS, FTP, HTTP) | **SQL `IMPORT` / `EXPORT`** |
| Federated queries without copying data | **Virtual Schemas** |

Use `IMPORT`/`EXPORT` when the data source or destination is a remote location accessible from the Exasol cluster. Use exapump when files are on your local machine — it tunnels data through a local JDBC connection. `FROM LOCAL CSV FILE` and `INTO LOCAL CSV FILE` only work from EXAplus or JDBC clients; they do **not** work inside UDF scripts.

---

## Connection Objects

Connection objects store credentials for remote data sources. They are required for all cloud `IMPORT`/`EXPORT` operations.

```sql
-- Create or replace a connection
CREATE OR REPLACE CONNECTION my_conn
TO 'https://...'
USER 'username' IDENTIFIED BY 'password';

-- List connections (DBA only)
SELECT * FROM EXA_DBA_CONNECTIONS;

-- Drop a connection
DROP CONNECTION my_conn;
```

### Cloud Connection Strings

**Amazon S3:**
```sql
CREATE OR REPLACE CONNECTION s3_conn
TO 'https://my-bucket.s3.eu-west-1.amazonaws.com'
USER '' IDENTIFIED BY 'S3_ACCESS_KEY=AKIA...;S3_SECRET_KEY=secret...';
```

**Azure Blob Storage (SAS token):**
```sql
CREATE OR REPLACE CONNECTION azure_conn
TO 'https://myaccount.blob.core.windows.net/mycontainer'
USER '' IDENTIFIED BY 'AZURE_SAS_TOKEN=sv=2021-06-08&ss=b&srt=co...';
```

**Azure Blob Storage (Entra ID / client secret):**
```sql
CREATE OR REPLACE CONNECTION azure_conn
TO 'https://myaccount.blob.core.windows.net/mycontainer'
USER 'myaccount' IDENTIFIED BY 'ClientSecret'
CLIENT ID 'app-id' TENANT ID 'tenant-id';
```

**Google Cloud Storage:**
```sql
CREATE OR REPLACE CONNECTION gcs_conn
TO 'https://storage.googleapis.com/my-bucket'
USER '' IDENTIFIED BY 'GCS_ACCESS_KEY=GOOG...;GCS_SECRET_KEY=secret...';
```

**TLS:** Append `;VERIFY=FALSE` to the `IDENTIFIED BY` string to skip certificate verification (not recommended for production).

---

## IMPORT Statement

### Syntax

```sql
IMPORT INTO schema.table_name
FROM {CSV | FBV | PARQUET} AT connection_name
FILE 'path/to/file'
[csv_options]
[ERRORS INTO error_target [REJECT LIMIT n | UNLIMITED]];
```

For local files (EXAplus/JDBC only):

```sql
IMPORT INTO schema.table_name
FROM LOCAL CSV FILE '/local/path/data.csv'
[csv_options];
```

### CSV Options

| Option | Default | Description |
|--------|---------|-------------|
| `COLUMN SEPARATOR` | `,` | Field delimiter character |
| `COLUMN DELIMITER` | `"` | Text qualifier / quoting character |
| `ROW SEPARATOR` | `LF` | `LF`, `CR`, `CRLF`, or a custom string |
| `SKIP = n` | `0` | Skip first n rows (e.g., header lines) |
| `TRIM` / `LTRIM` / `RTRIM` | — | Strip whitespace from field values |
| `ENCODING` | `UTF-8` | Character encoding of the source file |
| `NULL = 'str'` | — | String value that represents `NULL` |

Lines starting with `#` in CSV/FBV files are ignored as comments.

### Supported Formats

| Format | Sources | Notes |
|--------|---------|-------|
| `CSV` | LOCAL, FTP/SFTP, HTTP/HTTPS, S3, Azure, GCS | Most flexible; supports compression |
| `FBV` (Fixed-width) | LOCAL, FTP/SFTP, HTTP/HTTPS, S3, Azure, GCS | Columns defined by `SIZE`, `ALIGN`, `PADDING` |
| `PARQUET` | S3 only | Type mappings apply; supports wildcards |

### Wildcards and Multiple Files

```sql
-- Wildcard: load all matching files in parallel
IMPORT INTO my_schema.my_table FROM CSV AT s3_conn
FILE 'data/2024/*.csv';

-- Multiple FILE clauses: load from several paths in parallel
IMPORT INTO my_schema.my_table FROM CSV AT s3_conn
FILE 'data/region_a/orders.csv'
FILE 'data/region_b/orders.csv'
FILE 'data/region_c/orders.csv';
```

### Compressed Files

Exasol automatically detects and decompresses `.zip`, `.gz`, and `.bz2` files during IMPORT — no special syntax needed:

```sql
IMPORT INTO my_table FROM CSV AT s3_conn FILE 'data/archive.csv.gz';
```

### Examples

**From S3 (CSV):**
```sql
IMPORT INTO my_schema.my_table
FROM CSV AT s3_conn
FILE 'path/to/data.csv'
COLUMN SEPARATOR = ','
SKIP = 1;
```

**From S3 (Parquet):**
```sql
IMPORT INTO my_schema.my_table
FROM PARQUET AT s3_conn
FILE 'data/*.parquet';
```

Parquet type mappings: `INT32`/`INT64` → `DECIMAL`, `FLOAT`/`DOUBLE` → `DOUBLE PRECISION`, `BYTE_ARRAY` → `VARCHAR`, `BOOLEAN` → `BOOLEAN`, `INT96` → `TIMESTAMP`.

Use `SOURCE COLUMN NAMES` to map Parquet columns to table columns by name rather than position.

**From Azure Blob Storage:**
```sql
IMPORT INTO my_schema.my_table
FROM CSV AT azure_conn
FILE 'container/path/data.csv'
SKIP = 1;
```

**From local file (EXAplus/JDBC only):**
```sql
IMPORT INTO my_schema.my_table
FROM LOCAL CSV FILE '/path/to/data.csv'
COLUMN SEPARATOR = ','
COLUMN DELIMITER = '"'
SKIP = 1
REJECT LIMIT 0;
```

---

## EXPORT Statement

### Syntax

```sql
EXPORT {schema.table_name | (SELECT ...)}
INTO {CSV | FBV} AT connection_name
FILE 'path/to/output'
[csv_options]
[WITH COLUMN NAMES];
```

### CSV Export Options

All IMPORT CSV options apply, plus:

| Option | Description |
|--------|-------------|
| `WITH COLUMN NAMES` | Write a header row with column names |
| `DELIMIT = ALWAYS \| NEVER \| AUTO` | When to quote fields (default: `AUTO`) |
| `BOOLEAN = 'true/false'` | Custom boolean string representation |
| `NULL = 'str'` | String to write for `NULL` values |
| `REPLACE` | Drop the target table/file before export |
| `TRUNCATE` | Delete existing rows before export |

**Note:** `ORDER BY` is only honored at the top level of the exported query. Subqueries with `ORDER BY` may not preserve order in the output.

### Examples

**To S3:**
```sql
EXPORT my_schema.my_table
INTO CSV AT s3_conn
FILE 'exports/my_table.csv'
WITH COLUMN NAMES;
```

**Query result to Azure:**
```sql
EXPORT (SELECT * FROM my_table WHERE status = 'active')
INTO CSV AT azure_conn
FILE 'exports/active.csv'
WITH COLUMN NAMES;
```

**To local file (EXAplus/JDBC only):**
```sql
EXPORT my_schema.my_table
INTO LOCAL CSV FILE '/path/to/output.csv'
COLUMN SEPARATOR = ','
COLUMN DELIMITER = '"'
WITH COLUMN NAMES;
```

**To S3 with AWS server-side encryption:**
```sql
-- AES256 (SSE-S3)
EXPORT my_table INTO CSV AT s3_conn
FILE 'data.csv' (SSE='AES256')
WITH COLUMN NAMES;

-- KMS encryption
EXPORT my_table INTO CSV AT s3_conn
FILE 'data.csv' (SSE='aws:kms' SSEKmsKeyId='arn:aws:kms:...')
WITH COLUMN NAMES;
```

---

## Error Handling

### REJECT LIMIT

Controls how many rows can be rejected before the statement fails:

```sql
-- Fail on first bad row
IMPORT INTO my_table FROM CSV AT conn FILE 'data.csv' REJECT LIMIT 0;

-- Allow up to 100 rejected rows
IMPORT INTO my_table FROM CSV AT conn FILE 'data.csv' REJECT LIMIT 100;

-- Never fail due to bad rows
IMPORT INTO my_table FROM CSV AT conn FILE 'data.csv' REJECT LIMIT UNLIMITED;
```

### ERRORS INTO

Capture rejected rows for inspection:

```sql
-- Into a table
IMPORT INTO my_table FROM CSV AT conn FILE 'data.csv'
REJECT LIMIT 100
ERRORS INTO error_schema.import_errors;

-- Into a CSV file
IMPORT INTO my_table FROM CSV AT conn FILE 'data.csv'
REJECT LIMIT 100
ERRORS INTO LOCAL CSV FILE '/path/to/errors.csv';
```

The error table contains: row number, error message, and raw data of the rejected row.

**Constraint violations** (NOT NULL, PRIMARY KEY) always cause the statement to fail immediately — they are not subject to `REJECT LIMIT`.

---

## Staging Workflow

A common ETL pattern: load into a staging table, merge into production, then clean up.

```sql
-- 1. Create staging table matching the target
CREATE TABLE staging.orders_stg (LIKE production.orders INCLUDING DEFAULTS);

-- 2. Import into staging with error capture
IMPORT INTO staging.orders_stg
FROM CSV AT s3_conn
FILE 'daily/orders_*.csv'
COLUMN SEPARATOR = ','
SKIP = 1
REJECT LIMIT 100
ERRORS INTO staging.orders_errors;

-- 3. Upsert into production
MERGE INTO production.orders t
USING staging.orders_stg s ON (t.order_id = s.order_id)
WHEN MATCHED THEN UPDATE SET
    t.status = s.status,
    t.amount = s.amount,
    t.updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_id, s.customer_id, s.status, s.amount, CURRENT_TIMESTAMP
);

-- 4. Clean up staging
TRUNCATE TABLE staging.orders_stg;
COMMIT;
```

---

## Required Privileges

| Operation | Required Privilege |
|-----------|--------------------|
| `IMPORT` | `IMPORT` system privilege |
| `EXPORT` | `EXPORT` system privilege |
| Read from source table | `SELECT` on the table |
| Write to target table | `INSERT` on the table |
| Use a connection | `USE ANY CONNECTION` or `ACCESS ON CONNECTION` |
