---
name: exasol-virtual-schemas
description: "Exasol Virtual Schemas: creating and managing read-only federated access to external data sources via adapter scripts, JDBC connections, and pushdown query optimization."
tags: ["exasol", "virtual-schema", "federation", "jdbc", "adapter"]
---

# Exasol Virtual Schemas

Virtual schemas expose external data sources (other databases, files, cloud services) as read-only Exasol schemas. Queries against virtual tables are pushed down to the source where possible; the rest is processed in Exasol.

## How It Works

1. Exasol identifies virtual tables in a query and locates the adapter script.
2. It asks the adapter which operations it supports (pushdown capabilities).
3. A pushdown request is sent; the adapter returns an `IMPORT` statement or a `SELECT` with a UDF.
4. The result is merged into the rest of the query execution.

Metadata (table names, column types) is retrieved via JDBC and **cached** in Exasol until explicitly refreshed.

## Setup

### Step 1 — Upload the JDBC Driver to BucketFS

```
# Default driver path inside BucketFS
/buckets/bfsdefault/default/drivers/jdbc/<database>/<driver>.jar
```

Upload the JAR using the `download_file` BucketFS tool (downloads from a URL to BucketFS) or the BucketFS web interface. The `download_file` tool requires the JAR to be hosted at an accessible URL (e.g., a GitHub release or S3).

### Step 2 — Install the Adapter Script

```sql
CREATE SCHEMA adapter_schema;

CREATE OR REPLACE JAVA ADAPTER SCRIPT adapter_schema.jdbc_adapter AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/bfsdefault/default/virtual-schema-dist-12.0.0.jar;
  %jar /buckets/bfsdefault/default/postgresql-42.7.1.jar;
/
```

Replace the JAR names and paths with the actual versions in your BucketFS.

### Step 3 — Create a Connection Object

```sql
CREATE OR REPLACE CONNECTION pg_conn
TO 'jdbc:postgresql://host:5432/mydb'
USER 'user' IDENTIFIED BY 'password';
```

### Step 4 — Create the Virtual Schema

```sql
CREATE VIRTUAL SCHEMA pg_schema
USING adapter_schema.jdbc_adapter
WITH CONNECTION_NAME = 'PG_CONN'
     SCHEMA_NAME     = 'public';
```

`SCHEMA_NAME` is the remote schema to expose. Adapter-specific properties (e.g. `CATALOG_NAME`, `TABLE_FILTER`) go in the same `WITH` clause.

## SQL Reference

### CREATE VIRTUAL SCHEMA

```sql
CREATE VIRTUAL SCHEMA <vs_name>
USING <schema>.<adapter_script>
WITH <key> = '<value>'
     [<key> = '<value>' ...];
```

Common `WITH` properties:

| Property | Description |
|----------|-------------|
| `CONNECTION_NAME` | Name of the `CONNECTION` object (uppercase) |
| `SCHEMA_NAME` | Remote schema to import |
| `TABLE_FILTER` | Comma-separated list of tables to expose; omit to expose all |
| `IS_LOCAL` | Set to `'true'` for Exasol-to-Exasol connections on the same cluster — enables unrestricted `ORDER BY` |

### ALTER VIRTUAL SCHEMA

```sql
-- Refresh all metadata
ALTER VIRTUAL SCHEMA pg_schema REFRESH;

-- Refresh specific tables only
ALTER VIRTUAL SCHEMA pg_schema REFRESH TABLES orders customers;

-- Restrict visible tables
ALTER VIRTUAL SCHEMA pg_schema SET TABLE_FILTER = 'orders,customers';

-- Remove table filter (expose all tables again)
ALTER VIRTUAL SCHEMA pg_schema SET TABLE_FILTER = NULL;

-- Change an adapter property
ALTER VIRTUAL SCHEMA pg_schema SET SCHEMA_NAME = 'reporting';
```

### DROP VIRTUAL SCHEMA

```sql
DROP VIRTUAL SCHEMA pg_schema;
-- CASCADE drops dependent objects (views); RESTRICT (default) errors if any exist
DROP VIRTUAL SCHEMA pg_schema CASCADE;
```

### EXPLAIN VIRTUAL

Shows the query sent to the remote source by the adapter:

```sql
EXPLAIN VIRTUAL SELECT * FROM pg_schema.orders WHERE id > 100;
```

## Querying

```sql
-- Standard SELECT — pushed down where possible
SELECT * FROM pg_schema.orders WHERE status = 'open';

-- Join virtual and local tables
SELECT o.*, c.name
FROM pg_schema.orders o
JOIN local_schema.customers c ON o.customer_id = c.id;
```

Virtual tables are **read-only**. `INSERT`, `UPDATE`, `DELETE`, and `MERGE` are not supported.

## ORDER BY Limitation

`IMPORT`-based virtual schemas only support unordered data transfer. To sort results, wrap the query:

```sql
SELECT * FROM (
    SELECT * FROM pg_schema.big_table ORDER BY FALSE
) ORDER BY created_at DESC;
```

For Exasol-to-Exasol virtual schemas using local connections, set `IS_LOCAL = 'true'` to lift this restriction.

## Required Privileges

| Action | Required Privilege |
|--------|--------------------|
| `CREATE VIRTUAL SCHEMA` | `CREATE VIRTUAL SCHEMA` system privilege |
| `DROP VIRTUAL SCHEMA` | Ownership or `DROP ANY VIRTUAL SCHEMA` |
| `ALTER VIRTUAL SCHEMA` | Ownership or `ALTER ANY VIRTUAL SCHEMA` |
| Query a virtual table | `SELECT` on the virtual schema |
| Execute the adapter script | `EXECUTE` on the adapter script |
| Use the connection | `USE ANY CONNECTION` or `ACCESS ON CONNECTION <name>` |

Grant access to a virtual schema:

```sql
GRANT SELECT ON SCHEMA pg_schema TO analyst_role;
```

## Metadata System Tables

```sql
-- List all virtual schemas
SELECT * FROM EXA_ALL_VIRTUAL_SCHEMAS;

-- List tables in a virtual schema
SELECT * FROM EXA_ALL_TABLES WHERE TABLE_SCHEMA = 'PG_SCHEMA';

-- List columns of a virtual table
SELECT * FROM EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'PG_SCHEMA';
```

## Limits

| Resource | Limit |
|----------|-------|
| `CREATE VIRTUAL SCHEMA` statement | 64 MiB |
| Single `WITH` property value | 2 million characters |
| Adapter response | 2 GiB |
| Adapter notes field | 2 million characters |
| Recommended value list size | ≤ 100,000 entries |
