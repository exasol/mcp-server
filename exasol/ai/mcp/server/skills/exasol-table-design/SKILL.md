---
name: exasol-table-design
description: "Exasol table design for performance: DISTRIBUTE BY, PARTITION BY, zone maps, data types, replication, surrogate keys, and CREATE TABLE syntax."
tags: ["exasol", "table-design", "performance", "distribution", "partitioning", "zonemaps"]
---

# Exasol Table Design

## CREATE TABLE Syntax

```sql
CREATE [OR REPLACE] TABLE [IF NOT EXISTS] schema.table_name (
    column_name data_type [DEFAULT value] [IDENTITY [start [INCREMENT BY step]]]
                          [NOT NULL] [PRIMARY KEY] [REFERENCES ref_table (ref_col)],
    ...,
    [PRIMARY KEY (col1, col2, ...)],
    [FOREIGN KEY (col) REFERENCES ref_table (ref_col)],
    [LIKE other_table [INCLUDING DEFAULTS] [INCLUDING IDENTITY] [INCLUDING COMMENTS]],
    [DISTRIBUTE BY col1 [, col2, ...]],
    [PARTITION BY col]
);
```

Key options:

| Clause | Effect |
|--------|--------|
| `OR REPLACE` | Silently overwrites an existing table |
| `IF NOT EXISTS` | No-op if the table already exists |
| `LIKE t` | Copies column definitions (not constraints) from `t`; add `INCLUDING DEFAULTS` / `INCLUDING IDENTITY` / `INCLUDING COMMENTS` to copy those properties |
| `DISTRIBUTE BY` | Controls how rows are spread across cluster nodes |
| `PARTITION BY` | Controls physical data layout within each node |

### CREATE TABLE AS

```sql
-- Create and populate from a query
CREATE TABLE new_table AS SELECT * FROM source_table;

-- Create schema only (no data)
CREATE TABLE new_table AS SELECT * FROM source_table WITH NO DATA;
```

---

## Data Types

Choose the smallest type that fits the data — oversized declarations waste memory and slow comparisons.

| Type | Recommendation |
|------|----------------|
| `DECIMAL(p, s)` | Preferred exact numeric; use instead of `DOUBLE` when precision matters |
| `DOUBLE` / `FLOAT` | 64-bit floating point; only when approximate arithmetic is acceptable |
| `VARCHAR(n)` | Variable-length string; avoid declaring much larger than needed (e.g. `VARCHAR(2000000)` when 100 suffices) |
| `CHAR(n)` | Fixed-length; prefer over `VARCHAR` when all values have the same length (e.g. ISO country codes) |
| `DATE` | Calendar date only; use instead of `TIMESTAMP` when time-of-day is irrelevant |
| `TIMESTAMP(p)` | Include time; default precision is 3 (milliseconds) |
| `BOOLEAN` | For true/false flags |
| `HASHTYPE(n BYTE)` | Fixed-length binary hashes (UUIDs, MD5 digests, etc.) |

**Rules:**
- Always use **identical types** on both sides of a JOIN — mismatches force implicit type conversion and prevent local joins.
- Use `DECIMAL` join and group-by keys; joins on `VARCHAR` and `DATE`/`TIMESTAMP` are significantly more expensive.

---

## DISTRIBUTE BY

Controls how rows are spread across cluster nodes. Correct distribution turns **global joins** (network shuffle) into **local joins** (node-local, no network cost).

### Syntax

```sql
-- At creation
CREATE TABLE orders (
    order_id    INT,
    customer_id INT,
    order_date  DATE,
    amount      DECIMAL(10, 2),
    DISTRIBUTE BY customer_id
);

-- Alter existing table
ALTER TABLE orders DISTRIBUTE BY customer_id;

-- Remove distribution key (round-robin)
ALTER TABLE orders DROP DISTRIBUTION KEYS;
```

### When a Join Is Local

A join between `T1` and `T2` is local when both tables are distributed by columns that are a **subset** of the join condition:

```sql
-- T1 DISTRIBUTE BY customer_id
-- T2 DISTRIBUTE BY customer_id
-- → local join: no network transfer
SELECT * FROM orders T1 JOIN customers T2 ON T1.customer_id = T2.customer_id;

-- T1 DISTRIBUTE BY (customer_id, region_id)
-- T2 DISTRIBUTE BY (customer_id, region_id)
-- → local join: both columns present in join condition
SELECT * FROM T1 JOIN T2 ON T1.customer_id = T2.customer_id AND T1.region_id = T2.region_id;

-- T1 DISTRIBUTE BY (customer_id, region_id)
-- Joining only on customer_id → NOT local (multi-column key, only partial match)
```

**Envelope matching rule:** distributing by `(x, y)` enables local joins when the join condition contains **all** of `(x, y)`. It does NOT enable local joins on just `x` or just `y` alone.

### Choosing the Right Column

1. Pick the column used in the **most frequent and most expensive JOINs**.
2. **Single column is almost always optimal** — it benefits more queries (joins and GROUP BY) than a multi-column key.
3. Use multiple columns only when all are always present together in join conditions.
4. The column must have **high cardinality** (many distinct values) — low-cardinality columns cause severe data skew.
5. **Do not distribute on WHERE-only columns** — it concentrates all filtered data on one node and disables MPP on filter-only queries.

### Checking and Previewing Distribution

```sql
-- Check current distribution (row count per node)
SELECT iproc() AS node, COUNT(*) FROM my_table GROUP BY 1 ORDER BY 1;

-- Preview distribution with a candidate column (before changing)
SELECT value2proc(customer_id) AS future_node,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 2) AS pct
FROM orders
GROUP BY 1
ORDER BY 1;

-- Inspect the current distribution key
SELECT COLUMN_NAME, COLUMN_IS_DISTRIBUTION_KEY
FROM EXA_ALL_COLUMNS
WHERE COLUMN_TABLE = 'ORDERS' AND COLUMN_SCHEMA = 'MY_SCHEMA'
ORDER BY COLUMN_ORDINAL_POSITION;
```

### Anti-Patterns

| Anti-pattern | Why it hurts |
|---|---|
| Distributing by a `status` or `country` column | Low cardinality → severe data skew |
| Distributing by a column only used in `WHERE` | Disables MPP for the common filter case |
| Multi-column distribution when joins use only a subset | The key is never matched; no local joins |
| Different data types on distribution columns of joined tables | Prevents local join optimization |

---

## PARTITION BY

Controls physical data layout **within each node**. Enables range pruning — the engine skips partitions that don't match the `WHERE` predicate, reducing I/O and memory.

### Syntax

```sql
-- Combined with DISTRIBUTE BY (typical)
CREATE TABLE orders (
    order_id    INT,
    customer_id INT,
    order_date  DATE,
    amount      DECIMAL(10, 2),
    DISTRIBUTE BY customer_id,
    PARTITION BY order_date
);

-- Alter existing table
ALTER TABLE orders PARTITION BY order_date;

-- Remove partitioning
ALTER TABLE orders DROP PARTITION KEYS;
```

### Supported Column Types

`DECIMAL`, `DATE`, `TIMESTAMP`, `DOUBLE`, `BOOLEAN`, `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `HASHTYPE`

`VARCHAR` and `CHAR` are **not** supported as partition columns.

### When to Partition

- Partition by columns used in **range filter** (`WHERE` conditions with `BETWEEN`, `<`, `>`, date ranges).
- Date columns are the most common and effective choice for time-series data.
- Exasol automatically determines the number of partitions — no manual sizing needed.
- Do not partition small tables; the benefit only appears at significant data volumes.

### Combined DISTRIBUTE BY + PARTITION BY Pattern

The canonical pattern for large fact tables:

```sql
CREATE TABLE fact_sales (
    sale_id     INT,
    customer_id INT,      -- join column → distribute
    sale_date   DATE,     -- range filter column → partition
    amount      DECIMAL(15, 2),
    DISTRIBUTE BY customer_id,
    PARTITION BY sale_date
);
```

This enables:
- Local joins on `customer_id` (distribution)
- Partition pruning on `sale_date` range filters

---

## Zone Maps

Zone maps are a metadata layer on data segments that store the **minimum and maximum values** for a column within each segment. When a query has a predicate on a zone-mapped column, Exasol checks the zone records first and skips any segment whose min/max range cannot satisfy the predicate — reducing I/O without scanning data.

### Enablement

Zone maps are **automatically enabled on partition columns**. For non-partitioned columns, use:

```sql
-- Enable zone map on a column
ENFORCE ZONEMAP ON my_table (column_name);

-- Remove zone map from a column
DROP ZONEMAP ON my_table (column_name);
```

### When Zone Maps Help

Zone maps are most effective when the column has **data locality** — values that are naturally clustered or sorted within segments:

- Date/timestamp columns (inserts tend to be chronological)
- Monotonically increasing surrogate keys (`IDENTITY` columns)
- Columns already used as `PARTITION BY` keys
- Any column frequently used in range or equality filters

### Supported Predicates

Zone maps are applied for: `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `IN`, `IS NULL`, `IS NOT NULL`, and `AND` combinations.

### Data Type Support

| Support level | Types |
|---|---|
| Full | `BOOLEAN`, `DATE`, `DECIMAL`, `DOUBLE`, `INTERVAL YEAR TO MONTH`, `TIMESTAMP` |
| Limited (equality only) | `INTERVAL DAY TO SECOND` |
| Not supported | `CHAR`, `VARCHAR`, `GEOMETRY`, `HASHTYPE` |

### Inspection

```sql
-- Check which columns have zone maps
SELECT COLUMN_NAME, COLUMN_IS_ZONEMAPPED
FROM EXA_ALL_COLUMNS
WHERE COLUMN_TABLE = 'MY_TABLE' AND COLUMN_SCHEMA = 'MY_SCHEMA';

-- DESCRIBE also shows zone-mapped columns
DESCRIBE my_table;
```

Profiling output shows `WITH ZONEMAP` next to a scan step when zone records pruned segments during execution.

### Anti-Patterns

| Anti-pattern | Why it hurts |
|---|---|
| Zone map on a `VARCHAR` or `CHAR` column | Not supported — zone map has no effect |
| Zone map on a randomly inserted column (no locality) | Min/max spans the full range in every segment — no segments are skipped |
| Zone map instead of `PARTITION BY` for large tables | Partitioning prunes whole partitions; zone maps only skip segments within a partition — use both together on large tables |

---

## Replication

Small tables (dimension tables in star schemas) can be automatically replicated to every node, turning global joins into local joins without explicit distribution.

- **Default threshold**: 100,000 rows.
- Increase for larger dimension tables that are still small relative to fact tables:

```sql
ALTER SYSTEM SET REPLICATION_BORDER = 1000000;
```

The query optimizer replicates tables below this threshold into local DB RAM on each node, making any join against them local automatically. This complements (not replaces) proper distribution key design on large fact tables.

---

## Surrogate Keys

Use surrogate keys (synthetic `DECIMAL` IDs) in place of:
- **Multi-column natural keys** — multi-column join indexes are large and expensive.
- **Non-numeric natural keys** — joins on `VARCHAR` or `DATE`/`TIMESTAMP` keys are significantly slower than joins on `DECIMAL`.

```sql
-- Preferred: surrogate key as DECIMAL
CREATE TABLE customers (
    customer_id DECIMAL(18, 0) IDENTITY PRIMARY KEY,
    email       VARCHAR(255) NOT NULL,
    ...
);

-- Avoid: joining on VARCHAR natural key
SELECT * FROM orders o JOIN customers c ON o.email = c.email;

-- Prefer: joining on DECIMAL surrogate key
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
```

---

## Other Performance Rules

- **Avoid `ORDER BY` in views** — views are inlined into queries; an `ORDER BY` inside forces a sort that the outer query may discard.
- **Prefer `UNION ALL` over `UNION`** — `UNION` performs duplicate elimination (an expensive sort/hash); use `UNION ALL` unless deduplication is required.
- **Use explicit `ON` conditions instead of `USING`** in multi-join queries — `USING` in complex multi-join queries can cause exponential heap memory consumption.
- **Do not use `CROSS JOIN` unintentionally** — a missing `ON` condition produces a cartesian product.

---

## Common Patterns

### Staging Table for ETL

```sql
-- Inherit schema from production table; no data, no constraints
CREATE TABLE staging.orders_stg (LIKE production.orders INCLUDING DEFAULTS);

-- After loading into staging, merge into production
MERGE INTO production.orders t
USING staging.orders_stg s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET t.status = s.status, t.amount = s.amount
WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.status, s.amount, s.order_date);

TRUNCATE TABLE staging.orders_stg;
COMMIT;
```

### Large Fact Table with Distribution and Partitioning

```sql
CREATE TABLE fact_events (
    event_id    DECIMAL(18, 0) IDENTITY PRIMARY KEY,
    user_id     DECIMAL(18, 0) NOT NULL,   -- distribute: frequent join column
    event_ts    TIMESTAMP      NOT NULL,   -- partition: range filter column
    event_type  VARCHAR(64)    NOT NULL,
    payload     VARCHAR(4000),
    DISTRIBUTE BY user_id,
    PARTITION BY event_ts
);
```
