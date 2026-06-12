---
name: exasol-sql-dialect
description: "Exasol SQL dialect specifics: syntax, data types, functions, and common pitfalls for generating correct Exasol SQL."
tags: ["exasol", "sql", "dialect"]
---

# Exasol SQL Dialect

## Identifiers

- Identifiers (schema, table, column names) are **case-insensitive** by default and stored uppercase.
- To preserve case or use reserved words as identifiers, wrap them in **double quotes**: `"myColumn"`.
- Embed a literal double-quote inside a quoted identifier by doubling it: `"MY""COLUMN"`.

## String Literals

- Use **single quotes**: `'value'`.
- Embed a literal single-quote by doubling it: `'it''s a value'`.

## Data Types

| Type | Notes |
|---|---|
| `VARCHAR(n)` | Variable-length string, up to 2,000,000 characters; `n` counts characters (not bytes); empty string is `NULL` |
| `CHAR(n)` | Fixed-length, blank-padded; up to 2,000 characters |
| `DECIMAL(p, s)` | Exact numeric; also written `NUMERIC(p, s)`; default `DECIMAL(18, 0)` |
| `DOUBLE PRECISION` | 64-bit floating point; also `FLOAT` or `DOUBLE`; `NaN` is treated as `NULL`; `Infinity` is not supported |
| `BOOLEAN` | `TRUE` / `FALSE` |
| `DATE` | Calendar date only (no time component) |
| `TIMESTAMP(p)` | Date + time; precision `p` 0–9, default **3** (milliseconds) |
| `TIMESTAMP WITH LOCAL TIME ZONE` | Stored in UTC, displayed in session time zone |
| `INTERVAL YEAR TO MONTH` | Period expressed as years and months |
| `INTERVAL DAY TO SECOND` | Period expressed as days through seconds |
| `HASHTYPE(n BYTE)` | Fixed-length hash/binary; 1–1,024 bytes (default 16); accepts hex, UUID, base64 |
| `GEOMETRY` | Spatial data type |

## Row Limiting

Exasol uses `LIMIT`/`OFFSET`. `FETCH FIRST … ROWS ONLY` is **not** supported.

```sql
SELECT * FROM my_table ORDER BY id LIMIT 100;
SELECT * FROM my_table ORDER BY id LIMIT 20 OFFSET 10;
```

`OFFSET` requires `ORDER BY`. `LIMIT` is not allowed in correlated subqueries.

## String Operations

- Concatenation: use `||`, not `+`
- `SUBSTR(str, pos, len)` — 1-based positions
- `LENGTH(str)` — character count
- `TRIM`, `LTRIM`, `RTRIM` — whitespace or specific characters
- `UPPER`, `LOWER`, `INITCAP`
- `REPLACE(str, search, replacement)`
- `LPAD(str, len, pad)`, `RPAD(str, len, pad)`

## Regular Expressions

```sql
-- Boolean match (REGEXP_LIKE is not supported; use REGEXP_INSTR or REGEXP_COUNT instead)
WHERE REGEXP_INSTR(column, '^[A-Z]+$') > 0

-- Count matches
SELECT REGEXP_COUNT(column, '[0-9]+')

-- Replace with regex
SELECT REGEXP_REPLACE(column, '[0-9]+', '#')

-- Extract a match
SELECT REGEXP_SUBSTR(column, '[0-9]+', 1, 1)

-- Find position of first match
SELECT REGEXP_INSTR(column, '[0-9]+')
```

## Date and Time Functions

```sql
-- Parse strings to date/timestamp
TO_DATE('2024-01-15', 'YYYY-MM-DD')
TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')

-- Format dates as strings
TO_CHAR(my_date, 'YYYY-MM-DD')

-- Arithmetic
ADD_DAYS(my_date, 7)
ADD_MONTHS(my_date, 3)
MONTHS_BETWEEN(date1, date2)

-- Truncate to period
TRUNC(my_timestamp, 'MM')   -- first day of month
TRUNC(my_timestamp, 'DD')   -- midnight

-- Current time
CURRENT_DATE
CURRENT_TIMESTAMP
SYSTIMESTAMP    -- like CURRENT_TIMESTAMP but uses database time zone (not session time zone)
```

## NULL Handling

- Any arithmetic or string operation involving `NULL` returns `NULL`.
- Use `IS NULL` / `IS NOT NULL`, never `= NULL`.
- `NVL(expr, default)` — return default if expr is NULL
- `NVL2(expr, val_if_not_null, val_if_null)`
- `NULLIF(a, b)` — returns NULL if a equals b
- `COALESCE(a, b, c, ...)` — first non-NULL value

## Aggregation and GROUP BY

- Columns in `SELECT` that are not aggregated must appear in `GROUP BY`.
- `GROUP BY` accepts column positions: `GROUP BY 1, 2`.
- `HAVING` filters groups after aggregation.
- `ROLLUP`, `CUBE`, `GROUPING SETS` are supported.
- `COUNT(DISTINCT col)` is supported.

## Window Functions

Fully supported. Standard syntax:

```sql
SELECT
    col,
    ROW_NUMBER() OVER (PARTITION BY grp ORDER BY col) AS rn,
    SUM(val) OVER (PARTITION BY grp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
FROM my_table;
```

## Joins

Standard `INNER JOIN`, `LEFT/RIGHT/FULL OUTER JOIN`, `CROSS JOIN`. `NATURAL JOIN` is **not** supported.

## Set Operations

`UNION`, `UNION ALL`, `INTERSECT`, and `EXCEPT` work as in standard SQL. `MINUS` is an Oracle-compatible alias for `EXCEPT`:

```sql
SELECT * FROM a EXCEPT SELECT * FROM b;  -- standard
SELECT * FROM a MINUS SELECT * FROM b;   -- identical, Oracle-compatible
```

## MERGE

```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET t.val = s.val
WHEN NOT MATCHED THEN
    INSERT (id, val) VALUES (s.id, s.val);
```

## Hierarchical Queries (CONNECT BY)

```sql
SELECT level, id, parent_id, name
FROM hierarchy
START WITH parent_id IS NULL
CONNECT BY PRIOR id = parent_id
ORDER SIBLINGS BY name;
```

## Subqueries and CTEs

```sql
-- Common Table Expression
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn
    FROM my_table
)
SELECT * FROM ranked WHERE rn <= 10;

-- Scalar subquery
SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.cust_id = c.id) AS order_count
FROM customers c;
```

## CASE Expression

```sql
-- Searched form
CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END

-- Simple form
CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END
```

## Type Casting

```sql
-- Explicit cast
CAST(my_col AS VARCHAR(100))
CAST('42' AS INTEGER)

-- Implicit coercion: Exasol coerces VARCHAR to numeric in arithmetic,
-- but be explicit to avoid surprises
```

## Common Pitfalls

- **Case of stored names**: querying `EXA_ALL_COLUMNS` returns uppercase names unless the table was created with quoted identifiers.
- **VARCHAR size is in characters**: `VARCHAR(10)` means 10 characters (not bytes), regardless of UTF-8 encoding.
- **Empty string is NULL**: `''` is stored and returned as `NULL` in `VARCHAR` columns.
- **Timestamp precision**: default `TIMESTAMP` (i.e. `TIMESTAMP(3)`) has millisecond precision; use `TIMESTAMP(6)` for microseconds. Valid precision values are **0–9**; values outside this range cause an error.
