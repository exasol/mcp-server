---
description: "Exasol SQL dialect specifics: syntax, data types, functions, and common pitfalls for generating correct Exasol SQL."
tags: ["exasol", "sql", "dialect"]
---

# Exasol SQL Dialect

## Identifiers

- Identifiers (schema, table, column names) are **case-insensitive** by default and stored uppercase.
- To preserve case or use reserved words as identifiers, wrap them in **double quotes**: `"myColumn"`.
- String literals use **single quotes**: `'value'`.

## Data Types

| Type | Notes |
|---|---|
| `VARCHAR(n CHAR)` | Variable-length string; use `CHAR` suffix to count characters rather than bytes |
| `CHAR(n)` | Fixed-length, blank-padded |
| `DECIMAL(p, s)` | Exact numeric; also written `NUMERIC(p, s)` |
| `DOUBLE PRECISION` | 64-bit floating point; also `FLOAT` or `DOUBLE` |
| `BOOLEAN` | `TRUE` / `FALSE` |
| `DATE` | Calendar date only (no time component) |
| `TIMESTAMP` | Date + time |
| `TIMESTAMP WITH LOCAL TIME ZONE` | Stored in UTC, displayed in session time zone |
| `INTERVAL YEAR TO MONTH` | Period expressed as years and months |
| `INTERVAL DAY TO SECOND` | Period expressed as days through seconds |
| `GEOMETRY` | Spatial data type |

## Row Limiting

Exasol does **not** support `LIMIT n`. Use standard SQL row limiting:

```sql
-- Fetch first N rows
SELECT * FROM my_table FETCH FIRST 100 ROWS ONLY;

-- Offset + limit
SELECT * FROM my_table OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY;
```

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
-- Boolean match
WHERE REGEXP_LIKE(column, '^[A-Z]+$')

-- Replace with regex
SELECT REGEXP_REPLACE(column, '[0-9]+', '#')

-- Extract a match
SELECT REGEXP_SUBSTR(column, '[0-9]+', 1, 1)
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
SYSTIMESTAMP    -- same as CURRENT_TIMESTAMP
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

Standard `INNER JOIN`, `LEFT/RIGHT/FULL OUTER JOIN`, `CROSS JOIN`. Also supports:

```sql
-- Natural join (match on same-named columns)
FROM a NATURAL JOIN b

-- Partition outer join (Oracle/Exasol extension)
FROM a LEFT OUTER JOIN b PARTITION BY (b.dim) ON a.key = b.key
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

- **No `LIMIT`**: always use `FETCH FIRST n ROWS ONLY`.
- **Case of stored names**: querying `EXA_ALL_COLUMNS` returns uppercase names unless the table was created with quoted identifiers.
- **VARCHAR byte vs char**: `VARCHAR(10)` means 10 bytes in the default character set; use `VARCHAR(10 CHAR)` to measure in characters.
- **Integer division**: `5 / 2` returns `2` (integer). Cast to `DECIMAL` for fractional result: `CAST(5 AS DECIMAL) / 2`.
- **Empty string vs NULL**: Exasol treats `''` (empty string) as an empty string, not NULL (unlike Oracle).
- **Timestamp precision**: default `TIMESTAMP` has microsecond precision.
