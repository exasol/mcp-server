---
description: "Exasol system and statistics tables: what they contain, visibility prefixes (EXA_ALL_*, EXA_DBA_*, EXA_USER_*), and when to query them directly vs using MCP tools."
tags: ["exasol", "system-tables", "statistics", "metadata"]
---

# Exasol System and Statistics Tables

## Visibility Prefixes

Exasol system tables come in three visibility levels:

| Prefix | Accessible to | Shows |
|---|---|---|
| `EXA_ALL_*` | All users | Objects the current user has privileges on |
| `EXA_DBA_*` | DBA users only | All objects in the database |
| `EXA_USER_*` | All users | Objects owned by the current user |

Most day-to-day queries use `EXA_ALL_*`. If you need a complete picture (e.g., for auditing or capacity planning), use `EXA_DBA_*` (requires DBA role).

---

## Metadata Tables

### Schemas and Objects

| Table | Contents |
|---|---|
| `EXA_ALL_SCHEMAS` | All accessible schemas with `SCHEMA_NAME`, `SCHEMA_OWNER`, `SCHEMA_COMMENT` |
| `EXA_ALL_TABLES` | Tables and views: `TABLE_SCHEMA`, `TABLE_NAME`, `TABLE_TYPE` (`TABLE`/`VIEW`), `TABLE_ROW_COUNT`, `TABLE_COMMENT` |
| `EXA_ALL_COLUMNS` | Columns: `COLUMN_SCHEMA`, `COLUMN_TABLE`, `COLUMN_NAME`, `COLUMN_TYPE`, `COLUMN_ORDINAL_POSITION`, `COLUMN_DEFAULT`, `COLUMN_IS_NULLABLE`, `COLUMN_COMMENT` |
| `EXA_ALL_VIEWS` | View definitions: `VIEW_SCHEMA`, `VIEW_NAME`, `VIEW_TEXT` (DDL of the view) |
| `EXA_ALL_CONSTRAINTS` | Primary keys, foreign keys, not-null: `CONSTRAINT_SCHEMA`, `CONSTRAINT_TABLE`, `CONSTRAINT_NAME`, `CONSTRAINT_TYPE` |
| `EXA_ALL_CONSTRAINT_COLUMNS` | Which columns belong to each constraint |

### Storage

| Table | Contents |
|---|---|
| `EXA_ALL_OBJECT_SIZES` | Per-object storage metrics: `OBJECT_SCHEMA`, `OBJECT_NAME`, `OBJECT_TYPE`, `RAW_OBJECT_SIZE` (uncompressed bytes), `MEM_OBJECT_SIZE` (compressed in-memory bytes), `RAW_INDICES_SIZE`, `MEM_INDICES_SIZE`, `LAST_COMMIT` (timestamp of last write activity) |

`EXA_ALL_OBJECT_SIZES` is the primary source for table size and freshness information. Join it with `EXA_ALL_TABLES` on schema and name to combine row counts with storage metrics.

### Indices

| Table | Contents |
|---|---|
| `EXA_DBA_INDICES` | Auto-managed indices (DBA only): `INDEX_SCHEMA`, `INDEX_TABLE`, `INDEX_TYPE` (e.g. `ENUM_KEY`, `ENUM_GROUP`), `RAW_OBJECT_SIZE`, `MEM_OBJECT_SIZE`, `LAST_COMMIT` |

Exasol manages indices automatically; `EXA_DBA_INDICES` is useful for understanding index memory consumption and staleness.

### Functions and Scripts

| Table | Contents |
|---|---|
| `EXA_ALL_FUNCTIONS` | Custom SQL functions: `FUNCTION_SCHEMA`, `FUNCTION_NAME`, `FUNCTION_TEXT` |
| `EXA_ALL_SCRIPTS` | UDF scripts: `SCRIPT_SCHEMA`, `SCRIPT_NAME`, `SCRIPT_LANGUAGE`, `SCRIPT_TYPE`, `SCRIPT_INPUT_TYPE`, `SCRIPT_RESULT_TYPE`, `SCRIPT_TEXT` |
| `EXA_ALL_SCRIPT_LANGUAGES` | Registered Script Language Containers: `LANGUAGE_ALIAS`, `LANGUAGE_PATH` |

### Privileges and Users

| Table | Contents |
|---|---|
| `EXA_ALL_OBJECT_PRIVILEGES` | Which users/roles have which privileges on which objects |
| `EXA_DBA_USERS` | All database users: `USER_NAME`, `USER_COMMENT`, `DISTINGUISHED_NAME` |
| `EXA_DBA_ROLES` | All roles and their grant chains |
| `EXA_DBA_ROLE_PRIVS` | Role membership |

### Dependencies

| Table | Contents |
|---|---|
| `EXA_ALL_DEPENDENCIES` | Object dependency graph: `REFERENCED_OBJECT_SCHEMA`, `REFERENCED_OBJECT_NAME`, `REFERENCED_OBJECT_TYPE`, `REFERENCING_*` columns |

Use this to find what views or functions depend on a table before altering or dropping it.

---

## Statistics Tables

Statistics tables capture runtime performance metrics. Access them via the MCP tools `list_exasol_statistics_tables` and `describe_exasol_statistics_table`, or query them directly with `execute_exasol_query`.

### Session and Process Tables

| Table | Contents |
|---|---|
| `EXA_USER_SESSIONS` | Sessions belonging to the current user |
| `EXA_ALL_SESSIONS` | All sessions (DBA sees all, others see their own): `SESSION_ID`, `USER_NAME`, `STATUS`, `COMMAND_NAME`, `SQL_TEXT` |
| `EXA_DBA_SESSIONS` | All sessions with full detail (DBA only): all `EXA_ALL_SESSIONS` columns plus `ACTIVITY` (what the session is currently doing), `DURATION` (seconds the current statement has been running), `CONSUMER_GROUP`, `RESOURCES` (allocated CPU %) |
| `EXA_DBA_TRANSACTION_CONFLICTS` | Transaction conflicts: `START_TIME`, `STOP_TIME`, `CONFLICT_SESSION_ID`, `CONFLICT_TYPE`, `OBJECT_SCHEMA`, `OBJECT_NAME`, `DURATION` |

`EXA_DBA_SESSIONS` is richer than `EXA_ALL_SESSIONS` for session monitoring dashboards — it includes the running SQL text and resource consumption that `EXA_ALL_SESSIONS` omits.

### Query History

| Table | Period | Contents |
|---|---|---|
| `EXA_MONITOR_LAST_DAY` | Last 24 hours | Statement-level metrics: `START_TIME`, `STOP_TIME`, `USER_NAME`, `COMMAND_NAME`, `SQL_TEXT`, `CPU`, `MEM`, `HDD`, `NET`, `ROW_COUNT`, `SUCCESS` |
| `EXA_SQL_LAST_DAY` | Last 24 hours | Per-statement performance: `SESSION_ID`, `STMT_ID`, `DURATION`, `CPU`, `HDD_READ`, `HDD_WRITE`, `NET`. Join with `EXA_DBA_AUDIT_SQL` on `SESSION_ID` and `STMT_ID` to attach SQL text and command metadata to the performance figures. |
| `EXA_STATISTICS_LAST_DAY` | Last 24 hours | Aggregated hourly DB metrics |
| `EXA_STATISTICS_LAST_MONTH` | Last 30 days | Daily aggregated DB metrics |
| `EXA_STATISTICS_LAST_YEAR` | Last year | Monthly aggregated DB metrics |

### System Events

| Table | Contents |
|---|---|
| `EXA_SYSTEM_EVENTS` | System-level events: `EVENT_TYPE` (STARTUP, SHUTDOWN, BACKUP, RESTORE, FAILOVER, RESTART), `EVENT_TIME`, `DBMS_VERSION`, `NODES` (node count at event time), `DB_RAM_SIZE`, `CLUSTER_NAME` |

### Auditing (DBA only)

| Table | Contents |
|---|---|
| `EXA_DBA_AUDIT_SESSIONS` | Session open/close records |
| `EXA_DBA_AUDIT_SQL` | Full SQL text of executed statements: `SESSION_ID`, `STMT_ID`, `COMMAND_NAME`, `SQL_TEXT`, `SUCCESS`, `ERROR_MESSAGE` |

---

## Useful Queries

### Find slow queries in the last 24 hours (with SQL text)

```sql
SELECT a.SQL_TEXT, s.DURATION, s.CPU, s.HDD_READ, a.COMMAND_NAME, a.SUCCESS
FROM EXA_STATISTICS.EXA_SQL_LAST_DAY s
INNER JOIN EXA_STATISTICS.EXA_DBA_AUDIT_SQL a
    ON s.SESSION_ID = a.SESSION_ID AND s.STMT_ID = a.STMT_ID
WHERE s.DURATION >= 10
ORDER BY s.DURATION DESC
FETCH FIRST 20 ROWS ONLY;
```

### Table sizes and freshness

```sql
SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_ROW_COUNT,
       o.RAW_OBJECT_SIZE, o.MEM_OBJECT_SIZE, o.LAST_COMMIT
FROM SYS.EXA_ALL_TABLES t
JOIN SYS.EXA_ALL_OBJECT_SIZES o
    ON t.TABLE_SCHEMA = o.OBJECT_SCHEMA AND t.TABLE_NAME = o.OBJECT_NAME
WHERE t.TABLE_TYPE = 'TABLE'
ORDER BY o.RAW_OBJECT_SIZE DESC NULLS LAST
FETCH FIRST 30 ROWS ONLY;
```

### Schema storage summary

```sql
SELECT OBJECT_SCHEMA,
       COUNT(*) AS table_count,
       SUM(RAW_OBJECT_SIZE) AS total_raw_bytes,
       SUM(MEM_OBJECT_SIZE) AS total_compressed_bytes,
       MAX(LAST_COMMIT) AS last_write
FROM SYS.EXA_ALL_OBJECT_SIZES
WHERE OBJECT_TYPE = 'TABLE'
GROUP BY OBJECT_SCHEMA
ORDER BY total_raw_bytes DESC NULLS LAST;
```

### Recent transaction conflicts

```sql
SELECT START_TIME, STOP_TIME, DURATION, CONFLICT_TYPE,
       OBJECT_SCHEMA, OBJECT_NAME, CONFLICT_SESSION_ID
FROM EXA_STATISTICS.EXA_DBA_TRANSACTION_CONFLICTS
ORDER BY START_TIME DESC
FETCH FIRST 20 ROWS ONLY;
```

### Index overview for a schema

```sql
SELECT INDEX_TABLE, INDEX_TYPE, MEM_OBJECT_SIZE, LAST_COMMIT
FROM SYS.EXA_ALL_INDICES
WHERE INDEX_SCHEMA = 'MY_SCHEMA'
ORDER BY MEM_OBJECT_SIZE DESC NULLS LAST;
```

### Find all tables containing a column name

```sql
SELECT COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE
FROM SYS.EXA_ALL_COLUMNS
WHERE UPPER(COLUMN_NAME) LIKE '%CUSTOMER%'
ORDER BY COLUMN_SCHEMA, COLUMN_TABLE;
```

### Find what depends on a table

```sql
SELECT REFERENCING_OBJECT_SCHEMA, REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_TYPE
FROM SYS.EXA_ALL_DEPENDENCIES
WHERE REFERENCED_OBJECT_SCHEMA = 'MY_SCHEMA'
  AND REFERENCED_OBJECT_NAME   = 'MY_TABLE';
```

### Check active sessions

```sql
SELECT SESSION_ID, USER_NAME, STATUS, COMMAND_NAME, DURATION, SQL_TEXT
FROM EXA_STATISTICS.EXA_DBA_SESSIONS
ORDER BY DURATION DESC NULLS LAST;
```

---

## When to Use System Tables vs MCP Tools

**Prefer MCP tools** for simple, single-object lookups:
- `describe_exasol_table_or_view` instead of querying `EXA_ALL_COLUMNS` for one table.
- `list_exasol_user_defined_functions` instead of querying `EXA_ALL_SCRIPTS`.
- MCP tools apply server-side filtering, ranking, and consistent field naming.

**Prefer direct system table queries** (`execute_exasol_query`) for:
- Cross-schema or cross-object analysis (e.g., all columns matching a pattern across all schemas).
- Joining system tables (e.g., `EXA_SQL_LAST_DAY` with `EXA_DBA_AUDIT_SQL` for performance + SQL text).
- Storage analysis via `EXA_ALL_OBJECT_SIZES`.
- Performance monitoring from `EXA_MONITOR_LAST_DAY` or `EXA_SQL_LAST_DAY`.
- Transaction conflict and session activity analysis.

Use `list_exasol_system_tables` and `list_exasol_statistics_tables` to discover which tables are available on the connected database instance, since availability can vary by version and license.
