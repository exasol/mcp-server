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
| `EXA_ALL_SESSIONS` | All sessions (DBA sees all, others see their own) |
| `EXA_DBA_SESSIONS` | All sessions with full detail (DBA only) |
| `EXA_DBA_TRANSACTION_CONFLICTS` | Currently blocked transactions |

### Query History

| Table | Period | Contents |
|---|---|---|
| `EXA_MONITOR_LAST_DAY` | Last 24 hours | Statement-level metrics: `START_TIME`, `STOP_TIME`, `USER_NAME`, `COMMAND_NAME`, `SQL_TEXT`, `CPU`, `MEM`, `HDD`, `NET`, `ROW_COUNT`, `SUCCESS` |
| `EXA_STATISTICS_LAST_DAY` | Last 24 hours | Aggregated hourly DB metrics |
| `EXA_STATISTICS_LAST_MONTH` | Last 30 days | Daily aggregated DB metrics |
| `EXA_STATISTICS_LAST_YEAR` | Last year | Monthly aggregated DB metrics |

`EXA_MONITOR_LAST_DAY` is the most useful for debugging recent query performance.

### System Events

| Table | Contents |
|---|---|
| `EXA_SYSTEM_EVENTS` | System-level events: startups, shutdowns, backups, node failures |

### Auditing (DBA only)

| Table | Contents |
|---|---|
| `EXA_DBA_AUDIT_SESSIONS` | Session open/close records |
| `EXA_DBA_AUDIT_SQL` | Full SQL text of executed statements with timestamps and user names |

---

## Useful Queries

### Find all tables containing a column name

```sql
SELECT COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE
FROM EXA_ALL_COLUMNS
WHERE UPPER(COLUMN_NAME) LIKE '%CUSTOMER%'
ORDER BY COLUMN_SCHEMA, COLUMN_TABLE;
```

### Find what depends on a table

```sql
SELECT REFERENCING_OBJECT_SCHEMA, REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_TYPE
FROM EXA_ALL_DEPENDENCIES
WHERE REFERENCED_OBJECT_SCHEMA = 'MY_SCHEMA'
  AND REFERENCED_OBJECT_NAME   = 'MY_TABLE';
```

### Find slow queries in the last hour

```sql
SELECT SQL_TEXT,
       SECONDS_BETWEEN(START_TIME, STOP_TIME) AS duration_sec,
       USER_NAME, SUCCESS
FROM EXA_MONITOR_LAST_DAY
WHERE START_TIME > ADD_HOURS(CURRENT_TIMESTAMP, -1)
ORDER BY duration_sec DESC
FETCH FIRST 20 ROWS ONLY;
```

### Find large tables by row count

```sql
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROW_COUNT
FROM EXA_ALL_TABLES
WHERE TABLE_TYPE = 'TABLE'
ORDER BY TABLE_ROW_COUNT DESC NULLS LAST
FETCH FIRST 20 ROWS ONLY;
```

### Check active sessions

```sql
SELECT SESSION_ID, USER_NAME, STATUS, COMMAND_NAME, SQL_TEXT
FROM EXA_USER_SESSIONS
ORDER BY LOGIN_TIME DESC;
```

---

## When to Use System Tables vs MCP Tools

**Prefer MCP tools** for simple, single-object lookups:
- `describe_exasol_table_or_view` instead of querying `EXA_ALL_COLUMNS` for one table.
- `list_exasol_user_defined_functions` instead of querying `EXA_ALL_SCRIPTS`.
- MCP tools apply server-side filtering, ranking, and consistent field naming.

**Prefer direct system table queries** (`execute_exasol_query`) for:
- Cross-schema or cross-object analysis (e.g., all columns matching a pattern across all schemas).
- Joining system tables (e.g., columns with their constraint information).
- Performance analysis from `EXA_MONITOR_LAST_DAY`.
- Complex filtering or aggregation over metadata.

Use `list_exasol_system_tables` and `list_exasol_statistics_tables` to discover which tables are available on the connected database instance, since availability can vary by version and license.
