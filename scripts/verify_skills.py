"""
Verification script for Exasol skill documentation claims.
Tests against the live demo database.
"""

import os

import pyexasol

dsn = os.environ["EXA_DSN"]
user = os.environ["EXA_USER"]
password = os.environ["EXA_PASSWORD"]
SCHEMA = os.environ["EXA_SCHEMA"]  # The schema with write permission

conn = pyexasol.connect(dsn=dsn, user=user, password=password)

results = []


def ok(name, val=True, note=""):
    status = "PASS" if val else "FAIL"
    results.append((status, name, note))
    mark = "✓" if val else "✗"
    extra = f"  [{note}]" if note else ""
    print(f"  {mark} {name}{extra}")
    return val


def err(name, exc, note=""):
    results.append(("ERROR", name, f"{exc}: {note}"))
    print(f"  ! {name}: {exc}")


def q1(sql, **kw):
    """Return first column of first row."""
    return conn.execute(sql, **kw).fetchone()[0]


def qall(sql, **kw):
    return conn.execute(sql, **kw).fetchall()


def fails(sql):
    """Return True if SQL raises an exception."""
    try:
        conn.execute(sql)
        return False
    except Exception:
        return True


def cleanup(*ddls):
    for ddl in ddls:
        try:
            conn.execute(ddl)
        except Exception:
            pass


# ─────────────────────────────────────────────
print("\n=== SQL DIALECT ===")
# ─────────────────────────────────────────────

try:
    # Identifiers are stored uppercase
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_test_ids (myColumn VARCHAR(10))"
    )
    col = q1(
        f"SELECT COLUMN_NAME FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE='VERIFY_TEST_IDS' AND COLUMN_SCHEMA='{SCHEMA}'"
    )
    ok("Identifiers stored uppercase", col == "MYCOLUMN", f"got {col!r}")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_test_ids")
except Exception as e:
    err("Identifiers stored uppercase", e)

try:
    ok("Empty string is NULL", q1("SELECT '' IS NULL") == True)
except Exception as e:
    err("Empty string is NULL", e)

try:
    ok("String concat with ||", q1("SELECT 'foo' || 'bar'") == "foobar")
except Exception as e:
    err("String concat with ||", e)

try:
    ok("SUBSTR is 1-based", q1("SELECT SUBSTR('abc', 1, 1)") == "a")
except Exception as e:
    err("SUBSTR is 1-based", e)

try:
    ok("LENGTH returns character count", q1("SELECT LENGTH('café')") == 4)
except Exception as e:
    err("LENGTH returns character count", e)

try:
    ok("INITCAP function exists", q1("SELECT INITCAP('hello world')") == "Hello World")
except Exception as e:
    err("INITCAP function exists", e)

try:
    ok("UPPER/LOWER work", q1("SELECT UPPER('hello')") == "HELLO")
except Exception as e:
    err("UPPER/LOWER work", e)

try:
    ok("REPLACE function", q1("SELECT REPLACE('aXbXc', 'X', '-')") == "a-b-c")
except Exception as e:
    err("REPLACE function", e)

try:
    ok("LPAD function", q1("SELECT LPAD('x', 3, '0')") == "00x")
except Exception as e:
    err("LPAD function", e)

try:
    ok("RPAD function", q1("SELECT RPAD('x', 3, '0')") == "x00")
except Exception as e:
    err("RPAD function", e)

# FETCH FIRST not supported
try:
    ok("FETCH FIRST not supported", fails("SELECT 1 FROM DUAL FETCH FIRST 1 ROWS ONLY"))
except Exception as e:
    err("FETCH FIRST not supported", e)

# LIMIT supported
try:
    ok("LIMIT supported", q1("SELECT 1 FROM DUAL LIMIT 1") == 1)
except Exception as e:
    err("LIMIT supported", e)

# OFFSET requires ORDER BY
try:
    ok(
        "OFFSET requires ORDER BY (errors without it)",
        fails("SELECT * FROM DUAL LIMIT 5 OFFSET 2"),
    )
except Exception as e:
    err("OFFSET requires ORDER BY", e)

# OFFSET works with ORDER BY
try:
    val = q1(
        "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 3 AS x) ORDER BY x LIMIT 1 OFFSET 1"
    )
    ok("OFFSET works with ORDER BY", val == 2, f"got {val}")
except Exception as e:
    err("OFFSET works with ORDER BY", e)

# MINUS as alias for EXCEPT
try:
    r1 = qall("SELECT 1 FROM DUAL EXCEPT SELECT 2 FROM DUAL")
    r2 = qall("SELECT 1 FROM DUAL MINUS SELECT 2 FROM DUAL")
    ok("MINUS alias for EXCEPT", r1 == r2, f"EXCEPT={r1}, MINUS={r2}")
except Exception as e:
    err("MINUS alias for EXCEPT", e)

# UNION, INTERSECT, EXCEPT all work
try:
    r = qall("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL")
    ok("UNION ALL works", len(r) == 2)
except Exception as e:
    err("UNION ALL works", e)

try:
    r = q1("SELECT 1 FROM DUAL INTERSECT SELECT 1 FROM DUAL")
    ok("INTERSECT works", r == 1)
except Exception as e:
    err("INTERSECT works", e)

# REGEXP functions
try:
    ok("REGEXP_LIKE", q1("SELECT REGEXP_LIKE('hello123', '[0-9]+')") == True)
except Exception as e:
    err("REGEXP_LIKE", e)

try:
    ok(
        "REGEXP_REPLACE",
        q1("SELECT REGEXP_REPLACE('hello123', '[0-9]+', '#')") == "hello#",
    )
except Exception as e:
    err("REGEXP_REPLACE", e)

try:
    ok(
        "REGEXP_SUBSTR",
        q1("SELECT REGEXP_SUBSTR('hello123world', '[0-9]+', 1, 1)") == "123",
    )
except Exception as e:
    err("REGEXP_SUBSTR", e)

# Date/time functions
try:
    ok("TO_DATE", str(q1("SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD')")) == "2024-01-15")
except Exception as e:
    err("TO_DATE", e)

try:
    v = q1("SELECT TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')")
    ok("TO_TIMESTAMP", v is not None)
except Exception as e:
    err("TO_TIMESTAMP", e)

try:
    ok(
        "TO_CHAR for dates",
        q1("SELECT TO_CHAR(TO_DATE('2024-01-15', 'YYYY-MM-DD'), 'YYYY-MM-DD')")
        == "2024-01-15",
    )
except Exception as e:
    err("TO_CHAR for dates", e)

try:
    ok(
        "ADD_DAYS",
        str(q1("SELECT ADD_DAYS(TO_DATE('2024-01-01', 'YYYY-MM-DD'), 7)"))
        == "2024-01-08",
    )
except Exception as e:
    err("ADD_DAYS", e)

try:
    ok(
        "ADD_MONTHS",
        str(q1("SELECT ADD_MONTHS(TO_DATE('2024-01-15', 'YYYY-MM-DD'), 3)"))
        == "2024-04-15",
    )
except Exception as e:
    err("ADD_MONTHS", e)

try:
    v = q1(
        "SELECT MONTHS_BETWEEN(TO_DATE('2024-04-15', 'YYYY-MM-DD'), TO_DATE('2024-01-15', 'YYYY-MM-DD'))"
    )
    ok("MONTHS_BETWEEN", float(v) == 3.0, f"got {v}")
except Exception as e:
    err("MONTHS_BETWEEN", e)

try:
    v = str(q1("SELECT TRUNC(TO_DATE('2024-03-15', 'YYYY-MM-DD'), 'MM')"))
    ok("TRUNC date to month", v == "2024-03-01", f"got {v!r}")
except Exception as e:
    err("TRUNC date to month", e)

try:
    ok("CURRENT_DATE accessible", q1("SELECT CURRENT_DATE") is not None)
except Exception as e:
    err("CURRENT_DATE", e)

try:
    ok("CURRENT_TIMESTAMP accessible", q1("SELECT CURRENT_TIMESTAMP") is not None)
except Exception as e:
    err("CURRENT_TIMESTAMP", e)

try:
    ok("SYSTIMESTAMP accessible", q1("SELECT SYSTIMESTAMP") is not None)
except Exception as e:
    err("SYSTIMESTAMP", e)

# NULL functions
try:
    ok("NVL returns default on NULL", q1("SELECT NVL(NULL, 42)") == 42)
except Exception as e:
    err("NVL", e)

try:
    ok("NVL2 val_if_not_null", q1("SELECT NVL2('x', 'notnull', 'isnull')") == "notnull")
    ok("NVL2 val_if_null", q1("SELECT NVL2(NULL, 'notnull', 'isnull')") == "isnull")
except Exception as e:
    err("NVL2", e)

try:
    ok("NULLIF returns NULL when equal", q1("SELECT NULLIF(1, 1)") is None)
    ok("NULLIF returns first when unequal", q1("SELECT NULLIF(1, 2)") == 1)
except Exception as e:
    err("NULLIF", e)

try:
    ok("COALESCE returns first non-NULL", q1("SELECT COALESCE(NULL, NULL, 3, 4)") == 3)
except Exception as e:
    err("COALESCE", e)

# Data types
try:
    ok(
        "DECIMAL and NUMERIC are synonyms",
        q1("SELECT CAST(1 AS DECIMAL(5,2))") == q1("SELECT CAST(1 AS NUMERIC(5,2))"),
    )
except Exception as e:
    err("DECIMAL/NUMERIC synonyms", e)

try:
    ok(
        "DOUBLE PRECISION alias FLOAT alias DOUBLE",
        q1("SELECT CAST(1.5 AS DOUBLE PRECISION)")
        == q1("SELECT CAST(1.5 AS FLOAT)")
        == q1("SELECT CAST(1.5 AS DOUBLE)"),
    )
except Exception as e:
    err("DOUBLE/FLOAT/DOUBLE PRECISION synonyms", e)

try:
    # NaN treated as NULL in DOUBLE
    ok(
        "NaN treated as NULL in DOUBLE",
        q1("SELECT CAST('NaN' AS DOUBLE) IS NULL") == True,
    )
except Exception as e:
    err("NaN treated as NULL in DOUBLE", e)

try:
    # TIMESTAMP default precision is 3 (milliseconds)
    # Create a table and check precision
    conn.execute(f"CREATE OR REPLACE TABLE {SCHEMA}.verify_ts (ts TIMESTAMP)")
    prec = q1(
        f"SELECT COLUMN_NUM_PREC FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE='VERIFY_TS' AND COLUMN_SCHEMA='{SCHEMA}' AND COLUMN_NAME='TS'"
    )
    ok("TIMESTAMP default precision is 3", prec == 3, f"got {prec}")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_ts")
except Exception as e:
    err("TIMESTAMP default precision is 3", e)

try:
    # VARCHAR n is characters not bytes - test with multi-byte char
    conn.execute(f"CREATE OR REPLACE TABLE {SCHEMA}.verify_varchar (v VARCHAR(3))")
    conn.execute(
        f"INSERT INTO {SCHEMA}.verify_varchar VALUES ('€€€')"
    )  # each € is 3 bytes in UTF-8
    val = q1(f"SELECT v FROM {SCHEMA}.verify_varchar")
    ok(
        "VARCHAR n is char count (not bytes): 3 multi-byte chars fit in VARCHAR(3)",
        val == "€€€",
        f"got {val!r}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_varchar")
except Exception as e:
    err("VARCHAR n is char count", e)

try:
    # HASHTYPE data type
    conn.execute(f"CREATE OR REPLACE TABLE {SCHEMA}.verify_hash (h HASHTYPE(16 BYTE))")
    ok("HASHTYPE(n BYTE) supported", True)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_hash")
except Exception as e:
    err("HASHTYPE data type", e)

# GROUP BY with column positions
try:
    r = qall(
        "SELECT x, COUNT(*) FROM (SELECT 1 AS x UNION ALL SELECT 1 AS x UNION ALL SELECT 2 AS x) GROUP BY 1 ORDER BY 1"
    )
    ok("GROUP BY column position", r == [(1, 2), (2, 1)], f"got {r}")
except Exception as e:
    err("GROUP BY column position", e)

# ROLLUP
try:
    r = qall(
        "SELECT x, SUM(n) FROM (SELECT 1 AS x, 10 AS n UNION ALL SELECT 2 AS x, 20 AS n) GROUP BY ROLLUP(x) ORDER BY x NULLS LAST"
    )
    ok("ROLLUP supported", len(r) == 3, f"got {r}")  # 2 groups + 1 grand total
except Exception as e:
    err("ROLLUP supported", e)

# CUBE
try:
    r = qall(
        "SELECT x, y, COUNT(*) FROM (SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2 AS x, 'b' AS y) GROUP BY CUBE(x, y) ORDER BY x NULLS LAST, y NULLS LAST"
    )
    ok("CUBE supported", len(r) >= 4, f"got {r}")
except Exception as e:
    err("CUBE supported", e)

# GROUPING SETS
try:
    r = qall(
        "SELECT x, y, COUNT(*) FROM (SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2 AS x, 'b' AS y) GROUP BY GROUPING SETS((x), (y))"
    )
    ok("GROUPING SETS supported", len(r) >= 2, f"got {r}")
except Exception as e:
    err("GROUPING SETS supported", e)

# COUNT(DISTINCT col)
try:
    ok(
        "COUNT(DISTINCT) supported",
        q1(
            "SELECT COUNT(DISTINCT x) FROM (SELECT 1 AS x UNION ALL SELECT 1 AS x UNION ALL SELECT 2 AS x)"
        )
        == 2,
    )
except Exception as e:
    err("COUNT(DISTINCT)", e)

# Window functions
try:
    r = qall(
        "SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x)"
    )
    ok("ROW_NUMBER() window function", r == [(1, 1), (2, 2)], f"got {r}")
except Exception as e:
    err("ROW_NUMBER() window function", e)

try:
    r = qall(
        "SELECT x, SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) ORDER BY x"
    )
    ok("SUM() OVER with ROWS BETWEEN", r == [(1, 1), (2, 3)], f"got {r}")
except Exception as e:
    err("SUM() OVER with ROWS BETWEEN", e)

# CONNECT BY
try:
    r = qall("""
        SELECT LEVEL AS lvl, n FROM (
            SELECT 1 AS id, NULL AS parent, 'root' AS n UNION ALL
            SELECT 2 AS id, 1 AS parent, 'child' AS n
        )
        START WITH parent IS NULL
        CONNECT BY PRIOR id = parent
        ORDER BY lvl
    """)
    ok(
        "CONNECT BY hierarchical queries",
        len(r) == 2 and r[0][0] == 1 and r[1][0] == 2,
        f"got {r}",
    )
except Exception as e:
    err("CONNECT BY hierarchical queries", e)

# CTE
try:
    r = q1("WITH cte AS (SELECT 42 AS v) SELECT v FROM cte")
    ok("Common Table Expression (WITH)", r == 42)
except Exception as e:
    err("CTE", e)

# CASE expressions
try:
    ok(
        "CASE WHEN searched form",
        q1("SELECT CASE WHEN 5 > 3 THEN 'yes' ELSE 'no' END") == "yes",
    )
    ok(
        "CASE simple form",
        q1("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END") == "one",
    )
except Exception as e:
    err("CASE expression", e)

# CAST
try:
    ok("CAST to VARCHAR", q1("SELECT CAST(42 AS VARCHAR(10))") == "42")
    ok("CAST to INTEGER", q1("SELECT CAST('42' AS INTEGER)") == 42)
except Exception as e:
    err("CAST", e)

# MERGE
try:
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_merge_target (id INT, val VARCHAR(10))"
    )
    conn.execute(f"INSERT INTO {SCHEMA}.verify_merge_target VALUES (1, 'old')")
    conn.execute(f"""
        MERGE INTO {SCHEMA}.verify_merge_target t
        USING (SELECT 1 AS id, 'new' AS val UNION ALL SELECT 2 AS id, 'inserted' AS val) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.val = s.val
        WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
    """)
    rows = qall(f"SELECT id, val FROM {SCHEMA}.verify_merge_target ORDER BY id")
    ok("MERGE statement works", rows == [(1, "new"), (2, "inserted")], f"got {rows}")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_merge_target")
except Exception as e:
    err("MERGE statement", e)

# NATURAL JOIN
try:
    r = q1("""
        SELECT a.x FROM
        (SELECT 1 AS x) a
        NATURAL JOIN
        (SELECT 1 AS x) b
    """)
    ok("NATURAL JOIN supported", r == 1)
except Exception as e:
    err("NATURAL JOIN", e)

# ─────────────────────────────────────────────
print("\n=== TABLE DESIGN ===")
# ─────────────────────────────────────────────

try:
    conn.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA}.verify_src (
            id DECIMAL(18,0),
            name VARCHAR(100) DEFAULT 'anon',
            ts TIMESTAMP
        )
    """)
    # LIKE INCLUDING DEFAULTS
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_like (LIKE {SCHEMA}.verify_src INCLUDING DEFAULTS)"
    )
    # Check default was copied
    col_default = q1(f"""
        SELECT COLUMN_DEFAULT FROM EXA_ALL_COLUMNS
        WHERE COLUMN_TABLE='VERIFY_LIKE' AND COLUMN_SCHEMA='{SCHEMA}' AND COLUMN_NAME='NAME'
    """)
    ok(
        "LIKE INCLUDING DEFAULTS copies default",
        col_default is not None and "anon" in str(col_default),
        f"got {col_default!r}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_like")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_src")
except Exception as e:
    err("LIKE INCLUDING DEFAULTS", e)

try:
    # CREATE TABLE AS SELECT ... WITH NO DATA
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_ctas AS SELECT * FROM FLIGHTS.FLIGHTS WITH NO DATA"
    )
    cnt = q1(f"SELECT COUNT(*) FROM {SCHEMA}.verify_ctas")
    ok(
        "CREATE TABLE AS SELECT ... WITH NO DATA creates empty table",
        cnt == 0,
        f"got {cnt}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_ctas")
except Exception as e:
    err("CREATE TABLE AS SELECT WITH NO DATA", e)

try:
    # CREATE TABLE AS SELECT (with data)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_ctas2 AS SELECT 1 AS x, 'hello' AS y"
    )
    row = qall(f"SELECT x, y FROM {SCHEMA}.verify_ctas2")
    ok("CREATE TABLE AS SELECT (with data)", row == [(1, "hello")], f"got {row}")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_ctas2")
except Exception as e:
    err("CREATE TABLE AS SELECT (with data)", e)

try:
    # DISTRIBUTE BY
    conn.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA}.verify_dist (
            id DECIMAL(18,0),
            customer_id DECIMAL(18,0),
            DISTRIBUTE BY customer_id
        )
    """)
    dist_key = q1(f"""
        SELECT COLUMN_IS_DISTRIBUTION_KEY FROM EXA_ALL_COLUMNS
        WHERE COLUMN_TABLE='VERIFY_DIST' AND COLUMN_SCHEMA='{SCHEMA}' AND COLUMN_NAME='CUSTOMER_ID'
    """)
    ok(
        "DISTRIBUTE BY sets COLUMN_IS_DISTRIBUTION_KEY=TRUE",
        dist_key == True,
        f"got {dist_key!r}",
    )
    # Check non-dist column
    non_dist = q1(f"""
        SELECT COLUMN_IS_DISTRIBUTION_KEY FROM EXA_ALL_COLUMNS
        WHERE COLUMN_TABLE='VERIFY_DIST' AND COLUMN_SCHEMA='{SCHEMA}' AND COLUMN_NAME='ID'
    """)
    ok(
        "Non-distribution column has COLUMN_IS_DISTRIBUTION_KEY=FALSE",
        non_dist == False,
        f"got {non_dist!r}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_dist")
except Exception as e:
    err("DISTRIBUTE BY and EXA_ALL_COLUMNS.COLUMN_IS_DISTRIBUTION_KEY", e)

try:
    # PARTITION BY (DATE is supported)
    conn.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA}.verify_part (
            id DECIMAL(18,0),
            sale_date DATE,
            PARTITION BY sale_date
        )
    """)
    ok("PARTITION BY DATE supported", True)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_part")
except Exception as e:
    err("PARTITION BY DATE supported", e)

try:
    # PARTITION BY VARCHAR should fail
    ok(
        "PARTITION BY VARCHAR not supported",
        fails(
            f"CREATE TABLE {SCHEMA}.verify_part_varchar (id INT, name VARCHAR(100), PARTITION BY name)"
        ),
    )
except Exception as e:
    err("PARTITION BY VARCHAR not supported", e)

try:
    # PARTITION BY CHAR should fail
    ok(
        "PARTITION BY CHAR not supported",
        fails(
            f"CREATE TABLE {SCHEMA}.verify_part_char (id INT, code CHAR(3), PARTITION BY code)"
        ),
    )
except Exception as e:
    err("PARTITION BY CHAR not supported", e)

try:
    # ALTER TABLE DISTRIBUTE BY; removes distribution
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_alter_dist (id INT, cid INT, DISTRIBUTE BY cid)"
    )
    conn.execute(f"ALTER TABLE {SCHEMA}.verify_alter_dist DISTRIBUTE BY")
    any_dist = q1(f"""
        SELECT COUNT(*) FROM EXA_ALL_COLUMNS
        WHERE COLUMN_TABLE='VERIFY_ALTER_DIST' AND COLUMN_SCHEMA='{SCHEMA}'
          AND COLUMN_IS_DISTRIBUTION_KEY=TRUE
    """)
    ok(
        "ALTER TABLE DISTRIBUTE BY (no col) removes distribution key",
        any_dist == 0,
        f"got {any_dist}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_alter_dist")
except Exception as e:
    err("ALTER TABLE DISTRIBUTE BY removes distribution", e)

try:
    # ALTER TABLE PARTITION BY; removes partitioning
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_alter_part (id INT, d DATE, PARTITION BY d)"
    )
    conn.execute(f"ALTER TABLE {SCHEMA}.verify_alter_part PARTITION BY")
    ok("ALTER TABLE PARTITION BY (no col) is valid syntax", True)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_alter_part")
except Exception as e:
    err("ALTER TABLE PARTITION BY removes partitioning", e)

try:
    # iproc() function
    v = q1("SELECT iproc()")
    ok("iproc() function exists", v is not None, f"value={v}")
except Exception as e:
    err("iproc() function exists", e)

try:
    # value2proc() function
    v = q1("SELECT value2proc(42)")
    ok("value2proc() function exists", v is not None, f"value={v}")
except Exception as e:
    err("value2proc() function exists", e)

try:
    # IDENTITY column
    conn.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA}.verify_identity (
            id DECIMAL(18,0) IDENTITY PRIMARY KEY,
            name VARCHAR(50)
        )
    """)
    conn.execute(f"INSERT INTO {SCHEMA}.verify_identity (name) VALUES ('a')")
    conn.execute(f"INSERT INTO {SCHEMA}.verify_identity (name) VALUES ('b')")
    rows = qall(f"SELECT id, name FROM {SCHEMA}.verify_identity ORDER BY id")
    ok(
        "IDENTITY column auto-increments",
        len(rows) == 2 and rows[0][0] is not None and rows[1][0] > rows[0][0],
        f"got {rows}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_identity")
except Exception as e:
    err("IDENTITY column", e)

try:
    # PARTITION BY DECIMAL (supported)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_part_dec (id INT, amount DECIMAL(10,2), PARTITION BY amount)"
    )
    ok("PARTITION BY DECIMAL supported", True)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_part_dec")
except Exception as e:
    err("PARTITION BY DECIMAL supported", e)

try:
    # PARTITION BY BOOLEAN (supported per skill)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_part_bool (id INT, flag BOOLEAN, PARTITION BY flag)"
    )
    ok("PARTITION BY BOOLEAN supported", True)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_part_bool")
except Exception as e:
    err("PARTITION BY BOOLEAN supported", e)

try:
    # PARTITION BY TIMESTAMP (supported)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_part_ts (id INT, ts TIMESTAMP, PARTITION BY ts)"
    )
    ok("PARTITION BY TIMESTAMP supported", True)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_part_ts")
except Exception as e:
    err("PARTITION BY TIMESTAMP supported", e)

try:
    # PARTITION BY HASHTYPE (supported per skill)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_part_hash (id INT, h HASHTYPE(16 BYTE), PARTITION BY h)"
    )
    ok("PARTITION BY HASHTYPE supported", True)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_part_hash")
except Exception as e:
    err("PARTITION BY HASHTYPE supported", e)

try:
    # Only one partition column allowed
    ok(
        "Only one PARTITION BY column (multiple should fail)",
        fails(
            f"CREATE TABLE {SCHEMA}.verify_multi_part (id INT, d DATE, n INT, PARTITION BY d, n)"
        ),
    )
except Exception as e:
    err("Single PARTITION BY column only", e)

try:
    # REPLICATION_BORDER exists as a system parameter
    v = q1(
        "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME = 'REPLICATION_BORDER'"
    )
    ok(
        "REPLICATION_BORDER system parameter exists",
        v is not None,
        f"current value={v}",
    )
except Exception as e:
    err("REPLICATION_BORDER system parameter", e)

# ─────────────────────────────────────────────
print("\n=== UDFs / SCRIPTS ===")
# ─────────────────────────────────────────────

try:
    # Create Python3 SCALAR script
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_upper_words(text VARCHAR(2000))
        RETURNS VARCHAR(2000) AS

def run(ctx):
    return ctx.text.upper() if ctx.text else None
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_upper_words('hello world')")
    ok(
        "PYTHON3 SCALAR script create and call",
        result == "HELLO WORLD",
        f"got {result!r}",
    )
except Exception as e:
    err("PYTHON3 SCALAR script", e)

try:
    # SET script with EMITS (tokenizer)
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SET SCRIPT {SCHEMA}.verify_tokenize(text VARCHAR(2000))
        EMITS (token VARCHAR(200)) AS

def run(ctx):
    while True:
        if ctx.text:
            for word in ctx.text.split():
                ctx.emit(word)
        if not ctx.next():
            break
/
    """)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_tok_src (id INT, text VARCHAR(500))"
    )
    conn.execute(f"INSERT INTO {SCHEMA}.verify_tok_src VALUES (1, 'foo bar baz')")
    tokens = [
        r[0]
        for r in qall(
            f"SELECT {SCHEMA}.verify_tokenize(text) FROM {SCHEMA}.verify_tok_src GROUP BY id"
        )
    ]
    ok(
        "PYTHON3 SET EMITS script",
        sorted(tokens) == ["bar", "baz", "foo"],
        f"got {tokens}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_tok_src")
except Exception as e:
    err("PYTHON3 SET EMITS script", e)

try:
    # ctx.size() and ctx.reset() in SET script
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SET SCRIPT {SCHEMA}.verify_ctx_size(v INT)
        EMITS (sz INT, total INT) AS

def run(ctx):
    sz = ctx.size()
    total = 0
    while True:
        total += ctx.v if ctx.v else 0
        if not ctx.next():
            break
    ctx.emit(sz, total)
/
    """)
    conn.execute(f"CREATE OR REPLACE TABLE {SCHEMA}.verify_ctx_src (grp INT, v INT)")
    conn.execute(
        f"INSERT INTO {SCHEMA}.verify_ctx_src VALUES (1, 10), (1, 20), (1, 30)"
    )
    rows = qall(
        f"SELECT {SCHEMA}.verify_ctx_size(v) FROM {SCHEMA}.verify_ctx_src GROUP BY grp"
    )
    sz, total = rows[0]
    ok("ctx.size() returns group size", sz == 3, f"got size={sz}")
    ok("ctx.next() iterates all rows (total sum)", total == 60, f"got total={total}")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_ctx_src")
except Exception as e:
    err("ctx.size() and ctx.next()", e)

try:
    # EXA_ALL_SCRIPTS system table
    row = q1(
        f"SELECT SCRIPT_NAME FROM EXA_ALL_SCRIPTS WHERE SCRIPT_SCHEMA='{SCHEMA}' AND SCRIPT_NAME='VERIFY_UPPER_WORDS'"
    )
    ok(
        "EXA_ALL_SCRIPTS system table has script",
        row == "VERIFY_UPPER_WORDS",
        f"got {row!r}",
    )
except Exception as e:
    err("EXA_ALL_SCRIPTS system table", e)

try:
    # Script source in EXA_ALL_SCRIPTS.SCRIPT_TEXT
    src = q1(
        f"SELECT SCRIPT_TEXT FROM EXA_ALL_SCRIPTS WHERE SCRIPT_SCHEMA='{SCHEMA}' AND SCRIPT_NAME='VERIFY_UPPER_WORDS'"
    )
    ok(
        "EXA_ALL_SCRIPTS.SCRIPT_TEXT has source code",
        src is not None and "def run" in src,
        f"got {src!r}",
    )
except Exception as e:
    err("EXA_ALL_SCRIPTS.SCRIPT_TEXT", e)

try:
    # AGGREGATE script type
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 AGGREGATE SCRIPT {SCHEMA}.verify_mysum(v DOUBLE)
        RETURNS DOUBLE AS

def run(ctx):
    total = 0.0
    while True:
        if ctx.v is not None:
            total += ctx.v
        if not ctx.next():
            break
    return total
/
    """)
    result = q1(
        f"SELECT {SCHEMA}.verify_mysum(x) FROM (SELECT 1.0 AS x UNION ALL SELECT 2.0 AS x UNION ALL SELECT 3.0 AS x)"
    )
    ok("AGGREGATE script type supported", float(result) == 6.0, f"got {result}")
except Exception as e:
    err("AGGREGATE script type", e)

try:
    # LUA script
    conn.execute(f"""
        CREATE OR REPLACE LUA SCALAR SCRIPT {SCHEMA}.verify_lua_add(a DOUBLE, b DOUBLE)
        RETURNS DOUBLE AS

function run(ctx)
    return ctx.a + ctx.b
end
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_lua_add(3, 4)")
    ok("LUA SCALAR script", float(result) == 7.0, f"got {result}")
except Exception as e:
    err("LUA SCALAR script", e)

# Java script - syntax check only (class name must match script name)
try:
    conn.execute(f"""
        CREATE OR REPLACE JAVA SCALAR SCRIPT {SCHEMA}.verify_java_upper(text VARCHAR(200))
        RETURNS VARCHAR(200) AS

class VERIFY_JAVA_UPPER {{
    static String run(ExaMetadata meta, ExaIterator ctx) throws Exception {{
        String val = ctx.getString("text");
        return val != null ? val.toUpperCase() : null;
    }}
}}
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_java_upper('hello')")
    ok(
        "JAVA SCALAR script (class name matches script name)",
        result == "HELLO",
        f"got {result!r}",
    )
except Exception as e:
    err("JAVA SCALAR script", e)

try:
    # Python DECIMAL(p,0) maps to int
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_type_decimal(v DECIMAL(10,0))
        RETURNS VARCHAR(100) AS

def run(ctx):
    return type(ctx.v).__name__
/
    """)
    t = q1(f"SELECT {SCHEMA}.verify_type_decimal(42)")
    ok("DECIMAL(p,0) maps to Python int", t == "int", f"got {t!r}")
except Exception as e:
    err("DECIMAL(p,0) maps to Python int", e)

try:
    # DECIMAL(p,s) with s>0 maps to Decimal
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_type_decimal_s(v DECIMAL(10,2))
        RETURNS VARCHAR(100) AS

def run(ctx):
    return type(ctx.v).__name__
/
    """)
    t = q1(f"SELECT {SCHEMA}.verify_type_decimal_s(3.14)")
    ok("DECIMAL(p,s) s>0 maps to Python Decimal", t == "Decimal", f"got {t!r}")
except Exception as e:
    err("DECIMAL(p,s) s>0 maps to Python Decimal", e)

try:
    # DOUBLE maps to float in Python
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_type_double(v DOUBLE)
        RETURNS VARCHAR(100) AS

def run(ctx):
    return type(ctx.v).__name__
/
    """)
    t = q1(f"SELECT {SCHEMA}.verify_type_double(3.14)")
    ok("DOUBLE maps to Python float", t == "float", f"got {t!r}")
except Exception as e:
    err("DOUBLE maps to Python float", e)

try:
    # DATE maps to datetime.date in Python
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_type_date(v DATE)
        RETURNS VARCHAR(100) AS

def run(ctx):
    return type(ctx.v).__name__
/
    """)
    t = q1(f"SELECT {SCHEMA}.verify_type_date(TO_DATE('2024-01-01', 'YYYY-MM-DD'))")
    ok("DATE maps to Python datetime.date", t == "date", f"got {t!r}")
except Exception as e:
    err("DATE maps to Python datetime.date", e)

try:
    # TIMESTAMP maps to datetime.datetime in Python
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_type_timestamp(v TIMESTAMP)
        RETURNS VARCHAR(100) AS

def run(ctx):
    return type(ctx.v).__name__
/
    """)
    t = q1(
        f"SELECT {SCHEMA}.verify_type_timestamp(TO_TIMESTAMP('2024-01-01 10:00:00', 'YYYY-MM-DD HH24:MI:SS'))"
    )
    ok("TIMESTAMP maps to Python datetime.datetime", t == "datetime", f"got {t!r}")
except Exception as e:
    err("TIMESTAMP maps to Python datetime.datetime", e)

try:
    # BOOLEAN maps to bool in Python
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_type_bool(v BOOLEAN)
        RETURNS VARCHAR(100) AS

def run(ctx):
    return type(ctx.v).__name__
/
    """)
    t = q1(f"SELECT {SCHEMA}.verify_type_bool(TRUE)")
    ok("BOOLEAN maps to Python bool", t == "bool", f"got {t!r}")
except Exception as e:
    err("BOOLEAN maps to Python bool", e)

try:
    # VARCHAR maps to str in Python
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_type_varchar(v VARCHAR(100))
        RETURNS VARCHAR(100) AS

def run(ctx):
    return type(ctx.v).__name__
/
    """)
    t = q1(f"SELECT {SCHEMA}.verify_type_varchar('hello')")
    ok("VARCHAR maps to Python str", t == "str", f"got {t!r}")
except Exception as e:
    err("VARCHAR maps to Python str", e)

try:
    # NULL maps to None in Python
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_null_python(v VARCHAR(100))
        RETURNS VARCHAR(100) AS

def run(ctx):
    return 'none' if ctx.v is None else 'not_none'
/
    """)
    t = q1(f"SELECT {SCHEMA}.verify_null_python(NULL)")
    ok("SQL NULL maps to Python None", t == "none", f"got {t!r}")
except Exception as e:
    err("SQL NULL maps to Python None", e)

try:
    # exa.import_script works
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_helper()
        RETURNS INT AS

HELPER_VALUE = 99
/
    """)
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_import_script()
        RETURNS INT AS

import exa
exa.import_script('{SCHEMA}.verify_helper')

def run(ctx):
    return HELPER_VALUE
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_import_script()")
    ok("exa.import_script works", result == 99, f"got {result}")
except Exception as e:
    err("exa.import_script", e)

try:
    # Variadic script: ctx[i] 0-based access
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_variadic_idx(...)
        RETURNS VARCHAR(100) AS
def run(ctx):
    return ctx[0]
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_variadic_idx('first', 'second')")
    ok(
        "Variadic script: ctx[i] 0-based — ctx[0] is first arg",
        result == "first",
        f"got {result!r}",
    )
except Exception as e:
    err("Variadic script ctx[i] 0-based", e)

try:
    # exa.meta.input_column_count
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_meta_col_count(...)
        RETURNS INT AS
def run(ctx):
    return exa.meta.input_column_count
/
    """)
    count = q1(f"SELECT {SCHEMA}.verify_meta_col_count(1, 2, 3)")
    ok("exa.meta.input_column_count in variadic script", count == 3, f"got {count}")
except Exception as e:
    err("exa.meta.input_column_count", e)

try:
    # exa.meta.input_columns[i].name (stored uppercase)
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_meta_col_name(mycol VARCHAR(100))
        RETURNS VARCHAR(100) AS
def run(ctx):
    return exa.meta.input_columns[0].name
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_meta_col_name('x')")
    ok(
        "exa.meta.input_columns[i].name (lowercase)",
        result == "mycol",
        f"got {result!r}",
    )
except Exception as e:
    err("exa.meta.input_columns[i].name", e)

try:
    # exa.meta.input_columns[i].sql_type
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_meta_col_type(v DOUBLE)
        RETURNS VARCHAR(100) AS
def run(ctx):
    return exa.meta.input_columns[0].sql_type
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_meta_col_type(3.14)")
    ok(
        "exa.meta.input_columns[i].sql_type",
        result is not None and "DOUBLE" in result.upper(),
        f"got {result!r}",
    )
except Exception as e:
    err("exa.meta.input_columns[i].sql_type", e)

try:
    # exa.meta.script_name
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_meta_script_name()
        RETURNS VARCHAR(200) AS
def run(ctx):
    return exa.meta.script_name
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_meta_script_name()")
    ok(
        "exa.meta.script_name contains script name",
        result is not None and "VERIFY_META_SCRIPT_NAME" in result.upper(),
        f"got {result!r}",
    )
except Exception as e:
    err("exa.meta.script_name", e)

try:
    # exa.meta.session_id
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SCALAR SCRIPT {SCHEMA}.verify_meta_session_id()
        RETURNS VARCHAR(50) AS
def run(ctx):
    return str(exa.meta.session_id)
/
    """)
    result = q1(f"SELECT {SCHEMA}.verify_meta_session_id()")
    ok(
        "exa.meta.session_id accessible",
        result is not None and result.isdigit(),
        f"got {result}",
    )
except Exception as e:
    err("exa.meta.session_id", e)

try:
    # Variadic EMITS(...) with EMITS clause in SELECT
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SET SCRIPT {SCHEMA}.verify_variadic_emits(...)
        EMITS (...) AS
def run(ctx):
    while True:
        ctx.emit(ctx[0])
        if not ctx.next():
            break
/
    """)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_ve_src (grp INT, val VARCHAR(10))"
    )
    conn.execute(f"INSERT INTO {SCHEMA}.verify_ve_src VALUES (1, 'a'), (1, 'b')")
    rows = qall(
        f"SELECT {SCHEMA}.verify_variadic_emits(val) EMITS (outval VARCHAR(10)) FROM {SCHEMA}.verify_ve_src GROUP BY grp ORDER BY outval"
    )
    ok(
        "Variadic EMITS(...) with EMITS in SELECT",
        [r[0] for r in rows] == ["a", "b"],
        f"got {rows}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_ve_src")
except Exception as e:
    err("Variadic EMITS(...) with EMITS in SELECT", e)

try:
    # default_output_columns() derives schema without EMITS in SELECT
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SET SCRIPT {SCHEMA}.verify_default_out(...)
        EMITS (...) AS
def default_output_columns():
    return ', '.join(
        'col' + str(i) + ' VARCHAR(100)'
        for i in range(exa.meta.input_column_count)
    )

def run(ctx):
    while True:
        ctx.emit(*[ctx[i] for i in range(exa.meta.input_column_count)])
        if not ctx.next():
            break
/
    """)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_do_src (grp INT, a VARCHAR(10), b VARCHAR(10))"
    )
    conn.execute(f"INSERT INTO {SCHEMA}.verify_do_src VALUES (1, 'x', 'y')")
    rows = qall(
        f"SELECT {SCHEMA}.verify_default_out(a, b) FROM {SCHEMA}.verify_do_src GROUP BY grp"
    )
    ok(
        "default_output_columns() derives 2-column output schema",
        len(rows) == 1 and len(rows[0]) == 2,
        f"got {rows}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_do_src")
except Exception as e:
    err("default_output_columns()", e)

try:
    # ctx.get_dataframe() and ctx.emit(dataframe)
    conn.execute(f"""
        CREATE OR REPLACE PYTHON3 SET SCRIPT {SCHEMA}.verify_df_emit(v DOUBLE)
        EMITS (v DOUBLE) AS
def run(ctx):
    df = ctx.get_dataframe(1000)
    while df is not None:
        ctx.emit(df)
        df = ctx.get_dataframe(1000)
/
    """)
    conn.execute(f"CREATE OR REPLACE TABLE {SCHEMA}.verify_df_src (grp INT, v DOUBLE)")
    conn.execute(
        f"INSERT INTO {SCHEMA}.verify_df_src VALUES (1, 1.0), (1, 2.0), (1, 3.0)"
    )
    result = sorted(
        r[0]
        for r in qall(
            f"SELECT {SCHEMA}.verify_df_emit(v) FROM {SCHEMA}.verify_df_src GROUP BY grp"
        )
    )
    ok(
        "ctx.get_dataframe() and ctx.emit(dataframe) round-trip",
        result == [1.0, 2.0, 3.0],
        f"got {result}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_df_src")
except Exception as e:
    err("ctx.get_dataframe() and ctx.emit(dataframe)", e)

# Clean up UDF scripts
try:
    scripts = [
        "verify_upper_words",
        "verify_tokenize",
        "verify_ctx_size",
        "verify_mysum",
        "verify_lua_add",
        "verify_java_upper",
        "verify_type_decimal",
        "verify_type_decimal_s",
        "verify_type_double",
        "verify_type_date",
        "verify_type_timestamp",
        "verify_type_bool",
        "verify_type_varchar",
        "verify_null_python",
        "verify_helper",
        "verify_import_script",
        "verify_variadic_idx",
        "verify_meta_col_count",
        "verify_meta_col_name",
        "verify_meta_col_type",
        "verify_meta_script_name",
        "verify_meta_session_id",
        "verify_variadic_emits",
        "verify_default_out",
        "verify_df_emit",
    ]
    for s in scripts:
        try:
            conn.execute(f"DROP SCRIPT {SCHEMA}.{s}")
        except Exception:
            pass
except Exception:
    pass

# ─────────────────────────────────────────────
print("\n=== IMPORT / EXPORT ===")
# ─────────────────────────────────────────────

try:
    # EXA_DBA_CONNECTIONS view exists (may fail if not DBA, but view should exist)
    try:
        conn.execute("SELECT * FROM EXA_DBA_CONNECTIONS LIMIT 0")
        ok("EXA_DBA_CONNECTIONS view exists", True)
    except Exception as e2:
        # If permission denied, it still exists — just check differently
        if "not found" in str(e2).lower() or "does not exist" in str(e2).lower():
            ok("EXA_DBA_CONNECTIONS view exists", False, str(e2))
        else:
            ok(
                "EXA_DBA_CONNECTIONS view exists (permission check)",
                True,
                f"permission error expected: {e2}",
            )
except Exception as e:
    err("EXA_DBA_CONNECTIONS view", e)

try:
    # CREATE CONNECTION syntax works
    conn.execute(f"""
        CREATE OR REPLACE CONNECTION verify_test_conn
        TO 'https://example.com'
        USER '' IDENTIFIED BY 'testkey=abc'
    """)
    ok("CREATE OR REPLACE CONNECTION syntax works", True)
    conn.execute("DROP CONNECTION verify_test_conn")
    ok("DROP CONNECTION works", True)
except Exception as e:
    err("CREATE/DROP CONNECTION", e)

try:
    # LIKE INCLUDING DEFAULTS for staging pattern
    conn.execute(f"""
        CREATE OR REPLACE TABLE {SCHEMA}.verify_prod (
            order_id DECIMAL(18,0) PRIMARY KEY,
            status VARCHAR(20) DEFAULT 'pending',
            amount DECIMAL(10,2)
        )
    """)
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_stg (LIKE {SCHEMA}.verify_prod INCLUDING DEFAULTS)"
    )
    # Check the staging table has the status default but NOT the primary key
    default_val = q1(f"""
        SELECT COLUMN_DEFAULT FROM EXA_ALL_COLUMNS
        WHERE COLUMN_TABLE='VERIFY_STG' AND COLUMN_SCHEMA='{SCHEMA}' AND COLUMN_NAME='STATUS'
    """)
    ok(
        "LIKE INCLUDING DEFAULTS for staging: default copied",
        default_val is not None and "pending" in str(default_val),
        f"got {default_val!r}",
    )
    conn.execute(f"DROP TABLE {SCHEMA}.verify_stg")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_prod")
except Exception as e:
    err("LIKE INCLUDING DEFAULTS staging pattern", e)

try:
    # TRUNCATE TABLE works
    conn.execute(f"CREATE OR REPLACE TABLE {SCHEMA}.verify_trunc (id INT)")
    conn.execute(f"INSERT INTO {SCHEMA}.verify_trunc VALUES (1), (2)")
    conn.execute(f"TRUNCATE TABLE {SCHEMA}.verify_trunc")
    cnt = q1(f"SELECT COUNT(*) FROM {SCHEMA}.verify_trunc")
    ok("TRUNCATE TABLE works", cnt == 0, f"got {cnt}")
    conn.execute(f"DROP TABLE {SCHEMA}.verify_trunc")
except Exception as e:
    err("TRUNCATE TABLE", e)

try:
    # Constraint violation (NOT NULL) fails immediately without REJECT LIMIT consideration
    conn.execute(
        f"CREATE OR REPLACE TABLE {SCHEMA}.verify_notnull (id INT, val VARCHAR(10) NOT NULL)"
    )
    not_null_fails = fails(f"INSERT INTO {SCHEMA}.verify_notnull VALUES (1, NULL)")
    ok("NOT NULL constraint violation fails immediately", not_null_fails)
    conn.execute(f"DROP TABLE {SCHEMA}.verify_notnull")
except Exception as e:
    err("NOT NULL constraint violation", e)

# ─────────────────────────────────────────────
print("\n=== VIRTUAL SCHEMAS ===")
# ─────────────────────────────────────────────

try:
    ok(
        "EXA_ALL_VIRTUAL_SCHEMAS view exists",
        q1("SELECT COUNT(*) FROM EXA_ALL_VIRTUAL_SCHEMAS") is not None,
    )
except Exception as e:
    err("EXA_ALL_VIRTUAL_SCHEMAS view", e)

try:
    # EXA_ALL_TABLES for virtual schemas
    conn.execute(
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM EXA_ALL_TABLES WHERE TABLE_SCHEMA = 'NONEXISTENT_VS' LIMIT 0"
    )
    ok("EXA_ALL_TABLES queryable for virtual schema lookup", True)
except Exception as e:
    err("EXA_ALL_TABLES", e)

try:
    # EXA_ALL_COLUMNS for virtual schemas
    conn.execute(
        "SELECT COLUMN_SCHEMA, COLUMN_NAME FROM EXA_ALL_COLUMNS WHERE COLUMN_SCHEMA = 'NONEXISTENT_VS' LIMIT 0"
    )
    ok("EXA_ALL_COLUMNS queryable for virtual schema lookup", True)
except Exception as e:
    err("EXA_ALL_COLUMNS", e)

# ─────────────────────────────────────────────
print("\n=== SUMMARY ===")
# ─────────────────────────────────────────────

passed = sum(1 for r in results if r[0] == "PASS")
failed = sum(1 for r in results if r[0] == "FAIL")
errors = sum(1 for r in results if r[0] == "ERROR")
total = len(results)
print(f"\n{passed}/{total} PASS  |  {failed} FAIL  |  {errors} ERROR\n")

if failed > 0 or errors > 0:
    print("FAILURES / ERRORS:")
    for status, name, note in results:
        if status != "PASS":
            print(f"  {status}: {name}  -> {note}")
