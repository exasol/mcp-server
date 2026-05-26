from test.utils.text_utils import collapse_spaces
from textwrap import dedent
from unittest.mock import MagicMock

import pytest
import sqlglot.expressions as exp

from exasol.ai.mcp.server.tools.mcp_server import (
    ExasolMCPServer,
    _build_column_summaries,
    _build_preview_query,
    _build_profile_select,
    _build_stats_query,
    _build_top_values_query,
    _is_numeric_type,
    remove_info_column,
    verify_query,
)
from exasol.ai.mcp.server.tools.meta_query import INFO_COLUMN
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    DBColumn,
    DBObject,
)


def sample_select_query() -> str:

    return dedent("""
        WITH T2 AS (
            SELECT "DOC_ID"
            FROM "NLP"."TOPIC" T3
            WHERE T3."SETUP"='{TOPICS=["Select", "Insert", "Update", "Delete"]}'
        )
        WITH T1 AS (
            SELECT
                ROWID AS "ROWID",
                "DOC_ID",
                "TEXT"
            FROM NLP."DOCUMENTS"
        )
        SELECT "NLP"."TOPIC_CLASSIFIER_UDF"(
            T1."DOC_ID",
            T1."TEXT"
        )
        FROM T1
        LEFT OUTER JOIN T2 ON
            T1."DOC_ID"=T2."DOC_ID"
        WHERE
            T2."DOC_ID" IS NULL
        GROUP BY IPROC(), MOD(T1."ROWID", 2)
    """)


def sample_insert_query() -> str:

    return dedent(f"""
        INSERT INTO "NLP"."TOPIC"(
            "DOC_ID",
            "TOPIC_NAME",
            "ERROR_MESSAGE",
            "SETUP"
        )
        {sample_select_query()}
    """)


def sample_merge_query() -> str:
    return dedent(f"""
        MERGE INTO "NLP"."TEMP_TOPIC" T
        USING
        {sample_select_query()}
        AS U ON T."DOC_ID" = U."DOC_ID"
        WHEN MATCHED THEN
            UPDATE SET
                T."TOPIC_NAME" = U."TOPIC_NAME",
                T."SETUP" = U."SETUP"
            WHERE U."ERROR_MESSAGE" IS NULL
        WHEN NOT MATCHED THEN
            INSERT VALUES (
                U."DOC_ID",
                U."TOPIC_NAME",
                U."ERROR_MESSAGE",
                U."SETUP"
            )
    """)


def sample_create_table_query() -> str:

    return dedent(f"""
        CREATE OR REPLACE TABLE "NLP"."TEMP_TOPIC" AS
        {sample_select_query()}
    """)


def sample_export_query() -> str:
    return dedent(f"""
        EXPORT (
            {sample_select_query()}
        )
        INTO CSV
        AT 'https://testbucket.s3.amazonaws.com'
        USER 'my-ID' IDENTIFIED BY 'my-secret-key;sse_type=AES256'
        FILE 'testpath/my_topics.csv';
    """)


def sample_select_into_query() -> str:

    return dedent("""
        SELECT
            T1."DOC_ID",
            T2."TOPIC_NAME",
            T1."ERROR_MESSAGE",
            T3."SETUP"
        INTO TABLE "NLP"."TOPIC_DENORM"
        FROM "NLP"."TOPIC" T1
        LEFT OUTER JOIN "NLP"."TOPIC_LOOKUP" T2
        ON T1."TOPIC_NAME" = T2."ID"
        LEFT OUTER JOIN "NLP"."SETUP_LOOKUP" T3
        ON T1."SETUP" = T3."ID"
    """)


def sample_select_udf_emits_query() -> str:
    return dedent("""
        SELECT "MyUDF"("input1", "input2", 1000, 'xyz')
        EMITS (dbl_value DOUBLE, "text_value" VARCHAR(200))
        FROM "MyTable"
        WHERE "SomeKey"='Y'
    """)


def sample_invalid_query() -> str:
    return "FOR cnt := 1 TO max_cnt SELECT cnt"


@pytest.mark.parametrize(
    ["query", "expected_result"],
    [
        (sample_select_query(), True),
        (sample_select_into_query(), False),
        (sample_insert_query(), False),
        (sample_merge_query(), False),
        (sample_create_table_query(), False),
        (sample_export_query(), False),
        (sample_select_udf_emits_query(), True),
        (sample_invalid_query(), False),
    ],
    ids=[
        "select",
        "select-into",
        "insert",
        "merge",
        "create-table",
        "export",
        "select-udf-emits",
        "invalid",
    ],
)
def test_verify_query(query, expected_result):
    """
    The test checks that the query validation recognises as a SELECT statement
    only a query that selects data. There are various forms of valid SQL statements
    that include a subquery. Execution of such statements should not be allowed.

    Currently, the SQLGlot doesn't parse some of the valid queries with specific
    Exasol dialect, for instance MERGE and EXPORT. Frustrating as it is, what matters
    in this case is that such queries are not recognised as valid SQL statements.
    """
    assert verify_query(query) == expected_result


def test_remove_info_column():
    input_data = [
        {"name": "db_object1", "comment": "this is my first db object"},
        {
            "name": "db_object2",
            "comment": "this is my second db object",
            INFO_COLUMN: "this column should be removed",
        },
    ]
    output_data = remove_info_column(input_data)
    expected_output_data = [
        {"name": "db_object1", "comment": "this is my first db object"},
        {"name": "db_object2", "comment": "this is my second db object"},
    ]
    assert output_data == expected_output_data


def test_execute_meta_query_empty_result():
    connection = MagicMock()
    connection.execute_query.return_value.fetchall.return_value = []
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)
    result = server._execute_meta_query("SELECT 1", DBObject)
    assert result == []


@pytest.mark.parametrize(
    ["sql_type", "expected"],
    [
        ("DECIMAL(18,0)", True),
        ("NUMERIC(10,2)", True),
        ("DOUBLE", True),
        ("DOUBLE PRECISION", True),
        ("FLOAT", True),
        ("INTEGER", True),
        ("INT", True),
        ("BIGINT", True),
        ("SMALLINT", True),
        ("TINYINT", True),
        ("NUMBER", True),
        ("VARCHAR(100) UTF8", False),
        ("CHAR(10)", False),
        ("DATE", False),
        ("TIMESTAMP", False),
        ("BOOLEAN", False),
    ],
)
def test_is_numeric_type(sql_type, expected):
    assert _is_numeric_type(sql_type) == expected


def _make_table_ref() -> exp.Table:
    return exp.Table(
        this=exp.Identifier(this="my_table", quoted=True),
        db=exp.Identifier(this="my_schema", quoted=True),
    )


def test_build_stats_query_mixed_columns():
    columns = [
        DBColumn(name="id", type="DECIMAL(18,0)", comment=None),
        DBColumn(name="label", type="VARCHAR(100) UTF8", comment=None),
    ]
    query = collapse_spaces(_build_stats_query(_make_table_ref(), columns))
    expected_query = collapse_spaces("""
        SELECT
            COUNT(*) AS ROW_COUNT,
            COUNT(DISTINCT "id") AS DISTINCT_0,
            MIN("id") AS MIN_0,
            MAX("id") AS MAX_0,
            COUNT(*) - COUNT("id") AS NULL_COUNT_0,
            COUNT(DISTINCT "label") AS DISTINCT_1,
            COUNT(*) - COUNT("label") AS NULL_COUNT_1
        FROM "my_schema"."my_table"
    """)
    assert query == expected_query


def test_build_stats_query_all_non_numeric():
    columns = [
        DBColumn(name="a", type="VARCHAR(10)", comment=None),
        DBColumn(name="b", type="DATE", comment=None),
    ]
    query = collapse_spaces(_build_stats_query(_make_table_ref(), columns))
    expected_query = collapse_spaces("""
        SELECT
            COUNT(*) AS ROW_COUNT,
            COUNT(DISTINCT "a") AS DISTINCT_0,
            COUNT(*) - COUNT("a") AS NULL_COUNT_0,
            COUNT(DISTINCT "b") AS DISTINCT_1,
            COUNT(*) - COUNT("b") AS NULL_COUNT_1
        FROM "my_schema"."my_table"
    """)
    assert query == expected_query


def test_build_top_values_query():
    col = DBColumn(name="country", type="VARCHAR(100) UTF8", comment=None)
    query = collapse_spaces(_build_top_values_query(_make_table_ref(), col, 5))
    expected_query = collapse_spaces("""
        SELECT "country"
        FROM "my_schema"."my_table"
        WHERE NOT "country" IS NULL
        GROUP BY "country"
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    assert query == expected_query


def test_build_column_summaries_with_data():
    columns = [
        DBColumn(name="id", type="DECIMAL(18,0)", comment=None),
        DBColumn(name="label", type="VARCHAR(100) UTF8", comment="a label"),
    ]
    stats_row = {
        "ROW_COUNT": 10,
        "DISTINCT_0": 5,
        "MIN_0": 1,
        "MAX_0": 10,
        "NULL_COUNT_0": 0,
        "DISTINCT_1": 3,
        "NULL_COUNT_1": 2,
    }
    top_values = [[1, 2, 3], ["x", "y", "z"]]

    summaries = _build_column_summaries(columns, stats_row, top_values)

    assert len(summaries) == 2
    assert summaries[0].name == "id"
    assert summaries[0].distinct_count == 5
    assert summaries[0].min == "1"
    assert summaries[0].max == "10"
    assert summaries[0].top_values == [1, 2, 3]
    assert summaries[0].has_nulls is False
    assert summaries[0].null_percentage == 0
    assert summaries[1].name == "label"
    assert summaries[1].comment == "a label"
    assert summaries[1].distinct_count == 3
    assert summaries[1].min is None
    assert summaries[1].max is None
    assert summaries[1].top_values == ["x", "y", "z"]
    assert summaries[1].has_nulls is True
    assert summaries[1].null_percentage == 20


def test_build_column_summaries_empty_table():
    columns = [DBColumn(name="id", type="DECIMAL(18,0)", comment=None)]
    stats_row = {
        "ROW_COUNT": 0,
        "DISTINCT_0": 0,
        "MIN_0": None,
        "MAX_0": None,
        "NULL_COUNT_0": 0,
    }

    summaries = _build_column_summaries(columns, stats_row, [[]])

    assert summaries[0].distinct_count == 0
    assert summaries[0].min is None
    assert summaries[0].max is None
    assert summaries[0].top_values == []
    assert summaries[0].has_nulls is False
    assert summaries[0].null_percentage == 0


def test_build_column_summaries_no_stats_row():
    columns = [DBColumn(name="id", type="DECIMAL(18,0)", comment=None)]

    summaries = _build_column_summaries(columns, None, [[]])

    assert summaries[0].distinct_count == 0
    assert summaries[0].min is None
    assert summaries[0].max is None
    assert summaries[0].top_values == []
    assert summaries[0].has_nulls is False
    assert summaries[0].null_percentage == 0


def test_build_preview_query():
    query = 'SELECT * FROM "MY_SCHEMA"."MY_TABLE"'
    sql = collapse_spaces(_build_preview_query(query, 10))
    expected = collapse_spaces(f"SELECT * FROM ({query}) LIMIT 10")
    assert sql == expected


def test_build_preview_query_preserves_inner_query():
    query = 'SELECT "A", "B" FROM "S"."T" WHERE "X" > 0 ORDER BY "A"'
    sql = collapse_spaces(_build_preview_query(query, 1))
    expected = collapse_spaces(f"SELECT * FROM ({query}) LIMIT 1")
    assert sql == expected


_PROFILE_TABLE_SQL = "EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY"
_PROFILE_COLS_SQL = (
    "PART_NAME, PART_INFO, OBJECT_SCHEMA, OBJECT_NAME, OBJECT_ROWS, DURATION, CPU"
)


def test_build_profile_select():
    sql = collapse_spaces(_build_profile_select("SELECT 1"))
    expected = collapse_spaces(f"""
        SELECT {_PROFILE_COLS_SQL}
        FROM {_PROFILE_TABLE_SQL}
        WHERE SESSION_ID = CURRENT_SESSION AND STMT_ID = (CURRENT_STATEMENT - 4)
        ORDER BY PART_ID
    """)
    assert sql == expected
