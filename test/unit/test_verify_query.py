from textwrap import dedent

import pytest

from exasol.ai.mcp.server.mcp_server import verify_query


def sample_select_query() -> str:

    return dedent(
        """
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
    """
    )


def sample_insert_query() -> str:

    return dedent(
        f"""
        INSERT INTO "NLP"."TOPIC"(
            "DOC_ID",
            "TOPIC_NAME",
            "ERROR_MESSAGE",
            "SETUP"
        )
        {sample_select_query()}
    """
    )


def sample_merge_query() -> str:
    return dedent(
        f"""
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
    """
    )


def sample_create_table_query() -> str:

    return dedent(
        f"""
        CREATE OR REPLACE TABLE "NLP"."TEMP_TOPIC" AS
        {sample_select_query()}
    """
    )


def sample_export_query() -> str:
    return dedent(
        f"""
        EXPORT (
            {sample_select_query()}
        )
        INTO CSV
        AT 'https://testbucket.s3.amazonaws.com'
        USER 'my-ID' IDENTIFIED BY 'my-secret-key;sse_type=AES256'
        FILE 'testpath/my_topics.csv';
    """
    )


def sample_select_into_query() -> str:

    return dedent(
        f"""
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
    """
    )


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
        (sample_invalid_query(), False),
    ],
    ids=[
        "select",
        "select-into",
        "insert",
        "merge",
        "create-table",
        "export",
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
