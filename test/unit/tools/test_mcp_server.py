from test.utils.text_utils import collapse_spaces
from textwrap import dedent
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
import sqlglot.expressions as exp

from exasol.ai.mcp.server.tools.mcp_server import (
    ExasolMCPServer,
    _build_column_summaries,
    _build_current_preprocessor_query,
    _build_list_preprocessors_query,
    _build_preview_query,
    _build_profile_select,
    _build_profile_status_query,
    _build_set_preprocessor_query,
    _build_stats_query,
    _build_top_values_query,
    _is_numeric_type,
    _statement_to_tabular,
    remove_info_column,
    verify_query,
)
from exasol.ai.mcp.server.tools.meta_query import INFO_COLUMN
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    DBColumn,
    DBEmitFunction,
    DBObject,
    DBReturnFunction,
    DBTable,
    QueryResult,
)


def _mock_connection() -> MagicMock:
    """
    A `MagicMock` standing in for `DbConnection`, wired so that `execute_query`
    applies its `fetch` argument to the mock statement (`execute_query.return_value`)
    the same way the real `DbConnection.execute_query` does.
    """
    connection = MagicMock()
    mock_statement = connection.execute_query.return_value

    def execute_query(*args, fetch=lambda statement: statement, **kwargs):
        return fetch(mock_statement)

    connection.execute_query.side_effect = execute_query
    return connection


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
    connection = _mock_connection()
    connection.execute_query.return_value.fetchall.return_value = []
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)
    result = server._execute_meta_query("SELECT 1", DBObject)
    assert result == []


def test_statement_to_tabular():
    statement = MagicMock()
    statement.column_names.return_value = ["ID", "NAME"]
    statement.__iter__.return_value = iter([(1, "Alice"), (2, "Bob")])
    result = _statement_to_tabular(statement)
    assert statement.fetch_dict is False
    assert result == QueryResult(
        columns=["ID", "NAME"], rows=[[1, "Alice"], [2, "Bob"]]
    )


def test_statement_to_tabular_empty_result_keeps_columns():
    """
    Columns must come from the cursor metadata, not from the first row, otherwise an
    empty result set would be indistinguishable from "no columns".
    """
    statement = MagicMock()
    statement.column_names.return_value = ["ID", "NAME"]
    statement.fetchall.return_value = []
    result = _statement_to_tabular(statement)
    assert result == QueryResult(columns=["ID", "NAME"], rows=[])


def test_statement_to_tabular_preserves_duplicate_column_names():
    """
    dict(zip(col_names, row)), used for the dict-format output, silently collapses
    same-named columns (e.g. a join on two tables that both have an ID column). The
    tabular path reads rows positionally, so it must preserve both values.
    """
    statement = MagicMock()
    statement.column_names.return_value = ["ID", "ID"]
    statement.__iter__.return_value = iter([(1, 2)])
    result = _statement_to_tabular(statement)
    assert result == QueryResult(columns=["ID", "ID"], rows=[[1, 2]])


def test_execute_query_tabular():
    connection = _mock_connection()
    connection.execute_query.return_value.column_names.return_value = ["ID"]
    connection.execute_query.return_value.__iter__.return_value = iter([(1,), (2,)])
    config = MagicMock()
    config.enable_read_query = True
    config.default_row_limit = None
    server = ExasolMCPServer(connection=connection, config=config)
    result = server.execute_query_tabular("SELECT ID FROM T")
    assert result == QueryResult(columns=["ID"], rows=[[1], [2]])


def test_profile_query_tabular():
    server, connection = _make_profile_server(profile_already_on=True)
    connection.execute_query.return_value.column_names.return_value = ["PART_NAME"]
    connection.execute_query.return_value.__iter__.return_value = iter([("step1",)])
    result = server.profile_query_tabular("SELECT 1")
    assert result == QueryResult(columns=["PART_NAME"], rows=[["step1"]])


def test_execute_query():
    connection = _mock_connection()
    connection.execute_query.return_value.fetchall.return_value = [{"ID": 1}]
    config = MagicMock()
    config.enable_read_query = True
    config.default_row_limit = None
    server = ExasolMCPServer(connection=connection, config=config)
    result = server.execute_query("SELECT ID FROM T")
    assert result == [{"ID": 1}]
    query = connection.execute_query.call_args[0][0]
    assert query == "SELECT ID FROM T"


@pytest.mark.parametrize(
    "default_row_limit, row_limit, expected_query",
    [
        (5, None, "SELECT * FROM (SELECT ID FROM T) LIMIT 5"),
        (5, 100, "SELECT * FROM (SELECT ID FROM T) LIMIT 100"),
    ],
)
def test_execute_query_row_limit(default_row_limit, row_limit, expected_query):
    """
    Test that a configured default_row_limit is applied only when the caller
    omits row_limit, and that an explicit row_limit always takes precedence
    over the default.
    """
    connection = _mock_connection()
    connection.execute_query.return_value.fetchall.return_value = [{"ID": 1}]
    config = MagicMock()
    config.enable_read_query = True
    config.default_row_limit = default_row_limit
    server = ExasolMCPServer(connection=connection, config=config)
    server.execute_query("SELECT ID FROM T", row_limit=row_limit)
    query = connection.execute_query.call_args[0][0]
    assert collapse_spaces(query) == collapse_spaces(expected_query)


def test_list_keywords():
    connection = _mock_connection()
    connection.execute_query.return_value.fetchcol.return_value = ["SELECT", "INSERT"]
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)
    result = server.list_keywords(reserved=True, letter="S")
    assert result == ["SELECT", "INSERT"]


def test_list_preprocessors():
    connection = _mock_connection()
    connection.execute_query.return_value.fetchall.return_value = []
    connection.execute_query.return_value.fetchval.return_value = "MY_PREPROCESSOR"
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)
    result = server.list_preprocessors()
    assert result.preprocessors == []
    assert result.current_preprocessor == "MY_PREPROCESSOR"


def test_health_check_healthy():
    connection = _mock_connection()
    connection.execute_query.return_value.fetchval.return_value = 1
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)
    response = server.health_check()
    assert b'"status":"healthy"' in response.body


def test_health_check_unhealthy():
    connection = MagicMock()
    connection.execute_query.side_effect = RuntimeError("boom")
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)
    response = server.health_check()
    assert b'"status":"unhealthy"' in response.body


def test_describe_many_success():
    result = ExasolMCPServer._describe_many(
        "MY_SCHEMA", ["a", "b"], lambda schema, name: f"described-{schema}-{name}"
    )
    assert result == ["described-MY_SCHEMA-a", "described-MY_SCHEMA-b"]


def test_describe_many_aggregates_errors():
    def describe_one(schema_name: str, name: str) -> str:
        if name == "bad":
            raise ValueError(f"{schema_name}.{name} not found.")
        return name

    with pytest.raises(ValueError, match="MY_SCHEMA.bad not found"):
        ExasolMCPServer._describe_many("MY_SCHEMA", ["good", "bad"], describe_one)


def _make_describe_tables_server() -> ExasolMCPServer:
    connection = _mock_connection()
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)

    def describe_one(schema_name: str, table_name: str) -> DBTable:
        if table_name == "MISSING":
            raise ValueError(f"The table or view {schema_name}.{table_name} not found.")
        return DBTable(schema=schema_name, name=table_name, comment=None, columns=[])

    server._describe_one_table = describe_one
    return server


def test_describe_tables_batch_returns_list_in_order():
    server = _make_describe_tables_server()
    result = server.describe_tables("MY_SCHEMA", ["T1", "T2"])
    assert [t.name for t in result] == ["T1", "T2"]


def test_describe_tables_batch_missing_name_raises():
    server = _make_describe_tables_server()
    with pytest.raises(ValueError, match="MY_SCHEMA.MISSING not found"):
        server.describe_tables("MY_SCHEMA", ["T1", "MISSING"])


def test_describe_system_table_returns_single_object():
    server = _make_describe_tables_server()
    result = server.describe_system_table("EXA_ALL_COLUMNS")
    assert isinstance(result, DBTable)
    assert result.schema == "SYS"
    assert result.name == "EXA_ALL_COLUMNS"


def test_describe_statistics_table_returns_single_object():
    server = _make_describe_tables_server()
    result = server.describe_statistics_table("EXA_DBA_SESSIONS")
    assert isinstance(result, DBTable)
    assert result.schema == "EXA_STATISTICS"
    assert result.name == "EXA_DBA_SESSIONS"


def test_describe_functions_batch_returns_list_in_order():
    connection = _mock_connection()
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)

    def describe(schema_name: str, func_name: str) -> DBReturnFunction:
        return DBReturnFunction(
            schema=schema_name,
            name=func_name,
            comment=None,
            input=[],
            returns="INTEGER",
        )

    with patch(
        "exasol.ai.mcp.server.tools.mcp_server.FuncParameterParser"
    ) as parser_cls:
        parser_cls.return_value.describe.side_effect = describe
        result = server.describe_functions("MY_SCHEMA", ["F1", "F2"])

    assert [f.name for f in result] == ["F1", "F2"]


def test_describe_functions_batch_missing_name_raises():
    connection = _mock_connection()
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)

    def describe(schema_name: str, func_name: str) -> DBReturnFunction:
        if func_name == "MISSING":
            raise ValueError(
                f"The function or script {schema_name}.{func_name} not found."
            )
        return DBReturnFunction(
            schema=schema_name,
            name=func_name,
            comment=None,
            input=[],
            returns="INTEGER",
        )

    with patch(
        "exasol.ai.mcp.server.tools.mcp_server.FuncParameterParser"
    ) as parser_cls:
        parser_cls.return_value.describe.side_effect = describe
        with pytest.raises(ValueError, match="MY_SCHEMA.MISSING not found"):
            server.describe_functions("MY_SCHEMA", ["F1", "MISSING"])


def test_describe_scripts_batch_returns_list_in_order():
    connection = _mock_connection()
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)

    def describe(schema_name: str, func_name: str) -> DBReturnFunction | DBEmitFunction:
        return DBReturnFunction(
            schema=schema_name,
            name=func_name,
            comment=None,
            input=[],
            returns="INTEGER",
        )

    with patch(
        "exasol.ai.mcp.server.tools.mcp_server.ScriptParameterParser"
    ) as parser_cls:
        parser_cls.return_value.describe.side_effect = describe
        result = server.describe_scripts("MY_SCHEMA", ["S1", "S2"])

    assert [s.name for s in result] == ["S1", "S2"]


def test_describe_scripts_batch_missing_name_raises():
    connection = _mock_connection()
    config = MagicMock()
    server = ExasolMCPServer(connection=connection, config=config)

    def describe(schema_name: str, func_name: str) -> DBReturnFunction | DBEmitFunction:
        if func_name == "MISSING":
            raise ValueError(
                f"The function or script {schema_name}.{func_name} not found."
            )
        return DBReturnFunction(
            schema=schema_name,
            name=func_name,
            comment=None,
            input=[],
            returns="INTEGER",
        )

    with patch(
        "exasol.ai.mcp.server.tools.mcp_server.ScriptParameterParser"
    ) as parser_cls:
        parser_cls.return_value.describe.side_effect = describe
        with pytest.raises(ValueError, match="MY_SCHEMA.MISSING not found"):
            server.describe_scripts("MY_SCHEMA", ["S1", "MISSING"])


def _make_summarize_server() -> tuple[ExasolMCPServer, MagicMock]:
    connection = _mock_connection()
    statement = connection.execute_query.return_value
    statement.fetchone.return_value = {"ROW_COUNT": 2}
    statement.fetchcol.return_value = [1, 2]
    statement.fetchall.return_value = [{"id": 1}, {"id": 2}]
    config = MagicMock()
    config.enable_summarize_table = True
    server = ExasolMCPServer(connection=connection, config=config)
    columns = [DBColumn(name="id", type="DECIMAL(18,0)", comment=None)]
    server.describe_columns = MagicMock(return_value=columns)
    server._get_table_comment = MagicMock(return_value="a comment")
    return server, connection


def test_summarize_table():
    server, _ = _make_summarize_server()
    result = server.summarize_table("MY_SCHEMA", "MY_TABLE")
    assert result.schema == "MY_SCHEMA"
    assert result.comment == "a comment"
    assert result.row_count == 2
    assert result.sample == [{"id": 1}, {"id": 2}]


def test_summarize_table_tabular():
    server, _ = _make_summarize_server()
    result = server.summarize_table_tabular("MY_SCHEMA", "MY_TABLE")
    assert result.comment == "a comment"
    assert result.row_count == 2
    assert result.sample == QueryResult(columns=["id"], rows=[[1], [2]])


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
        WHERE SESSION_ID = CURRENT_SESSION AND STMT_ID = (
            SELECT MAX(STMT_ID) FROM {_PROFILE_TABLE_SQL}
            WHERE SESSION_ID = CURRENT_SESSION AND STMT_ID < CURRENT_STATEMENT AND COMMAND_CLASS = 'DQL'
        )
        ORDER BY PART_ID
    """)
    assert sql == expected


def test_build_profile_status_query():
    sql = collapse_spaces(_build_profile_status_query())
    expected = collapse_spaces("""
        SELECT "SESSION_VALUE"
        FROM "SYS"."EXA_PARAMETERS"
        WHERE "PARAMETER_NAME" = 'PROFILE'
    """)
    assert sql == expected


def _make_profile_server(profile_already_on: bool):
    connection = _mock_connection()
    connection.execute_query.return_value.fetchval.return_value = (
        "ON" if profile_already_on else "OFF"
    )
    connection.execute_query.return_value.fetchall.return_value = []
    config = MagicMock()
    config.enable_query_profiling = True
    server = ExasolMCPServer(connection=connection, config=config)
    return server, connection


def test_profile_query_enables_and_disables_profiling_when_off():
    server, connection = _make_profile_server(profile_already_on=False)
    server.profile_query("SELECT 1")
    calls = [str(c) for c in connection.execute_query.call_args_list]
    statements = connection.execute_query.call_args_list[-1][0][0]
    assert statements[0] == "ALTER SESSION SET PROFILE = 'ON'"
    assert statements[-2] == "ALTER SESSION SET PROFILE = 'OFF'"


def test_profile_query_skips_profile_toggle_when_already_on():
    server, connection = _make_profile_server(profile_already_on=True)
    server.profile_query("SELECT 1")
    statements = connection.execute_query.call_args_list[-1][0][0]
    assert "ALTER SESSION SET PROFILE = 'ON'" not in statements
    assert "ALTER SESSION SET PROFILE = 'OFF'" not in statements


def test_build_list_preprocessors_query():
    sql = collapse_spaces(_build_list_preprocessors_query())
    expected = collapse_spaces("""
        SELECT
            "SCRIPT_SCHEMA" AS "schema",
            "SCRIPT_NAME" AS "name",
            "SCRIPT_COMMENT" AS "comment"
        FROM "SYS"."EXA_ALL_SCRIPTS"
        WHERE "SCRIPT_TYPE" = 'PREPROCESSOR'
        ORDER BY "SCRIPT_SCHEMA" NULLS FIRST, "SCRIPT_NAME" NULLS FIRST
    """)
    assert sql == expected


def test_build_current_preprocessor_query():
    sql = collapse_spaces(_build_current_preprocessor_query())
    expected = collapse_spaces("""
        SELECT "SESSION_VALUE"
        FROM "SYS"."EXA_PARAMETERS"
        WHERE "PARAMETER_NAME" = 'SQL_PREPROCESSOR_SCRIPT'
    """)
    assert sql == expected


def test_build_set_preprocessor_query():
    sql = _build_set_preprocessor_query("MY_SCHEMA", "MY_PREPROCESSOR")
    assert (
        sql
        == 'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "MY_SCHEMA"."MY_PREPROCESSOR"'
    )
