"""
TEMPORARY CI probe, to be removed before merging: checks whether a Python UDF
can be executed in the CI test database at all.
"""


def test_probe_fibonacci_udf(pyexasol_connection, setup_database, db_schema_name):
    languages = pyexasol_connection.execute(
        "SELECT SESSION_VALUE FROM EXA_PARAMETERS "
        "WHERE PARAMETER_NAME = 'SCRIPT_LANGUAGES'"
    ).fetchval()
    print(f"\nPROBE SCRIPT_LANGUAGES: {languages}")
    rows = pyexasol_connection.execute(
        f'SELECT "{db_schema_name}"."fibonacci"(5) FROM DUAL'
    ).fetchall()
    print(f"PROBE fibonacci rows: {rows}")
    assert len(rows) == 5
