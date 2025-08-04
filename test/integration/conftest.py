from itertools import chain
from test.utils.db_objects import (
    ExaColumn,
    ExaConstraint,
    ExaFunction,
    ExaParameter,
    ExaSchema,
    ExaTable,
    ExaView,
)
from textwrap import dedent
from typing import Any

import pytest

from exasol.ai.mcp.server.utils import sql_text_value


def format_table_rows(rows: list[tuple[Any, ...]]) -> str:
    def format_value(val: Any) -> str:
        if isinstance(val, str):
            return sql_text_value(val)
        return str(val)

    def format_row(row: tuple[Any, ...]) -> str:
        column_list = ", ".join(map(format_value, row))
        return f"({column_list})"

    return ", ".join(map(format_row, rows))


@pytest.fixture(scope="session")
def db_schemas(db_schema_name) -> list[ExaSchema]:
    return [
        ExaSchema(name=db_schema_name, comment=None, is_new=False),
        ExaSchema(
            name="new_schema",
            comment="new schema for the integration tests",
            is_new=True,
        ),
    ]


@pytest.fixture(scope="session")
def db_tables() -> list[ExaTable]:
    return [
        ExaTable(
            name="ski_resort",
            comment="the table contains basic information about ski resorts",
            columns=[
                ExaColumn(
                    name="resort_id",
                    comment="the ski resort id",
                    type="DECIMAL(18,0)",
                ),
                ExaColumn(name="resort_name", comment=None, type="VARCHAR(1000) UTF8"),
                ExaColumn(name="country", comment=None, type="VARCHAR(100) UTF8"),
                ExaColumn(
                    name="altitude",
                    comment="the ski resort altitude above the see level in meters",
                    type="DECIMAL(18,0)",
                ),
            ],
            rows=[
                (1000, "Val Thorens", "France", 2300),
                (1001, "Courchevel", "France", 1850),
                (1002, "Kitzbuhel", "Austria", 762),
            ],
            constraints=[
                ExaConstraint(type="PRIMARY KEY", columns=["resort_id"]),
            ],
        ),
        ExaTable(
            name="ski_run",
            comment="the table contains detailed information about ski runs in different resorts",
            columns=[
                ExaColumn(
                    name="resort_id",
                    comment="the ski resort id",
                    type="DECIMAL(18,0)",
                ),
                ExaColumn(name="run_name", comment=None, type="VARCHAR(200) UTF8"),
                ExaColumn(
                    name="difficulty",
                    comment="the run difficulty level - green, blue, red, black",
                    type="VARCHAR(10) UTF8",
                ),
                ExaColumn(
                    name="length",
                    comment="the run length in meters",
                    type="DECIMAL(18,0)",
                ),
            ],
            constraints=[
                ExaConstraint(type="PRIMARY KEY", columns=["resort_id", "run_name"]),
                ExaConstraint(
                    name="RESORT_FK",
                    type="FOREIGN KEY",
                    columns=["resort_id"],
                    ref_table="ski_resort",
                    ref_columns=["resort_id"],
                ),
            ],
            rows=[
                (1000, "Christine", "Blue", 1200),
                (1000, "Allamande", "Red", 950),
                (1001, "Combe de la Saulire", "Red", 1550),
                (1001, "Chanrossa", "Black", 800),
                (1002, "Hochsaukaser", "Red", 1900),
                (1002, "Steilhang", "Black", 1200),
                (1002, "Sonnenrast", "Green", 200),
            ],
        ),
        ExaTable(
            name="competitions",
            comment="information about competitions in different resorts",
            columns=[
                ExaColumn(name="series", type="VARCHAR(500) UTF8", comment=None),
                ExaColumn(name="year", type="DECIMAL(18,0)", comment=None),
                ExaColumn(name="resort_id", type="DECIMAL(18,0)", comment=None),
                ExaColumn(
                    name="competition_run", type="VARCHAR(200) UTF8", comment=None
                ),
            ],
            constraints=[
                ExaConstraint(
                    name="COMPETITION_FK",
                    type="FOREIGN KEY",
                    columns=["resort_id", "competition_run"],
                    ref_table="ski_run",
                    ref_columns=["resort_id", "run_name"],
                )
            ],
            rows=[],
        ),
    ]


@pytest.fixture(scope="session")
def db_views() -> list[ExaView]:
    return [
        ExaView(
            name="high_altitude_resort",
            comment="ski resorts situated at the altitude higher than 2000 meters",
            sql='SELECT * FROM "{schema}"."ski_resort" WHERE "altitude" > 2000',
        ),
        ExaView(
            name="difficult_run",
            comment="the view lists all known black runs",
            sql=(
                'SELECT * FROM "{schema}"."ski_run" WHERE UPPER("difficulty")'
                " = 'BLACK'"
            ),
        ),
    ]


@pytest.fixture(scope="session")
def db_functions() -> list[ExaFunction]:
    return [
        ExaFunction(
            name="cut_middle",
            comment="cuts a middle of the provided text",
            inputs=[
                ExaParameter(name="inp_text", type="VARCHAR(1000)"),
                ExaParameter(name="cut_from", type="DECIMAL(18,0)"),
                ExaParameter(name="cut_to", type="DECIMAL(18,0)"),
            ],
            returns="VARCHAR(1000)",
            body=dedent(
                """
                CREATE OR REPLACE FUNCTION "{schema}"."cut_middle"(
                    inp_text VARCHAR(1000), cut_from DECIMAL(18,0), cut_to DECIMAL(18,0))
                RETURN VARCHAR(1000)
                IS
                    len INTEGER;
                    res VARCHAR(1000);
                BEGIN
                    len := LENGTH(inp_text);
                    IF cut_from <= 0 OR cut_to <= cut_from OR len < cut_from THEN
                        res := inp_text;
                    ELSE
                        res := LEFT(inp_text, cut_from) || RIGHT(inp_text, len - cut_to + 1);
                    END IF;
                    RETURN res;
                END;
                /
            """
            ),
        ),
        ExaFunction(
            name="factorial",
            comment="computes the factorial of a number",
            inputs=[ExaParameter(name="num", type="DECIMAL(18,0)")],
            returns="DECIMAL(18,0)",
            body=dedent(
                """
                CREATE OR REPLACE FUNCTION "{schema}"."factorial"(num DECIMAL(18,0))
                RETURN DECIMAL(18,0)
                IS
                    res INTEGER;
                BEGIN
                    res := 1;
                    FOR i := 1 TO num
                    DO
                        res := res * i;
                    END FOR;
                    RETURN res;
                END;
                /
            """
            ),
        ),
    ]


@pytest.fixture(scope="session")
def db_scripts() -> list[ExaFunction]:
    return [
        ExaFunction(
            name="fibonacci",
            comment="emits Fibonacci sequence of the given length",
            inputs=[ExaParameter(name="seq_length", type="DECIMAL(18,0)")],
            emits=[
                ExaParameter(name="NUM", type="DECIMAL(18,0)"),
                ExaParameter(name="VAL", type="DECIMAL(18,0)"),
            ],
            body=dedent(
                """
                CREATE OR REPLACE PYTHON3 SCALAR SCRIPT "{schema}"."fibonacci"(
                    seq_length DECIMAL(18,0))
                EMITS (num DECIMAL(18,0), val DECIMAL(18,0))
                AS
                def run(ctx):
                        last_two = [0, 1]
                        next_id = 0
                        for i in range(ctx.seq_length):
                                if i >= 2:
                                        last_two[next_id] = sum(last_two)
                                ctx.emit(i, last_two[next_id])
                                next_id = (next_id + 1) % 2
                /
            """
            ),
        ),
        ExaFunction(
            name="weighted_length",
            comment="computes weighted sum of the input text lengths",
            inputs=[
                ExaParameter(name="text", type="VARCHAR(100000) UTF8"),
                ExaParameter(name="weight", type="DOUBLE"),
            ],
            returns="DOUBLE",
            body=dedent(
                """
                CREATE OR REPLACE PYTHON3 SET SCRIPT "{schema}"."weighted_length"(
                    text VARCHAR(100000) UTF8, weight DOUBLE)
                RETURNS DOUBLE
                AS
                def run(ctx):
                        more_data = True
                        result = 0.0
                        while more_data:
                                result += len(ctx.text) * ctx.weight
                                more_data = ctx.next()
                        return result
                /
            """
            ),
        ),
    ]


@pytest.fixture(scope="session")
def setup_database(
    pyexasol_connection,
    db_schema_name,
    db_schemas,
    db_tables,
    db_views,
    db_functions,
    db_scripts,
) -> None:
    try:
        for schema in db_schemas:
            if schema.is_new:
                query = f'DROP SCHEMA IF EXISTS "{schema.name}" CASCADE'
                pyexasol_connection.execute(query=query)
                query = f'CREATE SCHEMA "{schema.name}"'
                # Will restore the currently opened schema after creating a new one.
                current_schema = pyexasol_connection.current_schema()
                try:
                    pyexasol_connection.execute(query=query)
                finally:
                    if current_schema:
                        pyexasol_connection.execute(f'OPEN SCHEMA "{current_schema}"')
                    else:
                        pyexasol_connection.execute("CLOSE SCHEMA")
                if schema.comment:
                    query = (
                        f'COMMENT ON SCHEMA "{schema.name}" ' f"IS '{schema.comment}'"
                    )
                    pyexasol_connection.execute(query=query)
            for table in db_tables:
                query = f"CREATE OR REPLACE TABLE {table.decl(schema.name)}"
                pyexasol_connection.execute(query=query)
                if table.rows:
                    query = (
                        f'INSERT INTO "{schema.name}"."{table.name}" '
                        f"VALUES {format_table_rows(table.rows)}"
                    )
                    pyexasol_connection.execute(query=query)
            for view in db_views:
                query = f"CREATE OR REPLACE VIEW {view.decl(schema.name)}"
                pyexasol_connection.execute(query=query)
            for func, func_type in chain(
                zip(db_functions, ["FUNCTION"] * len(db_functions)),
                zip(db_scripts, ["SCRIPT"] * len(db_scripts)),
            ):
                pyexasol_connection.execute(query=func.body.format(schema=schema.name))
                if func.comment:
                    query = (
                        f'COMMENT ON {func_type} "{schema.name}"."{func.name}" '
                        f"IS '{func.comment}'"
                    )
                    pyexasol_connection.execute(query=query)

        yield

    finally:
        for schema in db_schemas:
            if schema.is_new:
                query = f'DROP SCHEMA IF EXISTS "{schema.name}" CASCADE'
                pyexasol_connection.execute(query=query)
            else:
                for table in db_tables[::-1]:
                    query = f'DROP TABLE IF EXISTS "{schema.name}"."{table.name}"'
                    pyexasol_connection.execute(query=query)
                for view in db_views:
                    query = f'DROP VIEW IF EXISTS "{schema.name}"."{view.name}"'
                    pyexasol_connection.execute(query=query)
                for func in chain(db_functions, db_scripts):
                    query = f'DROP FUNCTION IF EXISTS "{schema.name}"."{func.name}"'
                    pyexasol_connection.execute(query=query)
