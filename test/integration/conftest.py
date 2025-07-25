from itertools import chain
from test.utils.db_objects import (
    ExaColumn,
    ExaConstraint,
    ExaFunction,
    ExaSchema,
    ExaTable,
    ExaView,
)
from textwrap import dedent

import pytest


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
                    constraint=ExaConstraint(type="PRIMARY KEY"),
                ),
                ExaColumn(name="country", comment=None, type="VARCHAR(100) UTF8"),
                ExaColumn(
                    name="altitude",
                    comment="the ski resort altitude above the see level in meters",
                    type="DECIMAL(18,0)",
                ),
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
                    constraint=ExaConstraint(
                        type="FOREIGN KEY", reference='"ski_resort"("resort_id")'
                    ),
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
            body=dedent(
                """
                CREATE OR REPLACE FUNCTION "{schema}"."cut_middle"(
                    inp_text VARCHAR(1000), cut_from INTEGER, cut_to INTEGER)
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
            body=dedent(
                """
                CREATE OR REPLACE FUNCTION "{schema}"."factorial"(num INTEGER)
                RETURN VARCHAR(1000)
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
            body=dedent(
                """
                CREATE OR REPLACE PYTHON3 SCALAR SCRIPT "{schema}"."fibonacci"(
                    seq_length INTEGER)
                EMITS (num INTEGER, val INTEGER)
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
            body=dedent(
                """
                CREATE OR REPLACE PYTHON3 SET SCRIPT "{schema}"."weighted_length"(
                    text VARCHAR(100000), weight DOUBLE)
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
                pyexasol_connection.execute(query=query)
                if schema.comment:
                    query = (
                        f'COMMENT ON SCHEMA "{schema.name}" ' f"IS '{schema.comment}'"
                    )
                    pyexasol_connection.execute(query=query)
            for table in db_tables:
                query = f"CREATE OR REPLACE TABLE {table.decl(schema.name)}"
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
