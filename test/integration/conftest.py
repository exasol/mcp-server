import time
from itertools import chain
from test.utils.db_objects import (
    ExaBfsDir,
    ExaBfsFile,
    ExaColumn,
    ExaConstraint,
    ExaFunction,
    ExaParameter,
    ExaSchema,
    ExaTable,
    ExaView,
)
from test.utils.mcp_oidc_constants import DOCKER_DB_NAME
from test.utils.sql_utils import format_table_rows
from textwrap import dedent

import exasol.bucketfs as bfs
import pytest
from exasol.saas.client.api_access import timestamp_name


def pytest_addoption(parser):
    parser.addoption(
        "--manual",
        action="store_true",
        default=False,
        help="Indicate that pytest is started manually, i.e. not from a CI workflow",
    )


@pytest.fixture(scope="session")
def started_manually(request):
    return request.config.getoption("--manual")


@pytest.fixture(scope="session")
def run_on_itde(backend) -> None:
    if backend != "onprem":
        pytest.skip()


@pytest.fixture(scope="session")
def run_on_saas(backend) -> None:
    if backend != "saas":
        pytest.skip()


@pytest.fixture(scope="session")
def database_name(backend, project_short_tag):
    """
    Overrides the DB name fixture, making it easy to know the container name.
    """
    if backend == "saas":
        return timestamp_name(project_short_tag)
    return DOCKER_DB_NAME


@pytest.fixture(scope="session")
def db_schemas(db_schema_name) -> list[ExaSchema]:
    return [
        ExaSchema(
            name=db_schema_name, comment=None, is_new=False, keywords=[db_schema_name]
        ),
        ExaSchema(
            name="new_schema",
            comment="new schema for the MCP integration tests",
            is_new=True,
            keywords=["new_schema", "MCP integration tests"],
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
            keywords=["ski_resort", "basic information"],
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
            keywords=["ski run", "detailed information"],
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
            keywords=["competitions", "different resorts"],
            rows=[],
        ),
    ]


@pytest.fixture(scope="session")
def db_views() -> list[ExaView]:
    return [
        ExaView(
            name="high_altitude_resort",
            comment="ski resorts situated at the altitude higher than 2000 meters",
            keywords=["high_altitude"],
            sql='SELECT * FROM "{schema}"."ski_resort" WHERE "altitude" > 2000',
        ),
        ExaView(
            name="difficult_run",
            comment="the view lists all known black runs",
            keywords=["difficult_run", "black"],
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
            keywords=["cut", "middle"],
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
            keywords=["factorial"],
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
            keywords=["fibonacci"],
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
            keywords=["weighted_length"],
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

        pyexasol_connection.execute(query="COMMIT")
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


@pytest.fixture(scope="session")
def bfs_data() -> ExaBfsDir:
    return ExaBfsDir(
        name="Species",
        items=[
            ExaBfsDir(
                name="Carnivores",
                items=[
                    ExaBfsDir(
                        name="Cat",
                        items=[
                            ExaBfsFile(
                                name="Cougar",
                                content=(
                                    b"A large, powerful cat with a tawny coat, also "
                                    b"known as a mountain lion or puma. It has the "
                                    b"greatest geographic range of any wild "
                                    b"terrestrial mammal in the Americas."
                                ),
                            ),
                            ExaBfsFile(
                                name="Bobcat",
                                content=(
                                    b"A medium-sized North American cat distinguished "
                                    b"by its tufted ears, spotted coat, and short "
                                    b'"bobbed" tail. It is highly adaptable and a '
                                    b"stealthy predator."
                                ),
                            ),
                        ],
                    ),
                    ExaBfsDir(
                        name="Dog",
                        items=[
                            ExaBfsFile(
                                name="Gray Wolf",
                                content=(
                                    b"The largest extant wild member of the dog family, "
                                    b"living and hunting in complex social packs. It is "
                                    b"a keystone predator known for its intelligence "
                                    b"and cooperative hunting."
                                ),
                            ),
                            ExaBfsFile(
                                name="Gray Fox",
                                content=(
                                    b"A unique fox species known for its grizzled gray "
                                    b"and rusty coat and its strong ability to climb "
                                    b"trees using semi-retractable claws, a trait rare "
                                    b"among canids."
                                ),
                            ),
                        ],
                    ),
                    ExaBfsDir(
                        name="Bear",
                        items=[
                            ExaBfsFile(
                                name="American Black Bear",
                                content=(
                                    b"The most common and widely distributed bear "
                                    b"species in North America. It is an omnivore "
                                    b"whose diet varies greatly by season and location."
                                ),
                            ),
                        ],
                    ),
                ],
            ),
            ExaBfsDir(
                name="Even-toed Ungulates",
                items=[
                    ExaBfsDir(
                        name="Deer",
                        items=[
                            ExaBfsFile(
                                name="White-tailed Deer",
                                content=(
                                    b"A medium-sized deer ubiquitous across much of "
                                    b"North America. It is named for the bright white "
                                    b"underside of its tail, which it raises as a flag "
                                    b"when alarmed."
                                ),
                            ),
                            ExaBfsFile(
                                name="Elk",
                                content=(
                                    b"One of the largest species within the deer "
                                    b"family, known for the males' large, branching "
                                    b"antlers and their loud, bugling vocalizations "
                                    b"during the rut."
                                ),
                            ),
                        ],
                    ),
                    ExaBfsFile(
                        name="Cattle_Sheep_Goat",
                        content=(
                            b"A massive family of cloven-hoofed ruminants that includes "
                            b"cattle, bison, sheep, goats, and antelope. Males (and "
                            b"often females) typically possess permanent, unbranched "
                            b"horns."
                        ),
                    ),
                ],
            ),
            ExaBfsDir(
                name="Rodents",
                items=[
                    ExaBfsDir(
                        name="Squirrel",
                        items=[
                            ExaBfsFile(
                                name="Eastern Gray Squirrel",
                                content=(
                                    b"A common tree squirrel in eastern North America, "
                                    b"primarily gray with a white underside. It is a "
                                    b"prolific scatter-hoarder of nuts and acorns."
                                ),
                            ),
                            ExaBfsFile(
                                name="Eastern Chipmunk",
                                content=(
                                    b"A small, striped ground squirrel with prominent "
                                    b"cheek pouches used to carry food. It is known for "
                                    b"its burrowing habits and energetic, chattering "
                                    b"behavior."
                                ),
                            ),
                        ],
                    ),
                    ExaBfsFile(
                        name="Hamster-Vole-Lemming",
                        content=(
                            b"A hugely diverse family of small rodents, including "
                            b"hamsters, voles, lemmings, and New World rats and mice. "
                            b"They are found in a vast array of habitats across the "
                            b"globe."
                        ),
                    ),
                ],
            ),
            ExaBfsFile(
                name="Rabbits_Hares",
                content=(
                    b"An order of herbivorous mammals that includes rabbits, hares, "
                    b"and pikas. They are characterized by two pairs of upper incisors, "
                    b"one behind the other."
                ),
            ),
        ],
    )


@pytest.fixture(scope="session")
def setup_bucketfs(
    backend_aware_bucketfs_params,
    bfs_data,
) -> None:

    def write_content(node: ExaBfsDir, bfs_path: bfs.path.PathLike) -> None:
        for item in node.items:
            if isinstance(item, ExaBfsFile):
                bfs_file = bfs_path.joinpath(item.name)
                bfs_file.write(item.content)
            elif isinstance(item, ExaBfsDir):
                bfs_sub_dir = bfs_path.joinpath(item.name)
                write_content(item, bfs_sub_dir)

    bfs_root = bfs.path.build_path(**backend_aware_bucketfs_params, path=bfs_data.name)
    write_content(bfs_data, bfs_root)
    time.sleep(10)
