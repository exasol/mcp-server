from typing import (
    Annotated,
    ClassVar,
)

from pydantic import (
    BaseModel,
    Field,
)


class DBObject(BaseModel):
    model_tag: ClassVar[str] = (
        "Information about a database object, such as: "
        "table, view, column, function, script or parameter. "
    )
    model_notes: ClassVar[str] = (
        "Important notes: "
        "- In an SQL query, the names of database objects should be enclosed in "
        "double quotes. "
        "- A reference to a table should include a reference to its schema. "
        "- The SELECT column list cannot have both the * and explicit column names."
    )
    name: Annotated[str, Field(description='Object name, e.g. "MY_TABLE"')]
    comment: Annotated[
        str, Field(description="Object comment, if available", default="")
    ]


class QualifiedDBObject(DBObject):
    schema: Annotated[
        str, Field(description="Name of the schema where the object is located")
    ]


class DBColumn(DBObject):
    type: Annotated[
        str,
        Field(
            description=(
                "SQL type. Examples: VARCHAR(n), DECIMAL(p,s), TIMESTAMP(p). "
                "Use the tool list_sql_types to get information about the "
                "SQL types supported by EXASOL."
            )
        ),
    ]


class DBConstraint(BaseModel):
    name: Annotated[str, Field(description="Constraint name, if available")]
    type: Annotated[str, Field(description='"PRIMARY KEY" or "FOREIGN KEY"')]
    columns: Annotated[
        str, Field(description="List of columns the constraint is applied to")
    ]
    referenced_schema: Annotated[
        str, Field(description="The schema referenced in the FOREIGN KEY constraint")
    ]
    referenced_table: Annotated[
        str, Field(description="The table referenced in the FOREIGN KEY constraint")
    ]


class DBFunctionBase(QualifiedDBObject):
    model_tag: ClassVar[str] = "Detailed information about a function"
    input: Annotated[
        list[DBColumn],
        Field(
            description=(
                "List of input parameters. "
                "For functions with dynamic input this list will be empty."
            )
        ),
    ]
    dynamic_input: Annotated[
        bool,
        Field(description="If True, the function accepts dynamic input parameters"),
    ]
    usage: Annotated[str, Field(description="Function usage notes")]


class DBFunction(DBFunctionBase):
    returns: DBColumn


class DBEmitFunction(DBFunctionBase):
    model_tag: ClassVar[str] = (
        "Detailed information about a User Defined Function that emits data rows"
    )
    emits: Annotated[
        list[DBColumn],
        Field(
            description=(
                "List of emitted columns. "
                "For functions with dynamic output this list will be empty."
            )
        ),
    ]
    dynamic_output: Annotated[
        bool, Field(description="If True, the function emits dynamic output columns")
    ]
