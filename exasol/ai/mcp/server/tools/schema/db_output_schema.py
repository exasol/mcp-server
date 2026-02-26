from typing import Annotated

from pydantic import (
    BaseModel,
    Field,
)

"""
Names of the attributes in the output schema.
These definitions should match the actual attribute names in the Pydantic models
defined in this module. It would be good to enforce this match somehow, but
currently there is no easy way of doing this.
"""
SCHEMA_FIELD = "schema"
NAME_FIELD = "name"
COMMENT_FIELD = "comment"
SQL_TYPE_FIELD = "type"
COLUMNS_FIELD = "columns"
CONSTRAINTS_FIELD = "constraints"
CONSTRAINT_NAME_FIELD = "name"
CONSTRAINT_TYPE_FIELD = "constraint_type"
CONSTRAINT_COLUMNS_FIELD = "columns"
REFERENCED_SCHEMA_FIELD = "referenced_schema"
REFERENCED_TABLE_FIELD = "referenced_table"
REFERENCED_COLUMNS_FIELD = "referenced_columns"
INPUT_FIELD = "input"
DYNAMIC_INPUT_FIELD = "dynamic_input"
USAGE_FIELD = "usage"
RETURNS_FIELD = "returns"
EMITS_FIELD = "emits"
DYNAMIC_OUTPUT_FIELD = "dynamic_output"
CREATE_PARAMS_FIELD = "create_params"
PRECISION_FIELD = "precision"


SQLType = Annotated[
    str,
    Field(description="SQL type. Examples: VARCHAR(n), DECIMAL(p,s), TIMESTAMP(p)."),
]


class DBObject(BaseModel):
    name: Annotated[
        str,
        Field(
            description=(
                'Database object name, e.g. "MY_TABLE". '
                "An object can be a table, view, column, function, script or "
                "a function parameter."
            )
        ),
    ]
    comment: Annotated[
        str | None, Field(description="Object comment, if available", default=None)
    ]


class QualifiedDBObject(DBObject):
    schema: Annotated[
        str, Field(description="Name of the schema where the object is located")
    ]


class DBColumn(DBObject):
    type: SQLType


class DBConstraint(BaseModel):
    name: Annotated[
        str | None, Field(description="Constraint name, if available", default=None)
    ]
    constraint_type: Annotated[str, Field(description='"PRIMARY KEY" or "FOREIGN KEY"')]
    columns: Annotated[
        str, Field(description="List of columns the constraint is applied to")
    ]
    referenced_schema: Annotated[
        str | None,
        Field(
            description="The schema referenced in the FOREIGN KEY constraint",
            default=None,
        ),
    ]
    referenced_table: Annotated[
        str | None,
        Field(
            description="The table referenced in the FOREIGN KEY constraint",
            default=None,
        ),
    ]
    referenced_columns: Annotated[
        str | None,
        Field(
            description=(
                "List of columns in the referenced table in the FOREIGN KEY constraint"
            ),
            default=None,
        ),
    ]


class DBTable(QualifiedDBObject):
    columns: list[DBColumn]
    constraints: list[DBConstraint] | None = None


class DBFunction(QualifiedDBObject):
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
        Field(
            description="If True, the function accepts dynamic input parameters",
            default=False,
        ),
    ]
    usage: Annotated[str, Field(description="Function usage notes", default="")]


class DBReturnFunction(DBFunction):
    returns: SQLType


class DBEmitFunction(DBFunction):
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
        bool,
        Field(
            description="If True, the function emits dynamic output columns",
            default=False,
        ),
    ]


class SQLTypeInfo(BaseModel):
    type: SQLType
    create_params: Annotated[
        str | None,
        Field(
            description="Type parameters to be specified when creating a table column",
            default=None,
        ),
    ]
    precision: Annotated[
        int | None, Field(description="Default precision when applicable", default=None)
    ]
