from dataclasses import dataclass
from typing import (
    Annotated,
    Any,
)

from pydantic import BaseModel
from pydantic.functional_validators import AfterValidator

from exasol.ai.mcp.server.utils import sql_text_value


def check_no_double_quotes(v: str) -> str:
    if '"' in v:
        raise ValueError("Double-quote characters are not allowed in a field name.")
    return v


NoDoubleQuotesStr = Annotated[str, AfterValidator(check_no_double_quotes)]


@dataclass
class ExaDbResult:
    result: list[str:Any]


class MetaSettings(BaseModel):
    """
    The general settings for a single type of metadata, e.g. tables.
    """

    enable: bool = True
    """
    Allows to disable the listing of a particular type of metadata.
    """

    name_field: NoDoubleQuotesStr = "name"
    """
    The name of the output field that contains the object name, e.g. "table_name".
    """

    comment_field: NoDoubleQuotesStr = "comment"
    """
    The name of the output field that contains the comment, e.g. "table_comment".
    """


class MetaListSettings(MetaSettings):
    """
    The settings for a type of metadata that can be listed and filtered.
    """

    like_pattern: str | None = None
    """
    An optional sql-style pattern for the object name filtering.

    Use case example: The user wants to create a set of purified de-normalised views on
    the existing database and limit the table listings to only these views. One way of
    achieving this is to create the views in a new schema and limit the listing of the
    schemas to this schema only. In the case of no permission to create schema, one can
    create the views in an existing schema and use some prefix for name disambiguation.
    This prefix can also be used for filtering the views in the listing.
    """
    regexp_pattern: str | None = None
    """
    An optional regular expression pattern for the object name filtering.
    Both like_pattern and regexp_pattern can be used at the same time, although there is
    not much point in doing so.
    """

    @property
    def select_predicate(self) -> str:
        """
        The SQL predicate for the object filtering by name.
        Empty string if neigher of the filtering patterns are defined.
        """
        conditions: list[str] = []
        if self.like_pattern:
            conditions.append(
                f"""local."{self.name_field}" LIKE {sql_text_value(self.like_pattern)}"""
            )
        if self.regexp_pattern:
            conditions.append(
                f"""local."{self.name_field}" REGEXP_LIKE {sql_text_value(self.regexp_pattern)}"""
            )
        return " AND ".join(conditions)


class MetaColumnSettings(MetaSettings):
    """
    The settings for listing columns and constraints when describing a table.
    """

    type_field: NoDoubleQuotesStr = "type"
    """
    The name of the output field for the column SQL type.
    """

    constraint_name_field: NoDoubleQuotesStr = "name"
    """
    The name of the output field for the constraint name if it was specified.
    """

    constraint_type_field: NoDoubleQuotesStr = "type"
    """
    The name of the output field for the constraint type, e.g. PRIMARY KEY.
    """

    constraint_columns_field: NoDoubleQuotesStr = "columns"
    """
    The name of the output field for the list of columns the constraint is applied to.
    """

    referenced_schema_field: NoDoubleQuotesStr = "referenced_schema"
    """
    The name of the output field for the referenced schema in the FOREIGN KEY constraint.
    """

    referenced_table_field: NoDoubleQuotesStr = "referenced_table"
    """
    The name of the output field for the referenced table in the FOREIGN KEY constraint.
    """

    referenced_columns_field: NoDoubleQuotesStr = "referenced_columns"
    """
    The name of the output field for the list of columns in a referenced table in the
    FOREIGN KEY constraint.
    """

    columns_field: NoDoubleQuotesStr = "columns"
    """
    The name of the output field for the list of columns in a table being described.
    """

    constraints_field: NoDoubleQuotesStr = "constraints"
    """
    The name of the output field for the list of constraints in a table being described.
    """

    table_comment_field: NoDoubleQuotesStr = "table_comment"
    """
    The name of the output field for the table/view comment.
    """

    usage_field: NoDoubleQuotesStr = "usage"
    """
    The name of the output field for general instructions on using a table.
    """


class MetaParameterSettings(MetaSettings):
    """
    The settings for listing input/output parameters when describing a function or a
    UDF script.
    """

    type_field: NoDoubleQuotesStr = "parameter_type"
    """
    The name of the output field for the SQL type of a parameter or return value.
    """

    input_field: NoDoubleQuotesStr = "inputs"
    """
    The name of the output field for the list of input parameters.
    """

    return_field: NoDoubleQuotesStr = "returns"
    """
    The name of the output field for the return value.
    """

    emit_field: NoDoubleQuotesStr = "emits"
    """
    The name of the output field for the list of parameters emitted by a UDF.
    """

    usage_field: NoDoubleQuotesStr = "usage"
    """
    The name of the output field for a function or UDF usage, e.g. a call example.
    """


class McpServerSettings(BaseModel):
    """
    MCP server configuration.
    """

    schemas: MetaListSettings = MetaListSettings(
        name_field="schema_name", comment_field="schema_comment"
    )
    tables: MetaListSettings = MetaListSettings(
        name_field="table_name", comment_field="table_comment"
    )
    views: MetaListSettings = MetaListSettings(
        enable=False, name_field="table_name", comment_field="table_comment"
    )
    functions: MetaListSettings = MetaListSettings(
        name_field="function_name", comment_field="function_comment"
    )
    scripts: MetaListSettings = MetaListSettings(
        name_field="script_name", comment_field="script_comment"
    )
    columns: MetaColumnSettings = MetaColumnSettings(
        name_field="column_name", comment_field="column_comment"
    )
    parameters: MetaParameterSettings = MetaParameterSettings(
        name_field="parameter_name", comment_field="function_comment"
    )
    enable_read_query: bool = False
