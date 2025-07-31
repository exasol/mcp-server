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
    The settings for a single type of metadata, e.g. tables.
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
    The settings for listing columns when describing a table. Adds few more fields to
    the metadata output.
    """

    type_field: NoDoubleQuotesStr = "column_type"
    primary_key_field: NoDoubleQuotesStr = "primary_key"
    foreign_key_field: NoDoubleQuotesStr = "foreign_key"
    ref_schema_field: NoDoubleQuotesStr = "referenced_schema"
    ref_table_field: NoDoubleQuotesStr = "referenced_table"
    ref_columns_field: NoDoubleQuotesStr = "referenced_columns"
    ref_name_field: NoDoubleQuotesStr = "reference_name"


class MetaParameterSettings(MetaSettings):
    """
    The settings for listing input/output parameters when describing a function of a
    script.
    """

    type_field: NoDoubleQuotesStr = "parameter_type"
    input_field: NoDoubleQuotesStr = "inputs"
    return_field: NoDoubleQuotesStr = "returns"
    emit_field: NoDoubleQuotesStr = "emits"


class McpServerSettings(BaseModel):
    """
    MCP server configuration.
    """

    schemas: MetaSettings = MetaSettings(
        name_field="schema_name", comment_field="schema_comment"
    )
    tables: MetaSettings = MetaSettings(
        name_field="table_name", comment_field="table_comment"
    )
    views: MetaSettings = MetaSettings(
        enable=False, name_field="table_name", comment_field="table_comment"
    )
    functions: MetaSettings = MetaSettings(
        name_field="function_name", comment_field="function_comment"
    )
    scripts: MetaSettings = MetaSettings(
        name_field="script_name", comment_field="script_comment"
    )
    columns: MetaColumnSettings = MetaColumnSettings(
        name_field="column_name", comment_field="column_comment"
    )
    parameters: MetaParameterSettings = MetaParameterSettings(
        name_field="parameter_name"
    )
    enable_query: bool = True
