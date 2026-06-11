import warnings
from typing import Any

from pydantic import (
    BaseModel,
    model_validator,
)

_DEPRECATED_FIELD_SETTINGS = frozenset(
    [
        "schema_field",
        "name_field",
        "comment_field",
        "type_field",
        "constraint_name_field",
        "constraint_type_field",
        "constraint_columns_field",
        "referenced_schema_field",
        "referenced_table_field",
        "referenced_columns_field",
        "columns_field",
        "constraints_field",
        "table_comment_field",
        "usage_field",
        "input_field",
        "return_field",
        "emit_field",
    ]
)


class MetaSettings(BaseModel):
    """
    The general settings for a single type of metadata, e.g. tables.
    """

    enable: bool = True
    """
    Allows to disable the listing of a particular type of metadata.
    """

    @model_validator(mode="before")
    @classmethod
    def _warn_deprecated_field_settings(cls, data: Any) -> Any:
        if isinstance(data, dict):
            found = _DEPRECATED_FIELD_SETTINGS.intersection(data)
            if found:
                names = ", ".join(sorted(found))
                warnings.warn(
                    f"The following settings have no effect and will be removed in a "
                    f"future version: {names}. Output field names are fixed and cannot "
                    f"be configured.",
                    DeprecationWarning,
                    stacklevel=2,
                )
        return data


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


class McpServerSettings(BaseModel):
    """
    MCP server configuration.
    """

    schemas: MetaListSettings = MetaListSettings()
    tables: MetaListSettings = MetaListSettings()
    views: MetaListSettings = MetaListSettings(enable=False)
    functions: MetaListSettings = MetaListSettings()
    scripts: MetaListSettings = MetaListSettings()
    columns: MetaSettings = MetaSettings()
    parameters: MetaSettings = MetaSettings()

    enable_read_query: bool = False
    enable_write_query: bool = False
    enable_summarize_table: bool = False
    enable_query_profiling: bool = False
    enable_preprocessor_tools: bool = True
    enable_read_bucketfs: bool = False
    enable_write_bucketfs: bool = False
    disable_elicitation: bool = False

    language: str = ""
    """
    An optional language of communication with the LLM, e.g. 'english'. It must be
    the same language that was used for naming and documenting the database objects.
    """

    case_sensitive: bool = False
    """
    This setting effects the selection of database objects in the tools that are looking
    for a particular object or a collection of objects. Depending on this setting, the
    parameters provided to these tools, e.g. a schema name, are used in case sensitive
    or case insensitive way.
    """
