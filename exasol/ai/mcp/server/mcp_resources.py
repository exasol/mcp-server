import importlib.resources
import json
from typing import Any

from exasol.ai.mcp.server.server_settings import ExaDbResult

BUILTIN_FUNCTIONS_JSON = "exasol_builtin_functions.json"
PACKAGE_RESOURCES = f"{__package__}.resources"


def list_builtin_functions(category: str) -> ExaDbResult:
    """
    Selects the list of builtin functions of the specified type (category), reading
    the resource json. Only takes the name and the description fields.
    """
    with importlib.resources.open_text(PACKAGE_RESOURCES, BUILTIN_FUNCTIONS_JSON) as f:
        func_list: list[dict[str, Any]] = json.load(f)
    allowed_fields = ["name", "description"]
    category = category.lower()
    selected_info = [
        {
            field_name: field_value
            for field_name, field_value in func_info.items()
            if field_name in allowed_fields
        }
        for func_info in func_list
        if category in func_info["types"]
    ]
    return ExaDbResult(selected_info)


def describe_builtin_function(name: str) -> ExaDbResult:
    """
    Loads details for the specified builtin function, reading the resource json.
    Returns all fields. Some functions, for example TO_CHAR, can have information in
    more than one row.
    """
    with importlib.resources.open_text(PACKAGE_RESOURCES, BUILTIN_FUNCTIONS_JSON) as f:
        func_list: list[dict[str, Any]] = json.load(f)
    name = name.upper()
    selected_info = list(filter(lambda func_info: func_info["name"] == name, func_list))
    return ExaDbResult(selected_info)
