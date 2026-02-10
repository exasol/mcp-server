import importlib.resources
import json
from functools import cache
from typing import (
    Annotated,
    Any,
)

from pydantic import Field

from exasol.ai.mcp.server.server_settings import ExaDbResult

BUILTIN_FUNCTIONS_JSON = "exasol_builtin_functions.json"
PACKAGE_RESOURCES = f"{__package__}.resources"


@cache
def load_builtin_func_list() -> list[dict[str, Any]]:
    with importlib.resources.open_text(PACKAGE_RESOURCES, BUILTIN_FUNCTIONS_JSON) as f:
        return json.load(f)


def builtin_function_categories() -> list[str]:
    """
    Returns a list of builtin function categories.
    """
    func_list = load_builtin_func_list()
    categories: set[str] = set()
    for func_info in func_list:
        categories.update(func_info["types"])
    return sorted(categories)


def list_builtin_functions(
    category: Annotated[str, Field(description="builtin function category")],
) -> list[str]:
    """
    Selects the list of builtin functions of the specified type (category), reading
    the resource json. Returns only the function names.
    """
    func_list = load_builtin_func_list()
    category = category.lower()
    return [
        func_info["name"] for func_info in func_list if category in func_info["types"]
    ]


def describe_builtin_function(
    name: Annotated[str, Field(description="builtin function name")],
) -> ExaDbResult:
    """
    Loads details for the specified builtin function, reading the resource json.
    Returns all fields. Some functions, for example TO_CHAR, can have information in
    more than one row.
    """
    func_list = load_builtin_func_list()
    name = name.upper()
    selected_info = [func_info for func_info in func_list if func_info["name"] == name]
    return ExaDbResult(selected_info)
