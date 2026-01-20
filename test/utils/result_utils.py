import json
from typing import (
    Any,
    cast,
)

from exasol.ai.mcp.server.server_settings import ExaDbResult


def result_sort_func(d: Any) -> str:
    if isinstance(d, dict):
        return ",".join(str(d[key]) for key in sorted(d.keys()))
    return str(d)


def get_result_content(result) -> str:
    return result.content[0].text


def get_result_json(result, content_extractor=get_result_content) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(content_extractor(result)))


def get_sort_result_json(
    result, content_extractor=get_result_content
) -> dict[str, Any]:
    result_json = get_result_json(result, content_extractor)
    return {
        key: sorted(val, key=result_sort_func) if isinstance(val, list) else val
        for key, val in result_json.items()
    }


def get_list_result_json(result, content_extractor=get_result_content) -> ExaDbResult:
    result_json = get_result_json(result, content_extractor)
    unsorted = ExaDbResult(**result_json)
    if isinstance(unsorted.result, list):
        return ExaDbResult(sorted(unsorted.result, key=result_sort_func))
    return unsorted
