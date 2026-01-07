from typing import Any
import pytest
import exasol.bucketfs as bfs

from exasol.ai.mcp.server.bucketfs_tools import (
    BucketFsTools, PATH_FIELD, NAME_FIELD
)
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)
from test.utils.db_objects import ExaBfsObject, ExaBfsDir, ExaBfsFile


def _get_sorted_result(res: ExaDbResult) -> list[dict[str, Any]]:
    return sorted(res.result, key=lambda d: d[PATH_FIELD])


def _get_expected_result(items: dict[str, ExaBfsObject]) -> list[dict[str, Any]]:
    res = [
        {
            PATH_FIELD: path,
            NAME_FIELD: item.name,
        }
        for path, item in items.items()
    ]
    return _get_sorted_result(ExaDbResult(res))


@pytest.fixture
def bucketfs_tools(backend_aware_bucketfs_params, setup_bucketfs):
    bfs_root = bfs.path.build_path(**backend_aware_bucketfs_params)
    return BucketFsTools(bfs_root, McpServerSettings())


def test_list_directories(bucketfs_tools, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsDir):
            path = f"{bfs_data.name}/{item.name}"
            result = bucketfs_tools.list_directories(path)
            sorted_result = _get_sorted_result(result)
            expected_nodes = {
                f"{path}/{sub_item.name}": sub_item
                for sub_item in item.items if isinstance(sub_item, ExaBfsDir)
            }
            expected_result = _get_expected_result(expected_nodes)
            assert sorted_result == expected_result


def test_list_files(bucketfs_tools, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsDir):
            path = f"{bfs_data.name}/{item.name}"
            result = bucketfs_tools.list_files(path)
            sorted_result = _get_sorted_result(result)
            expected_nodes = {
                f"{path}/{sub_item.name}": sub_item
                for sub_item in item.items if isinstance(sub_item, ExaBfsFile)
            }
            expected_result = _get_expected_result(expected_nodes)
            assert sorted_result == expected_result


def test_list_not_in_directory(bucketfs_tools, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsFile):
            path = f"{bfs_data.name}/{item.name}"
            with pytest.raises(NotADirectoryError):
                bucketfs_tools.list_directories(path)
            with pytest.raises(NotADirectoryError):
                bucketfs_tools.list_files(path)


def test_list_in_nowhere(bucketfs_tools, bfs_data) -> None:
    path = f"{bfs_data.name}/Unicorn"
    with pytest.raises(FileNotFoundError):
        bucketfs_tools.list_directories(path)
    with pytest.raises(FileNotFoundError):
        bucketfs_tools.list_files(path)


@pytest.mark.parametrize("path", ["Species/Carnivores", "Species", ""])
def test_find_files(bucketfs_tools, bfs_data, path) -> None:
    keywords = ["cat"]
    result = bucketfs_tools.find_files(keywords, path)
    sorted_result = _get_sorted_result(result)
    expected_nodes = bfs_data.find_descendants(["Cougar", "Bobcat"])
    expected_result = _get_expected_result(expected_nodes)
    assert sorted_result == expected_result
