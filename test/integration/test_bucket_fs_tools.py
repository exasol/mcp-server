from typing import Any
from pathlib import Path
import pytest
import exasol.bucketfs as bfs

from exasol.ai.mcp.server.bucket_fs_tools import (
    BucketFsTools, PATH_FIELD, NAME_FIELD, IS_DIR_FIELD, IS_FILE_FIELD
)
from exasol.ai.mcp.server.server_settings import ExaDbResult
from test.utils.db_objects import ExaBfsObject, ExaBfsDir, ExaBfsFile


def _get_sorted_result(res: ExaDbResult) -> list[dict[str, Any]]:
    return sorted(res.result, key=lambda d: d[PATH_FIELD])


def _get_expected_result(pth: Path, lst: list[ExaBfsObject]) -> list[dict[str, Any]]:
    res = [
        {
            PATH_FIELD: pth / item.name,
            NAME_FIELD: pth.name,
            IS_DIR_FIELD: isinstance(item, ExaBfsDir),
            IS_FILE_FIELD: isinstance(item, ExaBfsFile),
        }
        for item in lst
    ]
    return _get_sorted_result(ExaDbResult(res))


@pytest.fixture
def bucket_fs_tools(backend_aware_bucketfs_params):
    bfs_root = bfs.path.build_path(**backend_aware_bucketfs_params)
    return BucketFsTools(bfs_root)


def test_list(bucket_fs_tools, bfs_data) -> None:
    result = bucket_fs_tools.list("Rodents")
    sorted_result = _get_sorted_result(result)
    expected_result = _get_expected_result(Path("Rodents"), bfs_data.items[2])
    assert sorted_result == expected_result
