from typing import Any
import time
import pytest
import exasol.bucketfs as bfs

from exasol.ai.mcp.server.bucket_fs_tools import (
    BucketFsTools, PATH_FIELD, NAME_FIELD, IS_DIR_FIELD, IS_FILE_FIELD
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
            IS_DIR_FIELD: isinstance(item, ExaBfsDir),
            IS_FILE_FIELD: isinstance(item, ExaBfsFile),
        }
        for path, item in items.items()
    ]
    return _get_sorted_result(ExaDbResult(res))


@pytest.fixture
def bucket_fs_tools(backend_aware_bucketfs_params, setup_bucketfs):
    bfs_root = bfs.path.build_path(**backend_aware_bucketfs_params)
    return BucketFsTools(bfs_root, McpServerSettings())


def test_list_items(bucket_fs_tools, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsDir):
            path = f"{bfs_data.name}/{item.name}"
            result = bucket_fs_tools.list_items(path)
            sorted_result = _get_sorted_result(result)
            expected_nodes = {
                f"{path}/{sub_item.name}": sub_item for sub_item in item.items
            }
            expected_result = _get_expected_result(expected_nodes)
            assert sorted_result == expected_result


def test_list_items_not_directory(bucket_fs_tools, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsFile):
            path = f"{bfs_data.name}/{item.name}"
            with pytest.raises(NotADirectoryError):
                bucket_fs_tools.list_items(path)


def test_list_items_not_found(bucket_fs_tools, bfs_data) -> None:
    path = f"{bfs_data.name}/Unicorn"
    with pytest.raises(FileNotFoundError):
        bucket_fs_tools.list_items(path)


@pytest.mark.parametrize("path", ["Species/Carnivores", "Species", ""])
def test_find_items(bucket_fs_tools, bfs_data, path) -> None:
    keywords = ["cat"]
    result = bucket_fs_tools.find_items(keywords, path)
    sorted_result = _get_sorted_result(result)
    expected_nodes: dict[str, ExaBfsObject] = {}
    for name in ["Cat", "Cougar", "Bobcat"]:
        expected_nodes.update(bfs_data.find_descendants(name, bfs_data.name))
    expected_result = _get_expected_result(expected_nodes)
    assert sorted_result == expected_result


def test_read_file(bucket_fs_tools, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsDir):
            dir_path = f"{bfs_data.name}/{item.name}"
            for sub_item in item.items:
                if isinstance(sub_item, ExaBfsFile):
                    file_path = f"{dir_path}/{sub_item.name}"
                    content = bucket_fs_tools.read_file(file_path)
                    assert content == str(sub_item.content, encoding="utf-8")


def test_write_file(bucket_fs_tools) -> None:
    content = (
        "A human (Homo sapiens) is a bipedal primate species characterized by a large "
        "brain, high cognitive capacity, and complex culture. Morphologically, it is "
        "defined by a unique combination of traits including a high and rounded skull, "
        "reduced jaw and teeth, and a fully opposable thumb. Genetically, it is a "
        "distinct lineage within the hominid family, with a genome that demonstrates "
        "close relation to other Homo species (now extinct) and more distant relation "
        "to extant great apes."
    )
    path = "primates/human"
    bucket_fs_tools.write_file(path, content)
    assert bucket_fs_tools.read_file(path) == content


def test_download_file(bucket_fs_tools) -> None:
    path = "download_test/README"
    url = "https://raw.githubusercontent.com/octocat/Hello-World/master/README"
    bucket_fs_tools.download_file(path, url)
    time.sleep(5)
    full_path = self.bfs_location.joinpath(path)
    assert full_path.exits()
    assert full_path.is_file()
    assert bucket_fs_tools.read_file(path).strip() == "Hello World!"
