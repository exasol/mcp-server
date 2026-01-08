from typing import Any
from contextlib import contextmanager
from unittest.mock import patch

import pytest
import exasol.bucketfs as bfs

from exasol.ai.mcp.server.connection_factory import env_to_bucketfs
from exasol.ai.mcp.server.main import mcp_server
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)
from exasol.ai.mcp.server.bucketfs_tools import (
    BucketFsTools, PATH_FIELD, NAME_FIELD
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
def bucketfs_params_env(backend_aware_bucketfs_params, monkeypatch) -> None:
    """
    Stores the backend_aware_bucketfs_params into the environment variables.
    """
    bucketfs_to_env = {v: k for k, v in env_to_bucketfs.items()}
    backend = backend_aware_bucketfs_params["backend"]
    for k, v in backend_aware_bucketfs_params.items():
        # The parameter name can be disambiguated with appended backend name.
        env_name = bucketfs_to_env.get(k) or bucketfs_to_env.get(f"{k}|{backend}")
        if env_name:
            monkeypatch.setenv(env_name, str(v))


@pytest.fixture
def bucketfs_tools(backend_aware_bucketfs_params, setup_bucketfs):
    bfs_root = bfs.path.build_path(**backend_aware_bucketfs_params)
    return BucketFsTools(bfs_root, McpServerSettings())


@pytest.mark.parametrize("enable_bucketfs", [False, True])
@patch("exasol.ai.mcp.server.main.get_mcp_settings")
@patch("exasol.ai.mcp.server.connection_factory.get_connection_factory")
def test_mcp_server_with_bucketfs(
    mock_get_conn_factory,
    mock_get_mcp_settings,
    enable_bucketfs,
    pyexasol_connection,
    bucketfs_params_env,
) -> None:
    """
    Verifies that if BucketFS tools are enabled the server gets a valid PathLike object
    pointing to the root of the BucketFS bucket. Otherwise, even if the BucketFS access
    is configured, the object is None.
    """

    @contextmanager
    def connection_factory():
        yield pyexasol_connection

    mock_get_conn_factory.return_value = connection_factory
    mock_get_mcp_settings.return_value = McpServerSettings(
        enable_read_bucketfs=enable_bucketfs
    )
    server = mcp_server()
    assert (server.bucketfs_tools is not None) == enable_bucketfs


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
