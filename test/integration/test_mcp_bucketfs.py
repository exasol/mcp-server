import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from test.utils.db_objects import (
    ExaBfsDir,
    ExaBfsFile,
    ExaBfsObject,
)
from test.utils.result_utils import (
    get_list_result_json,
    result_sort_func,
)
from unittest.mock import (
    create_autospec,
    patch,
)

import exasol.bucketfs as bfs
import pyexasol
import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from exasol.ai.mcp.server.bucketfs_tools import PATH_FIELD
from exasol.ai.mcp.server.connection_factory import env_to_bucketfs
from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import (
    create_mcp_server,
    mcp_server,
)
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)


async def _run_tool_async(
    bucketfs_location: bfs.path.PathLike, tool_name: str, **kwargs
):
    @contextmanager
    def connection_factory() -> Generator[pyexasol.ExaConnection, None, None]:
        yield create_autospec(pyexasol.ExaConnection)

    db_connection = DbConnection(connection_factory, num_retries=1)

    config = McpServerSettings(enable_read_bucketfs=True, enable_write_bucketfs=True)
    exa_server = create_mcp_server(db_connection, config, bucketfs_location)
    async with Client(exa_server) as client:
        return await client.call_tool(tool_name, kwargs)


def _run_tool(bucketfs_location: bfs.path.PathLike, tool_name: str, **kwargs):
    return asyncio.run(_run_tool_async(bucketfs_location, tool_name, **kwargs))


def _get_expected_list_json(items: dict[str, ExaBfsObject]) -> ExaDbResult:
    expected_json = [{PATH_FIELD: path} for path, item in items.items()]
    return ExaDbResult(sorted(expected_json, key=result_sort_func))


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


@pytest.fixture(scope="session")
def bucketfs_location(backend_aware_bucketfs_params, setup_bucketfs):
    return bfs.path.build_path(**backend_aware_bucketfs_params)


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


def test_list_directories(bucketfs_location, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsDir):
            path = f"{bfs_data.name}/{item.name}"
            result = _run_tool(bucketfs_location, "list_directories", directory=path)
            result_json = get_list_result_json(result)
            expected_nodes = {
                f"{path}/{sub_item.name}": sub_item
                for sub_item in item.items
                if isinstance(sub_item, ExaBfsDir)
            }
            expected_json = _get_expected_list_json(expected_nodes)
            assert result_json == expected_json


def test_list_files(bucketfs_location, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsDir):
            path = f"{bfs_data.name}/{item.name}"
            result = _run_tool(bucketfs_location, "list_files", directory=path)
            result_json = get_list_result_json(result)
            expected_nodes = {
                f"{path}/{sub_item.name}": sub_item
                for sub_item in item.items
                if isinstance(sub_item, ExaBfsFile)
            }
            expected_json = _get_expected_list_json(expected_nodes)
            assert result_json == expected_json


def test_list_not_in_directory(bucketfs_location, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsFile):
            path = f"{bfs_data.name}/{item.name}"
            with pytest.raises(ToolError):
                _run_tool(bucketfs_location, "list_directories", directory=path)
            with pytest.raises(ToolError):
                _run_tool(bucketfs_location, "list_files", directory=path)


def test_list_in_nowhere(bucketfs_location, bfs_data) -> None:
    path = f"{bfs_data.name}/Unicorn"
    with pytest.raises(ToolError):
        _run_tool(bucketfs_location, "list_directories", directory=path)
    with pytest.raises(ToolError):
        _run_tool(bucketfs_location, "list_files", directory=path)


@pytest.mark.parametrize("path", ["Species/Carnivores", "Species", ""])
def test_find_files(bucketfs_location, bfs_data, path) -> None:
    keywords = ["cat"]
    result = _run_tool(
        bucketfs_location, "find_files", keywords=keywords, directory=path
    )
    result_json = get_list_result_json(result)
    expected_nodes = bfs_data.find_descendants(["Cougar", "Bobcat"])
    expected_json = _get_expected_list_json(expected_nodes)
    assert result_json == expected_json
