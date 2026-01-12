import asyncio
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from test.utils.db_objects import (
    ExaBfsDir,
    ExaBfsFile,
    ExaBfsObject,
)
from test.utils.result_utils import (
    get_list_result_json,
    get_result_content,
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
from fastmcp.client.elicitation import ElicitResult
from fastmcp.exceptions import ToolError

from exasol.ai.mcp.server.bucketfs_tools import (
    OVERWRITE_WARNING,
    PATH_FIELD,
)
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

_human = (
    "A human (Homo sapiens) is a bipedal primate species characterized by a large "
    "brain, high cognitive capacity, and complex culture. Morphologically, it is "
    "defined by a unique combination of traits including a high and rounded skull, "
    "reduced jaw and teeth, and a fully opposable thumb. Genetically, it is a "
    "distinct lineage within the hominid family, with a genome that demonstrates "
    "close relation to other Homo species (now extinct) and more distant relation "
    "to extant great apes."
)

_chimpanzee = (
    "Great ape, closest living relative to humans. Robust, covered in coarse black "
    "hair (face, palms, soles bare). Arms longer than legs. Pronounced brow ridge, "
    "large ears."
)

_home_luminis = (
    "These robust hominins possessed elongated, chisel-like fingernails of dense "
    "keratin, which they used to strip bark and dig for subterranean tubers. Their "
    "most distinctive feature was a network of bioluminescent patches along the "
    "shoulders and neck, likely used for social signaling in the dim forests they "
    "called home."
)


@dataclass
class ElicitationData:
    warning: bool
    action: str | None
    data: dict[str, str]


@dataclass
class WriteTestCase:
    path: str
    content: str
    elicitation: list[ElicitationData]

    def _expected_value(self, init_value, value_name: str) -> str:
        x_value = init_value
        for ed in self.elicitation:
            if value_name in ed.data:
                x_value = ed.data[value_name]
        return x_value

    @property
    def expected_path(self) -> str:
        return self._expected_value(self.path, "file_path")

    @property
    def expected_content(self) -> str:
        return self._expected_value(self.content, "file_content")


async def _run_tool_async(
    bucketfs_location: bfs.path.PathLike,
    tool_name: str,
    elicitation: list[ElicitationData] | None = None,
    **kwargs,
):
    elicit_count = 0

    @contextmanager
    def connection_factory() -> Generator[pyexasol.ExaConnection, None, None]:
        yield create_autospec(pyexasol.ExaConnection)

    async def elicitation_handler(message: str, response_type: type, params, context):
        nonlocal elicit_count
        elicitation_i = elicitation[elicit_count]
        assert (OVERWRITE_WARNING in message) == elicitation_i.warning
        action = elicitation_i.action
        response_data = response_type(**elicitation_i.data)
        elicit_count += 1
        return ElicitResult(action=action, content=response_data)

    db_connection = DbConnection(connection_factory, num_retries=1)

    config = McpServerSettings(enable_read_bucketfs=True, enable_write_bucketfs=True)
    exa_server = create_mcp_server(db_connection, config, bucketfs_location)
    elicit_handler = elicitation_handler if elicitation is not None else None
    async with Client(exa_server, elicitation_handler=elicit_handler) as client:
        return await client.call_tool(tool_name, kwargs)


def _run_tool(
    bucketfs_location: bfs.path.PathLike,
    tool_name: str,
    elicitation: list[ElicitationData] | None = None,
    **kwargs,
):
    return asyncio.run(
        _run_tool_async(bucketfs_location, tool_name, elicitation, **kwargs)
    )


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
    """
    Will delete this fixture from here. once it becomes available in the pytest-plugin.
    """
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


def test_list_directories_root(bucketfs_location, bfs_data) -> None:
    result = _run_tool(bucketfs_location, "list_directories")
    result_json = get_list_result_json(result)
    expected_nodes = {bfs_data.name: bfs_data}
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


def test_list_files_root(bucketfs_location) -> None:
    result = _run_tool(bucketfs_location, "list_files")
    result_json = get_list_result_json(result)
    expected_json = _get_expected_list_json({})
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


def test_read_file(bucketfs_location, bfs_data) -> None:
    for item in bfs_data.items:
        if isinstance(item, ExaBfsDir):
            dir_path = f"{bfs_data.name}/{item.name}"
            for sub_item in item.items:
                if isinstance(sub_item, ExaBfsFile):
                    file_path = f"{dir_path}/{sub_item.name}"
                    result = _run_tool(bucketfs_location, "read_file", path=file_path)
                    content = get_result_content(result)
                    assert content == str(sub_item.content, encoding="utf-8")


@pytest.mark.parametrize(
    "test_case",
    [
        WriteTestCase(
            # A simple case with one round of elicitation.
            path="Species/Primates/chimpanzee",
            content=_chimpanzee,
            elicitation=[ElicitationData(warning=False, action="accept", data={})],
        ),
        WriteTestCase(
            # First suggests a path that doesn't exist,
            # then, in the elicitation, changes it to another one that does exist.
            # This should cause another elicitation, where it changes it again.
            # The content is changed in the first elicitation.
            path="Species/Primates/human",
            content=_chimpanzee,
            elicitation=[
                ElicitationData(
                    warning=False,
                    action="accept",
                    data={
                        "file_path": "Species/Rodents/Squirrel",
                        "file_content": _human,
                    },
                ),
                ElicitationData(
                    warning=True,
                    action="accept",
                    data={"file_path": "Species/Primates/human"},
                ),
            ],
        ),
    ],
    ids=["one elicitation", "two elicitations"],
)
def test_write_file(bucketfs_location, test_case) -> None:
    _run_tool(
        bucketfs_location,
        "write_file",
        elicitation=test_case.elicitation,
        path=test_case.path,
        content=test_case.content,
    )
    try:
        result = _run_tool(bucketfs_location, "read_file", path=test_case.expected_path)
        content = get_result_content(result)
        assert content == test_case.expected_content
    finally:
        expected_bfs_path = bucketfs_location.joinpath(test_case.expected_path)
        expected_bfs_path.rm()


@pytest.mark.parametrize("action", ["decline", "cancel", None])
def test_write_file_not_accepted(bucketfs_location, action) -> None:
    path = "Species/Primates/home_luminis"
    elicitation = [
        ElicitationData(warning=False, action=action, data={}),
    ]
    with pytest.raises(ToolError):
        _run_tool(
            bucketfs_location,
            "write_file",
            elicitation=elicitation,
            path=path,
            content=_home_luminis,
        )
    bfs_path = bucketfs_location.joinpath(path)
    assert not bfs_path.exists()


@pytest.mark.parametrize(
    "test_case",
    [
        WriteTestCase(
            path="download_test/README",
            content="",
            elicitation=[ElicitationData(warning=False, action="accept", data={})],
        ),
        WriteTestCase(
            path="Species/Rodents/Squirrel",
            content="",
            elicitation=[
                ElicitationData(
                    warning=True,
                    action="accept",
                    data={
                        "file_path": "README",
                    },
                ),
            ],
        ),
    ],
    ids=["accept path", "change path"],
)
def test_download_file(bucketfs_location, test_case) -> None:
    _run_tool(
        bucketfs_location,
        "download_file",
        elicitation=test_case.elicitation,
        url="https://raw.githubusercontent.com/octocat/Hello-World/master/README",
        path=test_case.path,
    )
    time.sleep(5)
    try:
        result = _run_tool(bucketfs_location, "read_file", path=test_case.expected_path)
        content = get_result_content(result)
        assert content.strip() == "Hello World!"
    finally:
        expected_bfs_path = bucketfs_location.joinpath(test_case.expected_path)
        expected_bfs_path.rm()


@pytest.mark.parametrize("action", ["decline", "cancel", None])
def test_download_file_not_accepted(bucketfs_location, action) -> None:
    path = "download_test_rejected/README"
    elicitation = [
        ElicitationData(warning=False, action=action, data={}),
    ]
    with pytest.raises(ToolError):
        _run_tool(
            bucketfs_location,
            "download_file",
            elicitation=elicitation,
            url="https://raw.githubusercontent.com/octocat/Hello-World/master/README",
            path=path,
        )
    bfs_path = bucketfs_location.joinpath(path)
    assert not bfs_path.exists()
