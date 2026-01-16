import asyncio
from collections.abc import (
    ByteString,
    Generator,
)
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
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from exasol.ai.mcp.server.bucketfs_tools import (
    PATH_FIELD,
    PathStatus,
    get_path_warning,
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
    path_status: PathStatus
    action: str | None
    data: dict[str, str]


@dataclass
class ModifyTestCase:
    path: str
    elicitations: list[ElicitationData]
    content: str = ""

    def _expected_value(self, init_value, value_name: str) -> str:
        """
        Gets the expected final value of an element with the given name, that can be
        changed in elicitation.
        """
        elicit_value = init_value
        for elicit in self.elicitations:
            if value_name in elicit.data:
                elicit_value = elicit.data[value_name]
        return elicit_value

    @property
    def expected_path(self) -> str:
        return self._expected_value(self.path, "file_path")

    @property
    def expected_content(self) -> str:
        return self._expected_value(self.content, "file_content")


async def _run_tool_async(
    bucketfs_location: bfs.path.PathLike,
    tool_name: str,
    elicitation: list[ElicitationData] | None,
    expected_status: PathStatus | None,
    **kwargs,
):
    elicit_count = 0

    @contextmanager
    def connection_factory() -> Generator[pyexasol.ExaConnection, None, None]:
        yield create_autospec(pyexasol.ExaConnection)

    async def elicitation_handler(message: str, response_type: type, params, context):
        nonlocal elicit_count
        current_elicitation = elicitation[elicit_count]
        # Verify the presence of the correct warning in the elicitation message.
        for status in PathStatus:
            warning = get_path_warning(status, expected_status)
            if warning:
                assert (warning in message) == (
                    status == current_elicitation.path_status
                )
        action = current_elicitation.action
        response_data = response_type(**current_elicitation.data)
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
    expected_status: PathStatus | None = None,
    **kwargs,
):
    return asyncio.run(
        _run_tool_async(
            bucketfs_location, tool_name, elicitation, expected_status, **kwargs
        )
    )


def _get_expected_list_json(items: dict[str, ExaBfsObject]) -> ExaDbResult:
    expected_json = [{PATH_FIELD: path} for path, item in items.items()]
    return ExaDbResult(sorted(expected_json, key=result_sort_func))


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, min=1, max=15),
    stop=stop_after_attempt(7),
)
def _restore_file(bfs_file: bfs.path.PathLike, byte_content: ByteString | None) -> None:
    bfs_file.write(byte_content)


@contextmanager
def tmp_path_write(bfs_path: bfs.path.PathLike):
    """
    Allows to test creating a file or deleting a file or directory at the specified
    location, with subsequent restoration of the pre-existing file structure.
    """

    controlled_files: list[tuple[bfs.path.PathLike, ByteString]] = []
    if bfs_path.is_dir():
        for bfs_dir, sub_dirs, files in bfs_path.walk():
            for file in files:
                bfs_file = bfs_dir.joinpath(file)
                byte_content = b"".join(bfs_file.read())
                controlled_files.append((bfs_file, byte_content))
    elif bfs_path.is_file():
        byte_content = b"".join(bfs_path.read())
        controlled_files.append((bfs_path, byte_content))

    try:
        yield
    finally:
        for bfs_file, byte_content in controlled_files:
            _restore_file(bfs_file, byte_content)
        if not controlled_files:
            bfs_path.rm()


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
    file_path = "Species/Rodents/Squirrel/Eastern_Gray_Squirrel"
    item = next(iter(bfs_data.find_descendants(["Eastern_Gray_Squirrel"]).values()))
    assert isinstance(item, ExaBfsFile)
    result = _run_tool(bucketfs_location, "read_file", path=file_path)
    content = get_result_content(result)
    assert content == str(item.content, encoding="utf-8")


@pytest.mark.parametrize(
    "test_case",
    [
        ModifyTestCase(
            # A simple case with one round of elicitation.
            path="Species/Primates/chimpanzee",
            content=_chimpanzee,
            elicitations=[
                ElicitationData(path_status=PathStatus.Vacant, action="accept", data={})
            ],
        ),
        ModifyTestCase(
            # Set to overwrite an existing file.
            path="Species/Even-toed_Ungulates/Deer/Elk",
            content=_chimpanzee,
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.FileExists, action="accept", data={}
                )
            ],
        ),
        ModifyTestCase(
            # Start with a path that doesn't exist, then, change it to another one
            # that does exist. The content is changed in the first elicitation.
            path="Species/Primates/human",
            content=_chimpanzee,
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.Vacant,
                    action="accept",
                    data={
                        "file_path": "Species/Rodents/Squirrel/Eastern_Gray_Squirrel",
                        "file_content": _human,
                    },
                ),
                ElicitationData(
                    path_status=PathStatus.FileExists,
                    action="accept",
                    data={"file_path": "Species/Primates/human"},
                ),
            ],
        ),
        ModifyTestCase(
            # Start with a bad file path, then correct it after the second reminder.
            path="Species/Primates/home:luminis",
            content=_home_luminis,
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={"file_path": "Species/Primates/home-luminis"},
                ),
            ],
        ),
        ModifyTestCase(
            # Start from an existing directory, then set it to an existing file.
            path="Species/Rodents/Squirrel",
            content=_chimpanzee,
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.DirExists,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.DirExists,
                    action="accept",
                    data={"file_path": "Species/Rodents/Squirrel/Eastern_Chipmunk"},
                ),
                ElicitationData(
                    path_status=PathStatus.FileExists,
                    action="accept",
                    data={"file_content": _home_luminis},
                ),
            ],
        ),
    ],
    ids=[
        "base case",
        "overwrite file",
        "two elicitations",
        "invalid path",
        "directory",
    ],
)
def test_write_text_to_file(bucketfs_location, test_case) -> None:
    with tmp_path_write(bucketfs_location.joinpath(test_case.expected_path)):
        _run_tool(
            bucketfs_location,
            "write_text_to_file",
            elicitation=test_case.elicitations,
            path=test_case.path,
            content=test_case.content,
        )
        result = _run_tool(bucketfs_location, "read_file", path=test_case.expected_path)
        content = get_result_content(result)
        assert content == test_case.expected_content


@pytest.mark.parametrize("action", ["decline", "cancel", None])
def test_write_text_to_file_not_accepted(bucketfs_location, action) -> None:
    """
    Verifies the case when the file writing is rejected in elicitation.
    """
    path = "Species/Primates/home_luminis"
    elicitation = [
        ElicitationData(path_status=PathStatus.Vacant, action=action, data={}),
    ]
    with pytest.raises(ToolError):
        _run_tool(
            bucketfs_location,
            "write_text_to_file",
            elicitation=elicitation,
            path=path,
            content=_home_luminis,
        )
    bfs_path = bucketfs_location.joinpath(path)
    assert not bfs_path.exists()


@pytest.mark.parametrize(
    "test_case",
    [
        ModifyTestCase(
            # A simple case with one round of elicitation.
            path="humanoids/home-luminis",
            elicitations=[
                ElicitationData(path_status=PathStatus.Vacant, action="accept", data={})
            ],
        ),
        ModifyTestCase(
            # Set to overwrite an existing file.
            path="Species/Even-toed_Ungulates/Deer/Elk",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.FileExists, action="accept", data={}
                )
            ],
        ),
        ModifyTestCase(
            # Change the path to avoid overwriting existing file.
            path="Species/Rodents/Squirrel/Eastern_Gray_Squirrel",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.FileExists,
                    action="accept",
                    data={
                        "file_path": "home-luminis",
                    },
                ),
            ],
        ),
        ModifyTestCase(
            # Start with a bad path, then correct it after the second reminder.
            path="humanoids/home:luminis",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={
                        "file_path": "humanoids2/home-luminis",
                    },
                ),
            ],
        ),
        ModifyTestCase(
            # Start with a path pointing to an existing directory. Then change it
            # to a non-existent path.
            path="Species/Rodents/Squirrel",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.DirExists,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.DirExists,
                    action="accept",
                    data={"file_path": "humanoids3/home-luminis"},
                ),
            ],
        ),
    ],
    ids=[
        "base case",
        "overwrite file",
        "change path",
        "invalid path",
        "directory",
    ],
)
def test_download_file(bucketfs_location, test_case, httpserver) -> None:
    url_path = "/fake_science"
    httpserver.expect_request(url_path).respond_with_data(_home_luminis)
    with tmp_path_write(bucketfs_location.joinpath(test_case.expected_path)):
        _run_tool(
            bucketfs_location,
            "download_file",
            elicitation=test_case.elicitations,
            url=httpserver.url_for(url_path),
            path=test_case.path,
        )
        result = _run_tool(bucketfs_location, "read_file", path=test_case.expected_path)
        content = get_result_content(result)
        assert content == _home_luminis


@pytest.mark.parametrize("action", ["decline", "cancel", None])
def test_download_file_not_accepted(bucketfs_location, action, httpserver) -> None:
    """
    Verifies the case when the file downloading is rejected in elicitation.
    """
    url_path = "/fake_science"
    httpserver.expect_request(url_path).respond_with_data(_home_luminis)
    path = "humanoids3/home-luminis"
    elicitation = [
        ElicitationData(path_status=PathStatus.Vacant, action=action, data={}),
    ]
    with pytest.raises(ToolError):
        _run_tool(
            bucketfs_location,
            "download_file",
            elicitation=elicitation,
            url=httpserver.url_for(url_path),
            path=path,
        )
    bfs_path = bucketfs_location.joinpath(path)
    assert not bfs_path.exists()


def test_download_file_invalid_url(bucketfs_location, httpserver) -> None:
    """
    Verifies the case when the provided url doesn't exist.
    """
    url_path = "/fake_science"
    httpserver.expect_request(url_path).respond_with_data(_home_luminis)
    path = "humanoids4/home-luminis"
    elicitation = [
        ElicitationData(path_status=PathStatus.Vacant, action="accept", data={}),
    ]
    with pytest.raises(ToolError):
        _run_tool(
            bucketfs_location,
            "download_file",
            elicitation=elicitation,
            url=httpserver.url_for("/true_science"),
            path=path,
        )
    bfs_path = bucketfs_location.joinpath(path)
    assert not bfs_path.exists()


@pytest.mark.parametrize(
    "test_case",
    [
        ModifyTestCase(
            # A simple case with one round of elicitation.
            path="Species/Carnivores/Cat/Cougar",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.FileExists, action="accept", data={}
                )
            ],
        ),
        ModifyTestCase(
            # Start with a path that doesn't exist, change it to an existing one.
            path="Species/unicorn",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.Vacant,
                    action="accept",
                    data={"file_path": "Species/Rodents/Squirrel/Eastern_Chipmunk"},
                ),
            ],
        ),
        ModifyTestCase(
            # Start with a bad file path, then correct it after the second reminder.
            path="Species/Even-toed_Ungulates/Deer:White-tailed_Deer",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={
                        "file_path": "Species/Even-toed_Ungulates/Deer/White-tailed_Deer"
                    },
                ),
            ],
        ),
        ModifyTestCase(
            # Try to delete an existing directory, then set the path to an existing file.
            path="Species/Even-toed_Ungulates",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.DirExists,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.DirExists,
                    action="accept",
                    data={"file_path": "Species/Even-toed_Ungulates/Deer/Elk"},
                ),
            ],
        ),
    ],
    ids=[
        "base case",
        "file does not exist",
        "invalid path",
        "directory",
    ],
)
def test_delete_file(bucketfs_location, test_case) -> None:
    abs_path = bucketfs_location.joinpath(test_case.expected_path)
    assert abs_path.exists()
    with tmp_path_write(abs_path):
        _run_tool(
            bucketfs_location,
            "delete_file",
            elicitation=test_case.elicitations,
            expected_status=PathStatus.FileExists,
            path=test_case.path,
        )
        assert not abs_path.exists()


@pytest.mark.parametrize("action", ["decline", "cancel", None])
def test_delete_file_not_accepted(bucketfs_location, action) -> None:
    """
    Verifies the case when the file deletion is rejected in elicitation.
    """
    path = "Species/Even-toed_Ungulates/Deer/Elk"
    elicitation = [
        ElicitationData(path_status=PathStatus.FileExists, action=action, data={}),
    ]
    with pytest.raises(ToolError):
        _run_tool(bucketfs_location, "delete_file", elicitation=elicitation, path=path)
    bfs_path = bucketfs_location.joinpath(path)
    assert bfs_path.exists()


@pytest.mark.parametrize(
    "test_case",
    [
        ModifyTestCase(
            # A simple case with one round of elicitation.
            path="Species/Carnivores/",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.DirExists, action="accept", data={}
                )
            ],
        ),
        ModifyTestCase(
            # Start with a path that doesn't exist, change it to an existing one.
            path="Species/Unicorns",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.Vacant,
                    action="accept",
                    data={"file_path": "Species/Rodents"},
                ),
            ],
        ),
        ModifyTestCase(
            # Start with a bad path, then correct it after the second reminder.
            path="Species/Carnivores:Dog",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.Invalid,
                    action="accept",
                    data={"file_path": "Species/Carnivores/Dog"},
                ),
            ],
        ),
        ModifyTestCase(
            # Try to delete an existing file, then set the path to an existing
            # directory.
            path="Species/Carnivores/Dog/Gray_Fox",
            elicitations=[
                ElicitationData(
                    path_status=PathStatus.FileExists,
                    action="accept",
                    data={},
                ),
                ElicitationData(
                    path_status=PathStatus.FileExists,
                    action="accept",
                    data={"file_path": "Species/Carnivores/Dog"},
                ),
            ],
        ),
    ],
    ids=[
        "base case",
        "directory does not exist",
        "invalid path",
        "file",
    ],
)
def test_delete_directory(bucketfs_location, test_case) -> None:
    abs_path = bucketfs_location.joinpath(test_case.expected_path)
    assert abs_path.exists()
    with tmp_path_write(abs_path):
        _run_tool(
            bucketfs_location,
            "delete_directory",
            elicitation=test_case.elicitations,
            expected_status=PathStatus.DirExists,
            path=test_case.path,
        )
        assert not abs_path.exists()


@pytest.mark.parametrize("action", ["decline", "cancel", None])
def test_delete_directory_not_accepted(bucketfs_location, action) -> None:
    """
    Verifies the case when the directory deletion is rejected in elicitation.
    """
    path = "Species/Even-toed_Ungulates"
    elicitation = [
        ElicitationData(path_status=PathStatus.DirExists, action=action, data={}),
    ]
    with pytest.raises(ToolError):
        _run_tool(
            bucketfs_location, "delete_directory", elicitation=elicitation, path=path
        )
    bfs_path = bucketfs_location.joinpath(path)
    assert bfs_path.exists()
