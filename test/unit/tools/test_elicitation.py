import asyncio
import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from mcp.types import (
    ElicitResult,
    InputRequiredResult,
    ListRootsResult,
)

from exasol.ai.mcp.server.tools.bucketfs_tools import (
    PATH_WARNINGS,
    BucketFsTools,
    PathStatus,
)
from exasol.ai.mcp.server.tools.elicitation import (
    ELICITATION_KEY,
    is_input_required_era,
    raise_if_not_accepted,
    read_input,
    read_request_state,
    request_input,
)
from exasol.ai.mcp.server.tools.mcp_server import (
    WRITE_QUERY_MESSAGE,
    ExasolMCPServer,
    create_query_elicitation_type,
)

MODERN_PROTOCOL = "2026-07-28"
LEGACY_PROTOCOL = "2025-11-25"


def _make_ctx(
    response: ElicitResult | None = None,
    state: dict[str, Any] | None = None,
    protocol_version: str = MODERN_PROTOCOL,
) -> MagicMock:
    """
    Creates a mock MCP context for a guard pattern round. Without a response, this
    is the first round. A later round comes with the request state, which is empty
    unless specified. The input responses and the request state must be set
    explicitly, since a MagicMock attribute is not None.
    """
    ctx = MagicMock()
    ctx.request_context.protocol_version = protocol_version
    if response is None:
        ctx.input_responses = None
        ctx.request_state = None if state is None else json.dumps(state)
    else:
        ctx.input_responses = {ELICITATION_KEY: response}
        ctx.request_state = json.dumps(state or {})
    return ctx


def _get_elicit_params(result: InputRequiredResult):
    return result.input_requests[ELICITATION_KEY].params


def test_raise_if_not_accepted_accept() -> None:
    raise_if_not_accepted("accept", "The operation")


@pytest.mark.parametrize(
    ["action", "expected_message"],
    [
        ("decline", "The operation is declined by the user."),
        ("cancel", "The operation is cancelled by the user."),
    ],
)
def test_raise_if_not_accepted_interrupts(action, expected_message) -> None:
    with pytest.raises(InterruptedError, match=expected_message):
        raise_if_not_accepted(action, "The operation")


@pytest.mark.parametrize(
    ["protocol_version", "expected_result"],
    [(MODERN_PROTOCOL, True), (LEGACY_PROTOCOL, False)],
)
def test_is_input_required_era(protocol_version, expected_result) -> None:
    ctx = _make_ctx(protocol_version=protocol_version)
    assert is_input_required_era(ctx) == expected_result


def test_is_input_required_era_no_request_context() -> None:
    ctx = MagicMock()
    ctx.request_context = None
    assert not is_input_required_era(ctx)


def test_request_input() -> None:
    response_type = create_query_elicitation_type("DROP TABLE T")
    result = request_input("Hello", response_type, {"sql": "DROP TABLE T2"})
    params = _get_elicit_params(result)
    assert params.message == "Hello"
    assert params.requested_schema["properties"]["sql"]["default"] == "DROP TABLE T"
    assert json.loads(result.request_state) == {"sql": "DROP TABLE T2"}


def test_request_input_empty_state() -> None:
    response_type = create_query_elicitation_type("DROP TABLE T")
    result = request_input("Hello", response_type)
    assert json.loads(result.request_state) == {}


def test_read_input_first_round() -> None:
    response_type = create_query_elicitation_type("DROP TABLE T")
    assert read_input(_make_ctx(), response_type) is None


def test_read_input_no_request_state() -> None:
    """
    A response that comes without the state sealed by the server is ignored.
    """
    ctx = _make_ctx(ElicitResult(action="accept", content={"sql": "DROP TABLE T2"}))
    ctx.request_state = None
    response_type = create_query_elicitation_type("DROP TABLE T")
    assert read_input(ctx, response_type) is None


def test_read_input_no_key() -> None:
    ctx = _make_ctx(state={})
    ctx.input_responses = {"something_else": ElicitResult(action="accept")}
    response_type = create_query_elicitation_type("DROP TABLE T")
    assert read_input(ctx, response_type) is None


def test_read_input_accepted() -> None:
    ctx = _make_ctx(ElicitResult(action="accept", content={"sql": "DROP TABLE T2"}))
    response_type = create_query_elicitation_type("DROP TABLE T")
    user_input = read_input(ctx, response_type)
    assert user_input.action == "accept"
    assert user_input.data.sql == "DROP TABLE T2"


@pytest.mark.parametrize("action", ["decline", "cancel"])
def test_read_input_not_accepted(action) -> None:
    ctx = _make_ctx(ElicitResult(action=action))
    response_type = create_query_elicitation_type("DROP TABLE T")
    user_input = read_input(ctx, response_type)
    assert user_input.action == action
    assert user_input.data is None


def test_read_input_wrong_response() -> None:
    ctx = _make_ctx(state={})
    ctx.input_responses = {ELICITATION_KEY: ListRootsResult(roots=[])}
    response_type = create_query_elicitation_type("DROP TABLE T")
    with pytest.raises(TypeError):
        read_input(ctx, response_type)


@pytest.mark.parametrize(
    ["state", "expected_state"], [(None, {}), ({"a": "b"}, {"a": "b"})]
)
def test_read_request_state(state, expected_state) -> None:
    assert read_request_state(_make_ctx(state=state)) == expected_state


def _create_write_query_server() -> ExasolMCPServer:
    config = MagicMock(enable_write_query=True, disable_elicitation=False)
    return ExasolMCPServer(connection=MagicMock(), config=config)


def test_execute_write_query_guard_first_round() -> None:
    server = _create_write_query_server()
    result = asyncio.run(server.execute_write_query("DROP TABLE T", _make_ctx()))
    assert isinstance(result, InputRequiredResult)
    params = _get_elicit_params(result)
    assert params.message == WRITE_QUERY_MESSAGE
    assert params.requested_schema["properties"]["sql"]["default"] == "DROP TABLE T"
    assert json.loads(result.request_state) == {}
    server.connection.execute_query.assert_not_called()


def test_execute_write_query_guard_unrequested_response() -> None:
    """
    A response without the request state, e.g. sent in the very first call, doesn't
    execute the query. The elicitation is requested instead.
    """
    server = _create_write_query_server()
    ctx = _make_ctx(ElicitResult(action="accept", content={"sql": "DROP TABLE T2"}))
    ctx.request_state = None
    result = asyncio.run(server.execute_write_query("DROP TABLE T", ctx))
    assert isinstance(result, InputRequiredResult)
    server.connection.execute_query.assert_not_called()


def test_execute_write_query_guard_accepted() -> None:
    server = _create_write_query_server()
    ctx = _make_ctx(ElicitResult(action="accept", content={"sql": "DROP TABLE T2"}))
    result = asyncio.run(server.execute_write_query("DROP TABLE T", ctx))
    assert result == "DROP TABLE T2"
    server.connection.execute_query.assert_called_once_with(
        "DROP TABLE T2", snapshot=False
    )


@pytest.mark.parametrize("action", ["decline", "cancel"])
def test_execute_write_query_guard_not_accepted(action) -> None:
    server = _create_write_query_server()
    ctx = _make_ctx(ElicitResult(action=action))
    with pytest.raises(InterruptedError):
        asyncio.run(server.execute_write_query("DROP TABLE T", ctx))
    server.connection.execute_query.assert_not_called()


def _create_bucketfs_tools(path_status: dict[str, PathStatus]) -> BucketFsTools:
    """
    Creates the BucketFS tools, with the path status given by the provided mapping.
    """
    config = MagicMock(disable_elicitation=False)
    tools = BucketFsTools(MagicMock(), config)
    tools._get_path_status = MagicMock(side_effect=path_status.get)
    return tools


def _delete_file(tools: BucketFsTools, ctx: MagicMock):
    return asyncio.run(tools.delete_file("a/b", ctx))


def _write_text_to_file(tools: BucketFsTools, ctx: MagicMock):
    return asyncio.run(tools.write_text_to_file("a/b", "original", ctx))


def test_bucketfs_guard_first_round() -> None:
    tools = _create_bucketfs_tools({"a/b": PathStatus.FileExists})
    result = _write_text_to_file(tools, _make_ctx())
    assert isinstance(result, InputRequiredResult)
    params = _get_elicit_params(result)
    assert PATH_WARNINGS[PathStatus.FileExists] in params.message
    properties = params.requested_schema["properties"]
    assert properties["file_path"]["default"] == "a/b"
    assert properties["file_content"]["default"] == "original"
    assert json.loads(result.request_state) == {}
    tools.bfs_location.joinpath.assert_not_called()


def test_bucketfs_guard_reask() -> None:
    """
    The user changes the path to the one where a file exists. The elicitation is
    repeated, offering the changed data and warning about the existing file.
    """
    tools = _create_bucketfs_tools(
        {"a/b": PathStatus.Vacant, "a/c": PathStatus.FileExists}
    )
    ctx = _make_ctx(
        ElicitResult(
            action="accept", content={"file_path": "a/c", "file_content": "edited"}
        )
    )
    result = _write_text_to_file(tools, ctx)
    assert isinstance(result, InputRequiredResult)
    params = _get_elicit_params(result)
    assert PATH_WARNINGS[PathStatus.FileExists] in params.message
    properties = params.requested_schema["properties"]
    assert properties["file_path"]["default"] == "a/c"
    assert properties["file_content"]["default"] == "edited"
    assert json.loads(result.request_state) == {
        "file_path": "a/c",
        "file_content": "edited",
    }
    tools.bfs_location.joinpath.assert_not_called()


def test_bucketfs_guard_confirmed_after_reask() -> None:
    """
    The user confirms the path offered in the repeated elicitation. The content
    edited in the earlier round must be preserved, even though the client omits it.
    """
    tools = _create_bucketfs_tools(
        {"a/b": PathStatus.Vacant, "a/c": PathStatus.FileExists}
    )
    ctx = _make_ctx(
        ElicitResult(action="accept", content={"file_path": "a/c"}),
        state={"file_path": "a/c", "file_content": "edited"},
    )
    result = _write_text_to_file(tools, ctx)
    assert result is None
    tools.bfs_location.joinpath.assert_called_once_with("a/c")
    tools.bfs_location.joinpath.return_value.write.assert_called_once_with(b"edited")


@pytest.mark.parametrize(
    ["path_status", "expected_deletion"],
    [(PathStatus.FileExists, True), (PathStatus.Vacant, False)],
)
def test_bucketfs_guard_expected_status(path_status, expected_deletion) -> None:
    """
    The deletion goes ahead only if the accepted path has the expected status.
    Otherwise, the elicitation is repeated.
    """
    tools = _create_bucketfs_tools({"a/b": path_status})
    ctx = _make_ctx(ElicitResult(action="accept", content={"file_path": "a/b"}))
    result = _delete_file(tools, ctx)
    if expected_deletion:
        assert result is None
        tools.bfs_location.joinpath.return_value.rm.assert_called_once()
    else:
        assert isinstance(result, InputRequiredResult)
        params = _get_elicit_params(result)
        assert PATH_WARNINGS[PathStatus.Vacant] in params.message
        tools.bfs_location.joinpath.assert_not_called()


@pytest.mark.parametrize("action", ["decline", "cancel"])
def test_bucketfs_guard_not_accepted(action) -> None:
    tools = _create_bucketfs_tools({"a/b": PathStatus.FileExists})
    with pytest.raises(InterruptedError):
        _delete_file(tools, _make_ctx(ElicitResult(action=action)))
    tools.bfs_location.joinpath.assert_not_called()
