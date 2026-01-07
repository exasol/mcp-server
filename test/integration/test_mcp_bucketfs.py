from contextlib import contextmanager
from unittest.mock import patch

import exasol.bucketfs as bfs

from exasol.ai.mcp.server.main import mcp_server


@pytest.mark.parametrize("enable_bucketfs", [False, True])
@patch("exasol.ai.mcp.server.main.get_mcp_settings")
@patch("exasol.ai.mcp.server.connection_factory.get_bucketfs_location")
@patch("exasol.ai.mcp.server.connection_factory.get_connection_factory")
def test_mcp_server_with_bucketfs(
    mock_get_conn_factory,
    mock_get_bfs_location,
    mock_get_mcp_settings,
    enable_bucketfs,
    pyexasol_connection,
    backend_aware_bucketfs_params,
) -> None:
    """
    Verifies that if BucketFS tools are enabled the server gets a valid PathLike object
    pointing to the root of the BucketFS bucket. Otherwise, even if the BucketFS access
    is configured, the object is None.

    The `get_bucketfs_location` is not tested here but mocked instead. It is tested in
    a unit tests.
    """

    @contextmanager
    def connection_factory():
        yield pyexasol_connection

    mock_get_conn_factory.return_value = connection_factory
    mock_get_bfs_location.return_value = bfs.path.build_path(
        **backend_aware_bucketfs_params
    )
    mock_get_mcp_settings.return_value = McpServerSettings(
        enable_read_bucketfs=enable_bucketfs
    )
    server = mcp_server()
    if enable_bucketfs:
        assert server.bucketfs_location.exists()
    else:
        assert server.bucketfs_location is None
