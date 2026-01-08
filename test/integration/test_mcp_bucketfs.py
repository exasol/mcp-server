from contextlib import contextmanager
from unittest.mock import patch

import pytest

from exasol.ai.mcp.server.connection_factory import env_to_bucketfs
from exasol.ai.mcp.server.main import mcp_server
from exasol.ai.mcp.server.server_settings import McpServerSettings


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
    assert (server.bucketfs_location is not None) == enable_bucketfs
