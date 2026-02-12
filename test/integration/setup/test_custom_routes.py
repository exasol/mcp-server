import ssl

import httpx
import pytest
from fastmcp.utilities.tests import (
    find_available_port,
    run_server_in_process,
)

from exasol.ai.mcp.server.connection.connection_factory import (
    ENV_DSN,
    ENV_PASSWORD,
    ENV_USER,
    get_connection_factory,
)
from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.setup.server_settings import McpServerSettings


def _mcp_server_factory(env: dict[str, str]):

    def server_factory(host: str, port: int) -> None:

        connection_factory = get_connection_factory(
            env,
            websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
        )
        connection = DbConnection(connection_factory=connection_factory)

        mcp_server = create_mcp_server(
            connection=connection, config=McpServerSettings()
        )
        mcp_server.run(transport="http", host=host, port=port)

    return server_factory


@pytest.mark.parametrize(
    ["valid_password", "expected_status"], [(True, "healthy"), (False, "unhealthy")]
)
def test_health_check(
    run_on_itde, backend_aware_onprem_database_params, valid_password, expected_status
) -> None:
    env = {
        ENV_DSN: backend_aware_onprem_database_params["dsn"],
        ENV_USER: backend_aware_onprem_database_params["user"],
        ENV_PASSWORD: backend_aware_onprem_database_params["password"],
    }
    if not valid_password:
        env[ENV_PASSWORD] += "^^^"
    port = find_available_port()
    with run_server_in_process(_mcp_server_factory(env), port=port):
        url = f"http://localhost:{port}/health"
        response = httpx.request("GET", url)
        response.raise_for_status()
        response_json = response.json()
        assert response_json["service"] == "exasol-mcp-server"
        assert response_json["status"] == expected_status
