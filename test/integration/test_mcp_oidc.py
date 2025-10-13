"""
This module tests the integration of the FastMCP OAuth2 infrastructure with the
Exasol database that enables the OpenID Authentication.

In order to set up the tests the following steps are performed:

1. Create a new User identified by an OpenID token. The DB connection opened by the
   MCP server will use this username.

2. Start a mock identity (authorization is another name) server. The server accepts
   any client name (the client registration is trivial in that sense). The user
   authorizing the access to the MCP server and the database must have an "account"
   at the server, but creating it is a simple matter of providing the name (subject)
   of the user.

3. Set up the network for the DockerDB so that it can access the authorization server
   running on the host. The endpoint used by the database has to be known in advance.
   It must be provided as a parameter to the ITDE when an instance of the DockerDB is
   spawned. This happens in the pytest plugin and controlled from a GitHub workflow.

4. Start the MCP Server with the http transport, in a separate process. The server
   will use different OpenID parameters depending on the test.

5. Start the MCP Client with OAuth2 support, call a tool and validate the result.

The last two steps are repeated in each test, since we are going to test different
modes in regard to the OpenID setup.

Currently, we test two FastMCP options for the OpenID setup. In both cases the server
orchestrates the OpenID authentication/authorization. One - Remote OAuth - is a
preferred choice if the authorization server supports the Dynamic Client Registration
(DCR). Another one - OAuth Proxy - emulates the DCR for the servers that do not
support it. Please check https://gofastmcp.com/servers/auth/authentication for details.

We test two tools. One is an artificial that doesn't require the database. With this
tool we test only the MCP Client/Server setup. Another one is a real tool that does
require the database connection. This one allows us to run a complete test.

Remains to be tested:

- A bearer token mode. That's when the client application takes the responsibility of
  acquiring a valid access token. The MCP server just varifies the token it has been
  given. This is exactly how the Exasol database behaves in regard to the OpenID
  authentication.

- The mode when the authorization server issues a short-lived access token and a
  refresh token. The access token refreshing should be performed by the MCP Client.

- Exasol recommends two optional token verification tests - the audience and the
  issuer of the token. These options must be validated.
"""

import asyncio
import json
import multiprocessing
import ssl
import time
from collections.abc import Generator
from contextlib import ExitStack
from datetime import timedelta
from test.utils.db_objects import ExaSchema
from test.utils.mcp_oidc_constants import *
from unittest.mock import patch
from urllib.parse import quote

import docker
import httpx
import pytest
from authlib import jose
from authlib.integrations.flask_oauth2 import AuthorizationServer
from authlib.oauth2.rfc9068 import JWTBearerTokenGenerator
from fastmcp import Client
from fastmcp.client.auth.oauth import OAuth
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.utilities.tests import run_server_in_process
from oidc_provider_mock._app import (
    _JWS_ALG,
    app,
)
from oidc_provider_mock._server import _threaded_server
from oidc_provider_mock._storage import (
    Storage,
    User,
)

from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.main import (
    ENV_AUTH_ENDPOINT,
    ENV_AUTH_SERVERS,
    ENV_BASE_URL,
    ENV_CLIENT_ID,
    ENV_CLIENT_SECRET,
    ENV_DSN,
    ENV_JWKS_URI,
    ENV_TOKEN_ENDPOINT,
    ENV_USER,
    AuthenticationMethod,
    create_mcp_server,
    get_auth_provider,
    get_connection_factory,
)
from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaListSettings,
)


@pytest.fixture(scope="session")
def create_open_id_user(pyexasol_connection) -> None:
    """
    The fixture creates a new user identified by an OpenID access token.
    """
    create_query = f"""CREATE USER "{OIDC_USER_NAME}" IDENTIFIED BY OPENID SUBJECT '{OIDC_USER_SUB}'"""
    grant_query = f'GRANT CREATE SESSION TO "{OIDC_USER_NAME}"'
    drop_query = f'DROP USER IF EXISTS "{OIDC_USER_NAME}" CASCADE'
    for query in [drop_query, create_query, grant_query]:
        pyexasol_connection.execute(query)
    yield
    pyexasol_connection.execute(drop_query)


class OAuthHeadless(OAuth):
    """
    The standard OAuth client provider for MCP servers, included in FastMCP,
    pops up a default web browser to facilitate real authorization by a human user.
    Since we don't have a human in the test framework, we need to bypass this step
    and submit the authorization automatically.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    async def redirect_handler(self, authorization_url: str) -> None:
        # The code below is a replacement for
        # webbrowser.open(authorization_url)
        def send_authorization_request():
            with httpx.Client() as client:
                time.sleep(1)
                if not (
                    authorization_url.startswith(f"http://localhost:{OIDC_PORT}")
                    or authorization_url.startswith(f"http://127.0.0.1:{OIDC_PORT}")
                ):
                    # Here authorization url is a URL of a proxy. We need to get to its
                    # redirection URL before submitting the "user authorization".
                    response = client.get(authorization_url)
                    assert 300 <= response.status_code < 400
                    assert response.has_redirect_location
                    server_auth_url = response.headers["location"]
                else:
                    server_auth_url = authorization_url
                client.post(
                    server_auth_url, data={"sub": OIDC_USER_SUB}, follow_redirects=True
                )

        process = multiprocessing.Process(target=send_authorization_request)
        process.start()


@pytest.fixture(scope="session")
def oidc_server() -> str:
    """
    The fixture starts the mock authorization server.

    Few patches are required.

    Firstly, the AuthorizationServer in Flask OAuth2 by default generates reference
    tokens. This kind of token is just a random string, that the server generates and
    remembers. A client varifies the token by showing it to the server and asking
    "is it yours?". Exasol uses another type of tokens - Json Web Token (JWT). The token
    is encrypted using an asymmetric encryption. The server keeps the key and gives the
    public part of it to a client upon request. The client decrypts the token and
    validates it without making further requests to the server.

    The mock authorization server keeps a constant encryption key in the Storage object.
    The patch replaces this key and uses it for the JWT token generation, which, in turn,
    replaces the default generator.

    Another hack is related to the logic of JWT token generator, which I currently do not
    fully understand. It expects the subject to come from an ID of an authorizing user.
    https://github.com/authlib/authlib/blob/06015d20652a23eff8350b6ad71b32fe41dae4ba/authlib/oauth2/rfc9068/token.py#L142
    We should investigate possible implications of that, however, this is beyond our
    control. It is even outside the scope of the MCP protocol. It is actually between
    the client app, e.g. Claude, and the identity (authorization) server, e.g. WorkOS.
    """
    original_init_app = AuthorizationServer.init_app
    original_storage_init = Storage.__init__
    jwk = jose.RSAKey.generate_key(is_private=True)
    server_url = "to be provided"

    class MyJWTBearerTokenGenerator(JWTBearerTokenGenerator):
        def get_jwks(self):
            return jose.KeySet([jwk])

    def new_init_app(self, app, query_client=None, save_token=None):
        original_init_app(self, app, query_client, save_token)
        self.register_token_generator(
            "default", MyJWTBearerTokenGenerator(issuer=server_url, alg=_JWS_ALG)
        )

    def new_storage_init(self):
        original_storage_init(self)
        self.jwk = jwk

    def get_user_id(self) -> str:
        return OIDC_USER_SUB

    User.get_user_id = get_user_id

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "authlib.integrations.flask_oauth2.AuthorizationServer.init_app",
                new_init_app,
            )
        )
        stack.enter_context(
            patch("oidc_provider_mock._app.Storage.__init__", new_storage_init)
        )

        auth_app = app(
            require_client_registration=False,
            require_nonce=False,
            issue_refresh_token=True,
            access_token_max_age=timedelta(hours=1),
        )
        stack.enter_context(
            _threaded_server(host="0.0.0.0", port=OIDC_PORT, app=auth_app)
        )

        # The authorization service will listen on all available NICs, which is required
        # for connecting to it from the Docker container, but other clients running in
        # the host will connect to it through the localhost.
        server_url = f"http://localhost:{OIDC_PORT}"
        print(f"✓ Authorization server started at {server_url}")

        # Create the MCP User credentials at the mock authorization server.
        response = httpx.put(
            f"{server_url}/users/{quote(OIDC_USER_SUB)}",
            json={"name": "MCP_Test"},
        )
        assert response.status_code == 204
        print("✓ The user credentials are created")
        yield server_url


@pytest.fixture(scope="session")
def setup_docker_network(oidc_server):
    """
    The fixture sets up the networking for the DockerDB, allowing it to connect to the
    mock authorization server. The DB will fetch the token encryption key from it.
    """
    client = docker.from_env()
    network_name = "oidc-test-bridge-network"

    container = client.containers.get(CONTAINER_NAME)
    # Create or get a custom docker network
    try:
        network = client.networks.create(
            name=network_name,
            driver="bridge",
            ipam=docker.types.IPAMConfig(
                pool_configs=[docker.types.IPAMPool(subnet=DOCKER_NET_SUBNET)]
            ),
        )
        print(f"✓ Created docker network: {network_name}")
    except docker.errors.APIError:
        # Try to use the existing network.
        network = client.networks.get(network_name)
        print(f"✓ Using existing docker network: {network_name}")

    # Connect container to the network
    current_networks = container.attrs["NetworkSettings"]["Networks"]
    if network_name not in current_networks:
        network.connect(container)
        print(f"✓ Connected container {CONTAINER_NAME} to {network_name}")
        # Allow network to initialize
        time.sleep(2)
    else:
        print(f"✓ Container {CONTAINER_NAME} is already in {network_name}")

    # Verify that JWK endpoint is accessible from the DockerDB
    cmd = f"curl -s -o /dev/null -w '%{{http_code}}' -X GET http://{DOCKER_NET_GATEWAY_IP}:{OIDC_PORT}/jwks"
    exit_code, output = container.exec_run(cmd)
    if exit_code == 0:
        status_code = int(output[0].decode("utf-8").strip())
        if 200 <= status_code < 300:
            print(f"✓ JWK endpoint is accessible from the DB: HTTP {status_code}")
        else:
            raise RuntimeError(
                f"JWK endpoint returned unexpected status: HTTP {status_code}"
            )
    else:
        error_msg = output[1].decode("utf-8").strip() if output[1] else "Unknown error"
        raise RuntimeError(f"Failed to call the JWK endpoint: {error_msg}")

    yield

    # Cleanup
    network.disconnect(container)
    print(f"✓ Disconnected {CONTAINER_NAME} from {network_name}")
    network.remove()
    print(f"✓ Deleted network: {network_name}")


def _mcp_server_factory(env: dict[str, str]):
    """
    Returns an MCP server factory that creates the MCP server and runs it as an http
    server at the provided host and port.
    The factory also adds one more tool - say_hello - for testing the MCP OpenID
    infrastructure without the database.
    """

    def say_hello() -> str:
        return "Hello"

    def run_server(host: str, port: int) -> None:

        env[ENV_BASE_URL] = f"http://{host}:{port}"
        env[ENV_USER] = OIDC_USER_NAME
        auth = get_auth_provider(env)
        connection_factory = get_connection_factory(
            AuthenticationMethod.OPEN_ID,
            env,
            websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
        )
        connection = DbConnection(connection_factory=connection_factory)

        mcp_server = create_mcp_server(
            connection=connection,
            config=McpServerSettings(
                schemas=MetaListSettings(enable=True, name_field="name")
            ),
            auth=auth,
        )
        mcp_server.tool(say_hello, description="The tool just says Hello")
        mcp_server.run(transport="http", host=host, port=port)

    return run_server


def _start_mcp_server(env: dict[str, str]) -> Generator[None, None, str]:
    """
    Starts the MCP server in a separate process and returns its url.
    """
    with run_server_in_process(_mcp_server_factory(env)) as url:
        yield f"{url}/mcp"


@pytest.fixture
def mcp_server_with_remote_oauth(oidc_server, backend_aware_onprem_database_params):
    """
    Starts the MCP server using an external identity provider that supports DCR.
    https://gofastmcp.com/servers/auth/remote-oauth
    """
    env = {
        ENV_DSN: backend_aware_onprem_database_params["dsn"],
        ENV_JWKS_URI: f"{oidc_server}/jwks",
        ENV_AUTH_SERVERS: oidc_server,
    }
    for url in _start_mcp_server(env):
        print(f"✓ MCP server with Remote OAuth started at {url}")
        yield url


@pytest.fixture
def mcp_server_with_oauth_proxy(oidc_server, backend_aware_onprem_database_params):
    """
    Starts the MCP server using an external identity provider that doesn't support DCR.
    https://gofastmcp.com/servers/auth/oauth-proxy
    The mock authorization server does support DCR, but in this scenario we will not use
    this feature.
    """
    env = {
        ENV_DSN: backend_aware_onprem_database_params["dsn"],
        ENV_JWKS_URI: f"{oidc_server}/jwks",
        ENV_AUTH_ENDPOINT: f"{oidc_server}/oauth2/authorize",
        ENV_TOKEN_ENDPOINT: f"{oidc_server}/oauth2/token",
        ENV_CLIENT_ID: "MY_CLIENT_ID",
        ENV_CLIENT_SECRET: "MY_CLIENT_SECRET",
    }
    for url in _start_mcp_server(env):
        print(f"✓ MCP server with OAuth Proxy started at {url}")
        yield url


async def _run_tool_async(http_server_url: str, tool_name: str, **kwargs):
    """
    Creates an MCP client and calls the specified tool asynchronously.
    """
    oauth = OAuthHeadless(mcp_url=http_server_url)
    async with Client(
        transport=StreamableHttpTransport(http_server_url), auth=oauth
    ) as client:
        assert await client.ping()
        return await client.call_tool(tool_name, kwargs)


def _run_say_hello_test(http_server_url: str) -> None:
    """
    Tests the added test tool that doesn't require the database.
    """
    result = asyncio.run(_run_tool_async(http_server_url, "say_hello"))
    result_text = result.content[0].text
    assert result_text == "Hello"


def _run_list_schemas_test(http_server_url: str, db_schemas: list[ExaSchema]) -> None:
    """
    Tests one of the real tools that requires the database.
    """
    result = asyncio.run(_run_tool_async(http_server_url, "list_schemas"))
    result_json = json.loads(result.content[0].text)
    schemas = {s["name"] for s in result_json["result"]}
    expected_schemas = {schema.name for schema in db_schemas}
    assert schemas == expected_schemas


def test_remote_oauth_no_db(mcp_server_with_remote_oauth) -> None:
    _run_say_hello_test(mcp_server_with_remote_oauth)


def test_oauth_proxy_no_db(mcp_server_with_oauth_proxy) -> None:
    _run_say_hello_test(mcp_server_with_oauth_proxy)


def test_remote_oauth_with_db(
    mcp_server_with_remote_oauth,
    setup_docker_network,
    db_schemas,
    setup_database,
    create_open_id_user,
) -> None:
    _run_list_schemas_test(mcp_server_with_remote_oauth, db_schemas)


def test_oauth_proxy_with_db(
    mcp_server_with_oauth_proxy,
    setup_docker_network,
    db_schemas,
    setup_database,
    create_open_id_user,
) -> None:
    _run_list_schemas_test(mcp_server_with_oauth_proxy, db_schemas)
