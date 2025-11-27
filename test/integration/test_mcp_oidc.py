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

We test three FastMCP options for the OpenID setup. In two of them the server
orchestrates the OpenID authentication/authorization. One - Remote OAuth - is a
preferred choice if the authorization server supports the Dynamic Client Registration
(DCR). Another one - OAuth Proxy - emulates the DCR for the servers that do not
support it. Please check https://gofastmcp.com/servers/auth/authentication for details.

We also test the case when the MCP server only verifies an externally provided
access token - bearer token mode. In this mode the client application takes the
responsibility of acquiring a valid access token.

Lastly we test the MCP Server with the SaaS backend. With these backend the server tools
themselves are not protected with OAuth.

We test two tools. One, called "say_hello" is an artificial that doesn't require the
database. With this tool we test only the MCP Client/Server setup. Another one -
"list_schemas" - is a real tool that does require the database connection. This one
allows us to run a complete test.

Update on 13-Nov-2025:
The tests running MCP server with OAuth Proxy now can only run manually, from a CLI.
These tests have to be excluded from CI. Starting from FastMCP 2.13, it is no longer
possible to shortcut the User Authorization UI, when working with OAuth Proxy. The
redirection to the authorization endpoint of the OIDC server cannot be extracted from
the url passed to the OAuth.redirect_handler.
"""

import asyncio
import json
import multiprocessing
import ssl
import time
from collections.abc import Generator
from contextlib import (
    ExitStack,
    contextmanager,
)
from test.utils.db_objects import ExaSchema
from test.utils.mcp_oidc_constants import *
from unittest.mock import patch
from urllib.parse import quote

import docker
import httpx
import pytest
from _pytest.monkeypatch import MonkeyPatch
from authlib import jose
from authlib.integrations.flask_oauth2 import AuthorizationServer
from authlib.oauth2.rfc9068 import JWTBearerTokenGenerator
from docker.models.containers import Container
from fastmcp import Client
from fastmcp.client.auth.oauth import OAuth
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.server.auth import (
    AuthProvider,
    OAuthProxy,
    RemoteAuthProvider,
)
from fastmcp.server.auth.providers.jwt import JWTVerifier
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
from pyexasol import (
    ExaConnection,
    ExaRequestError,
)

from exasol.ai.mcp.server.connection_factory import (
    ENV_DSN,
    ENV_PASSWORD,
    ENV_SAAS_ACCOUNT_ID,
    ENV_SAAS_DATABASE_NAME,
    ENV_SAAS_HOST,
    ENV_SAAS_PAT,
    ENV_SAAS_PAT_HEADER,
    ENV_USER,
    ENV_USERNAME_CLAIM,
    get_connection_factory,
    get_oidc_user,
)
from exasol.ai.mcp.server.db_connection import DbConnection
from exasol.ai.mcp.server.generic_auth import (
    ENV_PROVIDER_TYPE,
    AuthParameter,
    exa_parameter_env_name,
    exa_provider_name,
    get_auth_provider,
)
from exasol.ai.mcp.server.main import create_mcp_server
from exasol.ai.mcp.server.server_settings import (
    McpServerSettings,
    MetaListSettings,
)


def _validate_db_oidc_setup(pyexasol_connection: ExaConnection) -> None:
    """
    Validates that the JWK endpoint was set up in the database.
    Warning! This function uses undocumented table EXA_COMMANDLINE. This table can be
    renamed or removed in future versions of the database. In such a case the function
    will have no effect apart from printing a warning.
    """
    query = (
        "SELECT PARAM_VALUE FROM EXA_COMMANDLINE WHERE PARAM_NAME = 'oidcProviderJKU'"
    )
    try:
        value = pyexasol_connection.execute(query).fetchval()
        if value != DOCKER_JWK_URL:
            raise RuntimeError(
                f"The expected JWK endpoint is not set up in the database. Found {value}"
            )
        print(f"✓ JWK endpoint is found in the database")
    except ExaRequestError:
        print("Warning: unable to read JWK endpoint from the database")


@pytest.fixture(scope="session")
def create_users(run_on_itde, pyexasol_connection) -> None:
    """
    The fixture creates two new users. One is identified by an OpenID access token,
    and another one is by password.
    """
    _validate_db_oidc_setup(pyexasol_connection)
    create_query1 = f"""CREATE USER "{OIDC_USER_NAME}" IDENTIFIED BY OPENID SUBJECT '{OIDC_USER_SUB}'"""
    create_query2 = (
        f'CREATE USER "{SERVER_USER_NAME}" IDENTIFIED BY "{SERVER_USER_PASSWORD}"'
    )
    grant_query1 = f'GRANT CREATE SESSION TO "{OIDC_USER_NAME}"'
    grant_query2 = f'GRANT CREATE SESSION TO "{SERVER_USER_NAME}"'
    grant_query3 = f'GRANT IMPERSONATE ANY USER TO "{SERVER_USER_NAME}"'
    drop_query1 = f'DROP USER IF EXISTS "{OIDC_USER_NAME}" CASCADE'
    drop_query2 = f'DROP USER IF EXISTS "{SERVER_USER_NAME}" CASCADE'
    for query in [
        drop_query1,
        drop_query2,
        create_query1,
        create_query2,
        grant_query1,
        grant_query2,
        grant_query3,
    ]:
        pyexasol_connection.execute(query)
    yield
    for query in [drop_query1, drop_query2]:
        pyexasol_connection.execute(query)


class OAuthHeadless(OAuth):
    """
    The standard OAuth client provider for MCP servers, included in FastMCP,
    pops up a default web browser to facilitate real authorization by a human user.
    Since we don't have a human in the test framework, we need to bypass this step
    and submit the authorization automatically.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @staticmethod
    def _is_oidc_server_url(url: str) -> bool:
        return url.startswith(f"http://localhost:{OIDC_PORT}") or url.startswith(
            f"http://127.0.0.1:{OIDC_PORT}"
        )

    async def redirect_handler(self, authorization_url: str) -> None:
        # The code below is a replacement for
        # webbrowser.open(authorization_url)
        def send_authorization_request():
            with httpx.Client() as client:
                time.sleep(1)
                if not self._is_oidc_server_url(authorization_url):
                    # Here authorization url is a URL of a proxy. We need to get to its
                    # redirection URL before submitting the "user authorization".
                    # Starting from FastMCP 2.13, this trick no longer works!
                    response = client.get(authorization_url)
                    assert 300 <= response.status_code < 400
                    assert response.has_redirect_location
                    server_auth_url = response.headers["location"]
                    assert self._is_oidc_server_url(server_auth_url)
                else:
                    server_auth_url = authorization_url
                client.post(
                    server_auth_url, data={"sub": OIDC_USER_SUB}, follow_redirects=True
                )

        process = multiprocessing.Process(target=send_authorization_request)
        process.start()


@contextmanager
def start_oidc_server() -> Generator[None, None, str]:
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

    Another hack is related to the logic of JWT token generator, in regard to the token
    subject. The JWTBearerTokenGenerator follows the RFC 9068 recommendation, which says
    "In cases of access tokens obtained through grants where a resource owner is involved,
    such as the authorization code grant, the value of "sub" SHOULD correspond to the
    subject identifier of the resource owner". Hence, we need to add a `get_user_id`
    function to the User.
    """
    original_init_app = AuthorizationServer.init_app
    original_storage_init = Storage.__init__
    jwk = jose.RSAKey.generate_key(is_private=True)

    class MyJWTBearerTokenGenerator(JWTBearerTokenGenerator):
        def get_jwks(self):
            return jose.KeySet([jwk])

        def get_audiences(self, client, user, scope):
            return TOKEN_AUDIENCE

        def get_extra_claims(self, client, grant_type, user, scope):
            return user.claims

    def new_init_app(self, app_, query_client=None, save_token=None):
        original_init_app(self, app_, query_client, save_token)
        self.register_token_generator(
            "default", MyJWTBearerTokenGenerator(issuer=TOKEN_ISSUER, alg=_JWS_ALG)
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
            json={"name": "MCP_Test", TOKEN_USERNAME: OIDC_USER_NAME},
        )
        assert response.status_code == 204
        print("✓ The user credentials are created")
        yield server_url


@pytest.fixture(scope="session")
def oidc_server(run_on_itde) -> str:
    with start_oidc_server() as server_url:
        yield server_url


def _verify_docker_network(container: Container) -> None:
    """
    Verifies that JWK endpoint is accessible from the DockerDB.
    The function relies on curl being installed in the ITDE. We will first
    check if this is the case. If not, the verification will be skipped.
    """
    command = ["curl", "--version"]
    exec_result = container.exec_run(command)
    if exec_result.exit_code != 0:
        print("Warning: Unable to verify the JWK endpoint access from the DockerDB")
        return
    command = [
        "curl",
        "-s",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        DOCKER_JWK_URL,
    ]
    exec_result = container.exec_run(command)
    if exec_result.exit_code == 0:
        status_code = int(exec_result.output.decode("utf-8"))
        if 200 <= status_code < 300:
            print(
                f"✓ JWK endpoint is accessible from the DockerDB: HTTP code {status_code}"
            )
        else:
            raise RuntimeError(
                f"JWK endpoint is inaccessible from the DockerDB: HTTP code {status_code}"
            )
    else:
        raise RuntimeError(
            f"Failed to call JWK endpoint from the DockerDB: exit code {exec_result.exit_code}"
        )


@pytest.fixture(scope="session")
def setup_docker_network(run_on_itde, oidc_server):
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

    _verify_docker_network(container)
    yield

    # Cleanup
    network.disconnect(container)
    print(f"✓ Disconnected {CONTAINER_NAME} from {network_name}")
    network.remove()
    print(f"✓ Deleted network: {network_name}")


def _set_auth_type(monkeypatch: MonkeyPatch, provider_type: type[AuthProvider]):
    monkeypatch.setenv(ENV_PROVIDER_TYPE, exa_provider_name(provider_type))


def _set_auth_param(monkeypatch: MonkeyPatch, name: str, value: str):
    monkeypatch.setenv(exa_parameter_env_name(AuthParameter(name)), value)


def _mcp_server_factory(env: dict[str, str], monkeypatch: MonkeyPatch | None = None):
    """
    Returns an MCP server factory that creates the MCP server and runs it as an http
    server at the provided host and port.
    The factory also adds one more tool - say_hello - for testing the MCP OpenID
    infrastructure without the database.
    """

    def say_hello() -> str:
        user, _ = get_oidc_user(TOKEN_USERNAME)
        return f"Hello {user}"

    def get_access_token_string() -> str:
        _, token = get_oidc_user(None)
        return token

    def run_server(host: str, port: int) -> None:

        if monkeypatch is not None:
            _set_auth_param(monkeypatch, "base_url", f"http://{host}:{port}")
        auth = get_auth_provider()
        connection_factory = get_connection_factory(
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
        mcp_server.tool(
            get_access_token_string, description="The tool returns the access token"
        )
        mcp_server.run(transport="http", host=host, port=port)

    return run_server


def _start_mcp_server(
    env: dict[str, str], monkeypatch: MonkeyPatch | None = None
) -> Generator[None, None, str]:
    """
    Starts the MCP server in a separate process and returns its url.
    """
    with run_server_in_process(_mcp_server_factory(env, monkeypatch)) as url:
        yield f"{url}/mcp"


@pytest.fixture(params=["A", "B", "C"])
def oidc_env(
    request, run_on_itde, backend_aware_onprem_database_params
) -> dict[str, str]:
    """
    The fixture builds a configuration for the `get_connection_factory` for the OnPrem.
    backend. It provides 3 configuration options - A, B and C - as described in the
    `get_connection_factory` docstring. Please refer to this documentation for more
    details on various connection options.
    """
    env = {ENV_DSN: backend_aware_onprem_database_params["dsn"]}
    if request.param in ["A", "C"]:
        env[ENV_USER] = SERVER_USER_NAME
        env[ENV_PASSWORD] = SERVER_USER_PASSWORD
    if request.param in ["B", "C"]:
        env[ENV_USERNAME_CLAIM] = TOKEN_USERNAME
    return env


@pytest.fixture
def oidc_env_run_once(oidc_env) -> None:
    """
    The `oidc env` fixture sets different options for DB connection.
    For the tests that do not use DB this is irrelevant. We don't want
    these test to run multiple times unnecessarily.
    """
    if ENV_USERNAME_CLAIM in oidc_env:
        pytest.skip()


@pytest.fixture(params=["D", "E"])
def saas_env(
    request,
    run_on_saas,
    saas_host,
    saas_account_id,
    saas_pat,
    database_name,
) -> dict[str, str]:
    """
    The fixture builds a configuration for the `get_connection_factory` for the SaaS
    backend. It provides 2 configuration options - D and E (pre-configured PAT and the PAT
    passed in a header).
    """
    env = {
        ENV_SAAS_HOST: saas_host,
        ENV_SAAS_ACCOUNT_ID: saas_account_id,
        ENV_SAAS_DATABASE_NAME: database_name,
    }
    if request.param == "D":
        env[ENV_SAAS_PAT] = saas_pat
    if request.param == "E":
        env[ENV_SAAS_PAT_HEADER] = PAT_HEADER
    return env


@pytest.fixture
def mcp_server_with_remote_oauth(oidc_server, oidc_env, monkeypatch):
    """
    Starts the MCP server using an external identity provider that supports DCR.
    https://gofastmcp.com/servers/auth/remote-oauth
    """
    _set_auth_type(monkeypatch, RemoteAuthProvider)
    _set_auth_param(monkeypatch, "jwks_uri", f"{oidc_server}/jwks")
    _set_auth_param(monkeypatch, "authorization_servers", oidc_server)

    for url in _start_mcp_server(oidc_env, monkeypatch):
        print(f"✓ MCP server with Remote OAuth started at {url}")
        yield url


@pytest.fixture
def mcp_server_with_oauth_proxy(started_manually, oidc_server, oidc_env, monkeypatch):
    """
    Starts the MCP server using an external identity provider that doesn't support DCR.
    https://gofastmcp.com/servers/auth/oauth-proxy
    The mock authorization server does support DCR, but in this scenario we will not use
    this feature.
    Tests using this fixture can only run in a manual mode, i.e. not from a CI.
    """
    if not started_manually:
        pytest.skip("OAuth Proxy tests can only be run manually")

    _set_auth_type(monkeypatch, OAuthProxy)
    _set_auth_param(monkeypatch, "jwks_uri", f"{oidc_server}/jwks")
    _set_auth_param(
        monkeypatch,
        "upstream_authorization_endpoint",
        f"{oidc_server}/oauth2/authorize",
    )
    _set_auth_param(
        monkeypatch, "upstream_token_endpoint", f"{oidc_server}/oauth2/token"
    )
    _set_auth_param(monkeypatch, "upstream_client_id", "MY_CLIENT_ID")
    _set_auth_param(monkeypatch, "upstream_client_secret", "MY_CLIENT_SECRET")

    for url in _start_mcp_server(oidc_env, monkeypatch):
        print(f"✓ MCP server with OAuth Proxy started at {url}")
        yield url


@pytest.fixture
def mcp_server_with_token_verifier(oidc_server, oidc_env, monkeypatch):
    """
    Starts the MCP server that only verifies externally provided tokens
    https://gofastmcp.com/servers/auth/token-verification
    """
    _set_auth_type(monkeypatch, JWTVerifier)
    _set_auth_param(monkeypatch, "jwks_uri", f"{oidc_server}/jwks")
    for url in _start_mcp_server(oidc_env):
        print(f"✓ MCP server with Token Verification started at {url}")
        yield url


@pytest.fixture
def mcp_server_with_saas(saas_env, monkeypatch):
    """
    Starts the MCP server with no authorization.
    """
    for url in _start_mcp_server(saas_env, monkeypatch):
        print(f"✓ MCP server with No OAuth started at {url}")
        yield url


async def _run_tool_async(
    http_server_url: str,
    tool_name: str,
    auto_auth: bool = True,
    token: str | None = None,
    headers: dict[str, str] | None = None,
    **kwargs,
) -> str:
    """
    Creates an MCP client with auth set to
    a. the token, if the one is provided,
    b. None, if the headers are provided (SaaS case),
    c. authomatic authorization, if `auto_auth` is True,
    d. UI based authorization, otherwise (for manual tests).
    Then calls the specified tool asynchronously.
    """
    if token:
        oauth = token
    elif headers:
        oauth = None
    elif auto_auth:
        oauth = OAuthHeadless(mcp_url=http_server_url)
    else:
        oauth = OAuth(mcp_url=http_server_url)
    async with Client(
        transport=StreamableHttpTransport(http_server_url, headers=headers), auth=oauth
    ) as client:
        assert await client.ping()
        result = await client.call_tool(tool_name, kwargs)
        return result.content[0].text


def _run_say_hello_test(
    http_server_url: str, auto_auth: bool = True, token: str | None = None
) -> None:
    """
    Tests the added test tool that doesn't require the database.
    """
    result_text = asyncio.run(
        _run_tool_async(http_server_url, "say_hello", auto_auth=auto_auth, token=token)
    )
    assert result_text == f"Hello {OIDC_USER_NAME}"


def _run_list_schemas_test(
    http_server_url: str,
    db_schemas: list[ExaSchema],
    auto_auth: bool = True,
    token: str | None = None,
    headers: dict[str, str] | None = None,
) -> None:
    """
    Tests one of the real tools that requires the database.
    """
    result_text = asyncio.run(
        _run_tool_async(
            http_server_url,
            "list_schemas",
            token=token,
            auto_auth=auto_auth,
            headers=headers,
        )
    )
    result_json = json.loads(result_text)
    schemas = {s["name"] for s in result_json["result"]}
    expected_schemas = {schema.name for schema in db_schemas}
    assert schemas == expected_schemas


@pytest.fixture
def bearer_token(mcp_server_with_remote_oauth) -> str:
    """
    This feature creates an access token for the bearer token mode testing,
    using another MCP server.
    """
    return asyncio.run(
        _run_tool_async(mcp_server_with_remote_oauth, "get_access_token_string")
    )


def test_remote_oauth_no_db(oidc_env_run_once, mcp_server_with_remote_oauth) -> None:
    _run_say_hello_test(mcp_server_with_remote_oauth)


def test_oauth_proxy_no_db(oidc_env_run_once, mcp_server_with_oauth_proxy) -> None:
    _run_say_hello_test(mcp_server_with_oauth_proxy, auto_auth=False)


def test_bearer_token_no_db(
    oidc_env_run_once, bearer_token, mcp_server_with_token_verifier
) -> None:
    _run_say_hello_test(mcp_server_with_token_verifier, token=bearer_token)


def test_remote_oauth_with_itde(
    create_users,
    mcp_server_with_remote_oauth,
    setup_docker_network,
    setup_database,
    db_schemas,
) -> None:
    _run_list_schemas_test(mcp_server_with_remote_oauth, db_schemas)


def test_oauth_proxy_with_itde(
    create_users,
    mcp_server_with_oauth_proxy,
    setup_docker_network,
    setup_database,
    db_schemas,
) -> None:
    _run_list_schemas_test(mcp_server_with_oauth_proxy, db_schemas, auto_auth=False)


def test_bearer_token_with_itde(
    create_users,
    bearer_token,
    mcp_server_with_token_verifier,
    setup_docker_network,
    setup_database,
    db_schemas,
) -> None:
    _run_list_schemas_test(
        mcp_server_with_token_verifier, db_schemas, token=bearer_token
    )


def test_remote_oauth_with_saas(
    mcp_server_with_saas, setup_database, db_schemas, saas_pat
) -> None:
    _run_list_schemas_test(
        mcp_server_with_saas, db_schemas, headers={PAT_HEADER: saas_pat}
    )
