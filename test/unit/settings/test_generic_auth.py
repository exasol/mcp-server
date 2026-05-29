from typing import Union
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from fastmcp.server.auth import (
    AuthProvider,
    OAuthProxy,
    RemoteAuthProvider,
)
from fastmcp.server.auth.oauth_proxy import proxy as oauth_proxy_module
from fastmcp.server.auth.oidc_proxy import OIDCConfiguration
from fastmcp.server.auth.oidc_proxy import OIDCProxy as FastMCPOIDCProxy
from fastmcp.server.auth.providers.auth0 import Auth0Provider
from fastmcp.server.auth.providers.aws import AWSCognitoProvider
from fastmcp.server.auth.providers.azure import AzureProvider
from fastmcp.server.auth.providers.google import GoogleProvider
from fastmcp.server.auth.providers.introspection import IntrospectionTokenVerifier
from fastmcp.server.auth.providers.jwt import JWTVerifier
from fastmcp.server.auth.providers.workos import AuthKitProvider
from key_value.aio.stores.memory import MemoryStore

from exasol.ai.mcp.server.setup.generic_auth import (
    ENV_DYNAMODB_AWS_ACCESS_KEY_ID,
    ENV_DYNAMODB_AWS_SECRET_ACCESS_KEY,
    ENV_DYNAMODB_AWS_SESSION_TOKEN,
    ENV_DYNAMODB_ENDPOINT_URL,
    ENV_DYNAMODB_REGION_NAME,
    ENV_DYNAMODB_TABLE_NAME,
    ENV_MONGODB_DB_NAME,
    ENV_MONGODB_URL,
    ENV_PROVIDER_TYPE,
    ENV_REDIS_DB,
    ENV_REDIS_HOST,
    ENV_REDIS_PASSWORD,
    ENV_REDIS_PORT,
    ENV_REDIS_URL,
    ENV_STORAGE_BACKEND,
    AuthParameter,
    AuthProviderInfo,
    _build_provider_info_from_type,
    _import_type,
    _type_to_converter,
    create_auth_provider,
    create_client_storage,
    exa_parameter_env_name,
    exa_provider_name,
    get_auth_kwargs,
    get_auth_provider,
    get_token_verifier,
    parameter_env_name,
    str_to_bool,
    str_to_bool_or_external,
    str_to_dict,
    str_to_int,
    str_to_list,
    str_to_str,
)


class FakeAuthProvider(AuthProvider):
    # pylint: disable=super-init-not-called
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


class TypeAnnotatedFakeProvider(AuthProvider):
    # pylint: disable=super-init-not-called
    def __init__(
        self,
        str_param: str,
        bool_param: bool,
        int_param: int,
        list_param: list[str],
        dict_param: dict[str, str],
        optional_param: str | None = None,
    ) -> None:
        self.kwargs = {
            "str_param": str_param,
            "bool_param": bool_param,
            "int_param": int_param,
            "list_param": list_param,
            "dict_param": dict_param,
            "optional_param": optional_param,
        }


@pytest.fixture
def fake_provider_info() -> AuthProviderInfo:
    return AuthProviderInfo(
        provider_type=FakeAuthProvider,
        parameters=[
            AuthParameter("str_param"),
            AuthParameter("bool_param", str_to_bool),
            AuthParameter("list_param", str_to_list),
            AuthParameter("dict_param", str_to_dict),
        ],
    )


@pytest.mark.parametrize(
    ["input_str", "expected_str"],
    [
        ("  ", ""),
        (" abc ", "abc"),
        (' " abc " ', "abc"),
        ('a"bc', 'a"bc'),
        ('"a""bc"', 'a"bc'),
        ('"a""""bc"', 'a""bc'),
    ],
)
def test_str_to_str(input_str, expected_str):
    assert str_to_str(input_str) == expected_str


@pytest.mark.parametrize(
    ["input_str", "expected_bool"],
    [
        (" True ", True),
        (" false", False),
    ],
)
def test_str_to_bool(input_str, expected_bool):
    assert str_to_bool(input_str) == expected_bool


def test_str_to_bool_error():
    with pytest.raises(ValueError, match="Invalid boolean parameter"):
        str_to_bool("maybe")


@pytest.mark.parametrize(
    ["input_str", "expected_value"],
    [(" true ", True), (" false ", False), (" external ", "external")],
)
def test_str_to_bool_or_external(input_str, expected_value):
    assert str_to_bool_or_external(input_str) == expected_value


@pytest.mark.parametrize(
    ["input_str", "expected_int"],
    [
        (" 5 ", 5),
        (' "5"', 5),
    ],
)
def test_str_to_int(input_str, expected_int):
    assert str_to_int(input_str) == expected_int


@pytest.mark.parametrize(
    ["input_str", "expected_list"],
    [
        (" ", []),
        ("abc", ["abc"]),
        ("abc, def,g", ["abc", "def", "g"]),
        ("abc, def\n,\ng", ["abc", "def", "g"]),
        ('" a,bc ", "d""ef", """g""" ', ["a,bc", 'd"ef', '"g"']),
    ],
)
def test_str_to_list(input_str, expected_list):
    assert str_to_list(input_str) == expected_list


@pytest.mark.parametrize(
    ["input_str", "expected_dict"],
    [
        (" ", {}),
        ("abc, def", {"abc": "def"}),
        ("abc , def, gh, 5", {"abc": "def", "gh": "5"}),
        ("abc , def,\ngh,5", {"abc": "def", "gh": "5"}),
        ('"abc", "def""", g, "h,f"', {"abc": 'def"', "g": "h,f"}),
    ],
)
def test_str_to_dict(input_str, expected_dict):
    assert str_to_dict(input_str) == expected_dict


def test_str_to_dict_error():
    with pytest.raises(ValueError, match="Invalid dictionary parameter"):
        str_to_dict("abc, def, ghf")


def test_exa_provider_name() -> None:
    assert (
        exa_provider_name(FakeAuthProvider) == "exa.test_generic_auth.FakeAuthProvider"
    )


def test_exa_parameter_env_name() -> None:
    assert exa_parameter_env_name(AuthParameter("abc")) == "EXA_AUTH_ABC"


def test_fastmcp_parameter_env_name() -> None:
    provider_info = AuthProviderInfo(
        provider_type=GoogleProvider,
        provider_name="fastmcp.server.auth.providers.google.GoogleProvider",
        env_prefix="FASTMCP_SERVER_AUTH_GOOGLE_",
        parameters=[],
    )
    assert parameter_env_name(provider_info, AuthParameter("client_id")) == (
        "FASTMCP_SERVER_AUTH_GOOGLE_CLIENT_ID"
    )


@pytest.mark.parametrize(
    ["params", "extra_kwargs", "expected_kwargs"],
    [
        (
            {"str_param": "abc", "bool_param": "true"},
            {"something_else": 777},
            {"str_param": "abc", "bool_param": True, "something_else": 777},
        ),
        (
            {"list_param": 'abc, "e,d"', "dict_param": "x, 1, y, 2"},
            {},
            {"list_param": ["abc", "e,d"], "dict_param": {"x": "1", "y": "2"}},
        ),
    ],
    ids=["str_and_bool", "list_and_dict"],
)
def test_create_auth_provider(
    fake_provider_info, monkeypatch, params, extra_kwargs, expected_kwargs
) -> None:
    for key, value in params.items():
        monkeypatch.setenv(exa_parameter_env_name(AuthParameter(key)), value)
    provider = create_auth_provider(fake_provider_info, **extra_kwargs)
    assert isinstance(provider, FakeAuthProvider)
    assert provider.kwargs == expected_kwargs


def _join_params(first_params: dict[str, str], *other_params) -> dict[str, str]:
    params = dict(first_params)
    for other in other_params:
        params.update(other)
    return params


_JWT_params = {
    "jwks_uri": "http://my_identity_server.com/jwks",
    "base_url": "http://my_mcp_server.com",
}
_Introspection_params = {
    "introspection_url": "http://my_identity_server.com/introspection",
    "client_id": "my_client_id",
    "client_secret": "my_client_secret",
    "base_url": "http://my_mcp_server.com",
}
_RemoteAuth_params = {"authorization_servers": "http://my_identity_server.com/"}
_OAuthProxy_params = {
    "upstream_authorization_endpoint": "http://my_identity_server.com/authorize",
    "upstream_token_endpoint": "http://http://my_identity_server.com/token",
    "upstream_client_id": "my_client_id",
    "upstream_client_secret": "my_client_secret",
}


@pytest.mark.parametrize(
    ["verifier_type", "provider_type", "params"],
    [
        (JWTVerifier, JWTVerifier, _JWT_params),
        (IntrospectionTokenVerifier, IntrospectionTokenVerifier, _Introspection_params),
        (JWTVerifier, JWTVerifier, _join_params(_JWT_params, _Introspection_params)),
        (
            IntrospectionTokenVerifier,
            IntrospectionTokenVerifier,
            _join_params(_JWT_params, _Introspection_params),
        ),
        (
            JWTVerifier,
            RemoteAuthProvider,
            _join_params(_RemoteAuth_params, _JWT_params),
        ),
        (
            IntrospectionTokenVerifier,
            OAuthProxy,
            _join_params(_OAuthProxy_params, _Introspection_params),
        ),
    ],
    ids=[
        "JWT",
        "Introspection",
        "JWT2",
        "Introspection2",
        "JMT-RemoteAuth",
        "Introspection-OAuthProxy",
    ],
)
def test_get_token_verifier(monkeypatch, verifier_type, provider_type, params) -> None:
    provider_name = exa_provider_name(provider_type)
    monkeypatch.setenv(ENV_PROVIDER_TYPE, provider_name)
    for key, value in params.items():
        monkeypatch.setenv(exa_parameter_env_name(AuthParameter(key)), value)
    verifier, verifier_name = get_token_verifier(provider_name)
    assert isinstance(verifier, verifier_type)
    assert verifier_name == exa_provider_name(verifier_type)


def test_get_token_verifier_error(monkeypatch) -> None:
    provider_name = exa_provider_name(RemoteAuthProvider)
    monkeypatch.setenv(ENV_PROVIDER_TYPE, provider_name)
    params = {
        "introspection_url": "http://my_identity_server.com/introspection",
        "base_url": "http://my_mcp_server.com",
    }
    for key, value in params.items():
        monkeypatch.setenv(exa_parameter_env_name(AuthParameter(key)), value)
    with pytest.raises(ValueError, match="Insufficient parameters"):
        get_token_verifier(provider_name)


def test_get_auth_provider_none() -> None:
    """
    The test verifiers that get_auth_provider can be called safely without
    configuring any provider, and it will return None.
    """
    provider = get_auth_provider()
    assert provider is None


@pytest.mark.parametrize(
    ["provider_type", "params"],
    [
        (JWTVerifier, _JWT_params),
        (IntrospectionTokenVerifier, _Introspection_params),
        (RemoteAuthProvider, _join_params(_RemoteAuth_params, _Introspection_params)),
        (OAuthProxy, _join_params(_OAuthProxy_params, _JWT_params)),
    ],
    ids=["JWT", "Introspection", "RemoteAuth-Introspection", "OAuthProxy-JWT"],
)
def test_get_auth_provider(monkeypatch, provider_type, params, tmp_path) -> None:
    monkeypatch.setenv(ENV_PROVIDER_TYPE, exa_provider_name(provider_type))
    if provider_type is OAuthProxy:
        monkeypatch.setattr(oauth_proxy_module.settings, "home", tmp_path)
    for key, value in params.items():
        monkeypatch.setenv(exa_parameter_env_name(AuthParameter(key)), value)
    provider = get_auth_provider()
    assert isinstance(provider, provider_type)


def test_get_auth_provider_error(monkeypatch) -> None:
    monkeypatch.setenv(ENV_PROVIDER_TYPE, "exa.non_existent_provider")
    assert get_auth_provider() is None


@pytest.fixture
def oidc_config() -> OIDCConfiguration:
    return OIDCConfiguration.model_validate(
        {
            "issuer": "https://issuer.example.com",
            "authorization_endpoint": "https://issuer.example.com/authorize",
            "token_endpoint": "https://issuer.example.com/token",
            "jwks_uri": "https://issuer.example.com/jwks",
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
        }
    )


@pytest.fixture
def patch_oidc(monkeypatch, oidc_config):
    monkeypatch.setattr(
        FastMCPOIDCProxy,
        "get_oidc_configuration",
        staticmethod(lambda config_url, strict=None, timeout_seconds=None: oidc_config),
    )


_FASTMCP_PROVIDER_SELECTORS = {
    GoogleProvider: "fastmcp.server.auth.providers.google.GoogleProvider",
    AzureProvider: "fastmcp.server.auth.providers.azure.AzureProvider",
    AuthKitProvider: "fastmcp.server.auth.providers.workos.AuthKitProvider",
    Auth0Provider: "fastmcp.server.auth.providers.auth0.Auth0Provider",
    AWSCognitoProvider: "fastmcp.server.auth.providers.aws.AWSCognitoProvider",
}

_FASTMCP_ENV_PREFIXES = {
    GoogleProvider: "FASTMCP_SERVER_AUTH_GOOGLE_",
    AzureProvider: "FASTMCP_SERVER_AUTH_AZURE_",
    AuthKitProvider: "FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_",
    Auth0Provider: "FASTMCP_SERVER_AUTH_AUTH0_",
    AWSCognitoProvider: "FASTMCP_SERVER_AUTH_AWS_COGNITO_",
}


@pytest.mark.parametrize(
    ["provider_type", "params"],
    [
        (
            GoogleProvider,
            {
                "client_id": "google-client",
                "client_secret": "google-secret",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,email",
                "timeout_seconds": "12",
                "extra_authorize_params": "access_type, offline, prompt, consent",
                "enable_cimd": "false",
            },
        ),
        (
            AzureProvider,
            {
                "client_id": "azure-client",
                "client_secret": "azure-secret",
                "tenant_id": "azure-tenant",
                "required_scopes": "read",
                "base_url": "https://server.example.com",
                "additional_authorize_scopes": "openid,profile",
                "enable_cimd": "false",
                "require_authorization_consent": "external",
            },
        ),
        (
            AuthKitProvider,
            {
                "authkit_domain": "https://tenant.authkit.app",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,profile,email",
            },
        ),
        (
            Auth0Provider,
            {
                "config_url": "https://auth.example.com/.well-known/openid-configuration",
                "client_id": "auth0-client",
                "client_secret": "auth0-secret",
                "audience": "https://api.example.com",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,email",
                "allowed_client_redirect_uris": "http://localhost:3000/*,https://app.example.com/*",
                "require_authorization_consent": "external",
                "forward_resource": "false",
            },
        ),
        (
            AWSCognitoProvider,
            {
                "user_pool_id": "eu-central-1_abc123",
                "aws_region": "eu-central-1",
                "client_id": "aws-client",
                "client_secret": "aws-secret",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,email",
                "forward_resource": "false",
            },
        ),
    ],
    ids=["Google", "Azure", "AuthKit", "Auth0", "AWSCognito"],
)
def test_get_auth_provider_fastmcp_v2_envs(
    monkeypatch, provider_type, params, tmp_path, patch_oidc
) -> None:
    """Old FastMCP v2 env var prefix names are still accepted (backward compat)."""
    monkeypatch.setenv(ENV_PROVIDER_TYPE, _FASTMCP_PROVIDER_SELECTORS[provider_type])
    monkeypatch.setattr(oauth_proxy_module.settings, "home", tmp_path)
    prefix = _FASTMCP_ENV_PREFIXES[provider_type]
    for key, value in params.items():
        monkeypatch.setenv(f"{prefix}{key.upper()}", value)
    provider = get_auth_provider()
    assert isinstance(provider, provider_type)


@pytest.mark.parametrize(
    ["provider_type", "params"],
    [
        (
            GoogleProvider,
            {
                "client_id": "google-client",
                "client_secret": "google-secret",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,email",
                "timeout_seconds": "12",
                "extra_authorize_params": "access_type, offline, prompt, consent",
                "enable_cimd": "false",
            },
        ),
        (
            AzureProvider,
            {
                "client_id": "azure-client",
                "client_secret": "azure-secret",
                "tenant_id": "azure-tenant",
                "required_scopes": "read",
                "base_url": "https://server.example.com",
                "additional_authorize_scopes": "openid,profile",
                "enable_cimd": "false",
                "require_authorization_consent": "external",
            },
        ),
        (
            AuthKitProvider,
            {
                "authkit_domain": "https://tenant.authkit.app",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,profile,email",
            },
        ),
        (
            Auth0Provider,
            {
                "config_url": "https://auth.example.com/.well-known/openid-configuration",
                "client_id": "auth0-client",
                "client_secret": "auth0-secret",
                "audience": "https://api.example.com",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,email",
                "allowed_client_redirect_uris": "http://localhost:3000/*,https://app.example.com/*",
                "require_authorization_consent": "external",
                "forward_resource": "false",
            },
        ),
        (
            AWSCognitoProvider,
            {
                "user_pool_id": "eu-central-1_abc123",
                "aws_region": "eu-central-1",
                "client_id": "aws-client",
                "client_secret": "aws-secret",
                "base_url": "https://server.example.com",
                "required_scopes": "openid,email",
                "forward_resource": "false",
            },
        ),
    ],
    ids=["Google", "Azure", "AuthKit", "Auth0", "AWSCognito"],
)
def test_get_auth_provider_new_prefix(
    monkeypatch, provider_type, params, tmp_path, patch_oidc
) -> None:
    """New derived class-name prefix (FASTMCP_SERVER_AUTH_<CLASSNAME>_) is accepted."""
    monkeypatch.setenv(ENV_PROVIDER_TYPE, _FASTMCP_PROVIDER_SELECTORS[provider_type])
    monkeypatch.setattr(oauth_proxy_module.settings, "home", tmp_path)
    prefix = f"FASTMCP_SERVER_AUTH_{provider_type.__name__.upper()}_"
    for key, value in params.items():
        monkeypatch.setenv(f"{prefix}{key.upper()}", value)
    provider = get_auth_provider()
    assert isinstance(provider, provider_type)


def test_get_auth_kwargs(monkeypatch) -> None:
    monkeypatch.setenv(ENV_PROVIDER_TYPE, exa_provider_name(JWTVerifier))
    monkeypatch.setenv(
        exa_parameter_env_name(AuthParameter("jwks_uri")), "http://some_url:1234"
    )
    kwargs = get_auth_kwargs()
    assert len(kwargs) == 1
    assert isinstance(kwargs["auth"], JWTVerifier)


def test_get_auth_kwargs_empty() -> None:
    assert not get_auth_kwargs()


# ---------------------------------------------------------------------------
# _type_to_converter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ["annotation", "input_str", "expected"],
    [
        (str, " hello ", "hello"),
        (bool, "true", True),
        (bool, "false", False),
        (int, " 7 ", 7),
        (list, "a, b", ["a", "b"]),
        (list[str], "a, b", ["a", "b"]),
        (dict, "k, v", {"k": "v"}),
        (dict[str, str], "k, v", {"k": "v"}),
        (Union[bool, str], "hello", "hello"),  # unrecognised union → str fallback
        # Optional unwrapping
        (str | None, " x ", "x"),
        (bool | None, "yes", True),
        (int | None, "3", 3),
        (list[str] | None, "a,b", ["a", "b"]),
    ],
    ids=[
        "str",
        "bool_true",
        "bool_false",
        "int",
        "list",
        "list_str",
        "dict",
        "dict_str_str",
        "union_bool_str",
        "optional_str",
        "optional_bool",
        "optional_int",
        "optional_list",
    ],
)
def test__type_to_converter(annotation, input_str, expected) -> None:
    converter = _type_to_converter(annotation)
    assert converter(input_str) == expected


def test__type_to_converter_bool_or_external() -> None:
    from typing import Literal

    annotation = Union[bool, Literal["external"]]
    conv = _type_to_converter(annotation)
    assert conv("true") is True
    assert conv("false") is False
    assert conv("external") == "external"


# ---------------------------------------------------------------------------
# _import_type
# ---------------------------------------------------------------------------


def test__import_type_success() -> None:
    assert (
        _import_type("fastmcp.server.auth.providers.google.GoogleProvider")
        is GoogleProvider
    )


@pytest.mark.parametrize(
    "name",
    [
        "no_dot_here",
        "non_existent_module.SomeClass",
        "fastmcp.server.auth.providers.google.NonExistentClass",
    ],
    ids=["no_dot", "bad_module", "bad_class"],
)
def test__import_type_failure(name) -> None:
    assert _import_type(name) is None


# ---------------------------------------------------------------------------
# _build_provider_info_from_type
# ---------------------------------------------------------------------------


def test__build_provider_info_from_type() -> None:
    info = _build_provider_info_from_type(TypeAnnotatedFakeProvider)
    qualified = f"{TypeAnnotatedFakeProvider.__module__}.{TypeAnnotatedFakeProvider.__qualname__}"
    assert info.provider_name == qualified
    assert (
        info.env_prefix
        == f"FASTMCP_SERVER_AUTH_{TypeAnnotatedFakeProvider.__name__.upper()}_"
    )
    assert info.legacy_env_prefix is None  # not a known FastMCP built-in

    param_names = [p.name for p in info.parameters]
    assert param_names == [
        "str_param",
        "bool_param",
        "int_param",
        "list_param",
        "dict_param",
        "optional_param",
    ]

    converters = {p.name: p.conv for p in info.parameters}
    assert converters["str_param"](" hello ") == "hello"
    assert converters["bool_param"]("true") is True
    assert converters["int_param"]("42") == 42
    assert converters["list_param"]("a, b") == ["a", "b"]
    assert converters["dict_param"]("k, v") == {"k": "v"}
    assert converters["optional_param"](" x ") == "x"


# ---------------------------------------------------------------------------
# get_auth_provider — fully dynamic path (TypeAnnotatedFakeProvider)
# ---------------------------------------------------------------------------


def test_get_auth_provider_dynamic(monkeypatch) -> None:
    qualified_name = f"{TypeAnnotatedFakeProvider.__module__}.{TypeAnnotatedFakeProvider.__qualname__}"
    prefix = f"FASTMCP_SERVER_AUTH_{TypeAnnotatedFakeProvider.__name__.upper()}_"
    monkeypatch.setenv(ENV_PROVIDER_TYPE, qualified_name)
    monkeypatch.setenv(f"{prefix}STR_PARAM", "hello")
    monkeypatch.setenv(f"{prefix}BOOL_PARAM", "true")
    monkeypatch.setenv(f"{prefix}INT_PARAM", "42")
    monkeypatch.setenv(f"{prefix}LIST_PARAM", "a, b, c")
    monkeypatch.setenv(f"{prefix}DICT_PARAM", "k1, v1, k2, v2")
    provider = get_auth_provider()
    assert isinstance(provider, TypeAnnotatedFakeProvider)
    assert provider.kwargs["str_param"] == "hello"
    assert provider.kwargs["bool_param"] is True
    assert provider.kwargs["int_param"] == 42
    assert provider.kwargs["list_param"] == ["a", "b", "c"]
    assert provider.kwargs["dict_param"] == {"k1": "v1", "k2": "v2"}
    assert provider.kwargs["optional_param"] is None


# ---------------------------------------------------------------------------
# create_client_storage
# ---------------------------------------------------------------------------


def test_create_client_storage_default(monkeypatch) -> None:
    monkeypatch.delenv(ENV_STORAGE_BACKEND, raising=False)
    assert create_client_storage() is None


def test_create_client_storage_filetree(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "filetree")
    assert create_client_storage() is None


def test_create_client_storage_memory(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "memory")
    storage = create_client_storage()
    assert isinstance(storage, MemoryStore)


def test_create_client_storage_unknown(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "disk")
    with pytest.raises(ValueError, match="Unknown storage backend"):
        create_client_storage()


# ---------------------------------------------------------------------------
# DynamoDB backend
# ---------------------------------------------------------------------------

# aioboto3, redis, and pymongo are optional extras absent from the dev venv.
# The production helpers (_create_dynamodb_storage etc.) use deferred imports
# (`from key_value.aio.stores.X import XStore`) so the ImportError only fires
# at call time, not at module load.  patch.dict("sys.modules", ...) intercepts
# that import and returns a MagicMock, letting us assert on constructor args
# without installing the real packages.


def _mock_dynamodb_module():
    mock_store = MagicMock()
    mock_module = MagicMock()
    mock_module.DynamoDBStore = mock_store
    return mock_store, {"key_value.aio.stores.dynamodb": mock_module}


def test_create_client_storage_dynamodb_minimal(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "dynamodb")
    monkeypatch.setenv(ENV_DYNAMODB_TABLE_NAME, "my-table")
    for var in (
        ENV_DYNAMODB_REGION_NAME,
        ENV_DYNAMODB_ENDPOINT_URL,
        ENV_DYNAMODB_AWS_ACCESS_KEY_ID,
        ENV_DYNAMODB_AWS_SECRET_ACCESS_KEY,
        ENV_DYNAMODB_AWS_SESSION_TOKEN,
    ):
        monkeypatch.delenv(var, raising=False)

    mock_store, modules = _mock_dynamodb_module()
    with patch.dict("sys.modules", modules):
        create_client_storage()

    mock_store.assert_called_once_with(
        table_name="my-table",
        region_name=None,
        endpoint_url=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
    )


def test_create_client_storage_dynamodb_full(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "dynamodb")
    monkeypatch.setenv(ENV_DYNAMODB_TABLE_NAME, "my-table")
    monkeypatch.setenv(ENV_DYNAMODB_REGION_NAME, "us-east-1")
    monkeypatch.setenv(ENV_DYNAMODB_ENDPOINT_URL, "http://localhost:8000")
    monkeypatch.setenv(ENV_DYNAMODB_AWS_ACCESS_KEY_ID, "AKID")
    monkeypatch.setenv(ENV_DYNAMODB_AWS_SECRET_ACCESS_KEY, "SECRET")
    monkeypatch.setenv(ENV_DYNAMODB_AWS_SESSION_TOKEN, "TOKEN")

    mock_store, modules = _mock_dynamodb_module()
    with patch.dict("sys.modules", modules):
        create_client_storage()

    mock_store.assert_called_once_with(
        table_name="my-table",
        region_name="us-east-1",
        endpoint_url="http://localhost:8000",
        aws_access_key_id="AKID",
        aws_secret_access_key="SECRET",
        aws_session_token="TOKEN",
    )


def test_create_client_storage_dynamodb_missing_table_name(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "dynamodb")
    monkeypatch.delenv(ENV_DYNAMODB_TABLE_NAME, raising=False)

    mock_store, modules = _mock_dynamodb_module()
    with patch.dict("sys.modules", modules):
        with pytest.raises(ValueError, match=ENV_DYNAMODB_TABLE_NAME):
            create_client_storage()


# ---------------------------------------------------------------------------
# Redis backend
# ---------------------------------------------------------------------------


# Same sys.modules patching rationale as for DynamoDB above.
def _mock_redis_module():
    mock_store = MagicMock()
    mock_module = MagicMock()
    mock_module.RedisStore = mock_store
    return mock_store, {"key_value.aio.stores.redis": mock_module}


def test_create_client_storage_redis_url(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "redis")
    monkeypatch.setenv(ENV_REDIS_URL, "redis://redis.example.com:6379/1")
    for var in (ENV_REDIS_HOST, ENV_REDIS_PORT, ENV_REDIS_DB, ENV_REDIS_PASSWORD):
        monkeypatch.delenv(var, raising=False)

    mock_store, modules = _mock_redis_module()
    with patch.dict("sys.modules", modules):
        create_client_storage()

    mock_store.assert_called_once_with(url="redis://redis.example.com:6379/1")


def test_create_client_storage_redis_host_port(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "redis")
    monkeypatch.delenv(ENV_REDIS_URL, raising=False)
    monkeypatch.setenv(ENV_REDIS_HOST, "myredis.internal")
    monkeypatch.setenv(ENV_REDIS_PORT, "6380")
    monkeypatch.setenv(ENV_REDIS_DB, "2")
    monkeypatch.setenv(ENV_REDIS_PASSWORD, "s3cr3t")

    mock_store, modules = _mock_redis_module()
    with patch.dict("sys.modules", modules):
        create_client_storage()

    mock_store.assert_called_once_with(
        host="myredis.internal",
        port=6380,
        db=2,
        password="s3cr3t",
    )


def test_create_client_storage_redis_defaults(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "redis")
    for var in (
        ENV_REDIS_URL,
        ENV_REDIS_HOST,
        ENV_REDIS_PORT,
        ENV_REDIS_DB,
        ENV_REDIS_PASSWORD,
    ):
        monkeypatch.delenv(var, raising=False)

    mock_store, modules = _mock_redis_module()
    with patch.dict("sys.modules", modules):
        create_client_storage()

    mock_store.assert_called_once_with(
        host="localhost",
        port=6379,
        db=0,
        password=None,
    )


# ---------------------------------------------------------------------------
# MongoDB backend
# ---------------------------------------------------------------------------


# Same sys.modules patching rationale as for DynamoDB above.
def _mock_mongodb_module():
    mock_store = MagicMock()
    mock_module = MagicMock()
    mock_module.MongoDBStore = mock_store
    return mock_store, {"key_value.aio.stores.mongodb": mock_module}


def test_create_client_storage_mongodb_url_only(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "mongodb")
    monkeypatch.setenv(ENV_MONGODB_URL, "mongodb://mongo.example.com:27017")
    monkeypatch.delenv(ENV_MONGODB_DB_NAME, raising=False)

    mock_store, modules = _mock_mongodb_module()
    with patch.dict("sys.modules", modules):
        create_client_storage()

    mock_store.assert_called_once_with(url="mongodb://mongo.example.com:27017")


def test_create_client_storage_mongodb_with_db_name(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "mongodb")
    monkeypatch.setenv(ENV_MONGODB_URL, "mongodb://mongo.example.com:27017")
    monkeypatch.setenv(ENV_MONGODB_DB_NAME, "oauth_db")

    mock_store, modules = _mock_mongodb_module()
    with patch.dict("sys.modules", modules):
        create_client_storage()

    mock_store.assert_called_once_with(
        url="mongodb://mongo.example.com:27017",
        db_name="oauth_db",
    )


def test_create_client_storage_mongodb_missing_url(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "mongodb")
    monkeypatch.delenv(ENV_MONGODB_URL, raising=False)

    mock_store, modules = _mock_mongodb_module()
    with patch.dict("sys.modules", modules):
        with pytest.raises(ValueError, match=ENV_MONGODB_URL):
            create_client_storage()


# ---------------------------------------------------------------------------
# create_auth_provider — storage injection
# ---------------------------------------------------------------------------


class StorageCapableProvider(AuthProvider):
    # pylint: disable=super-init-not-called
    def __init__(self, client_storage=None, **kwargs) -> None:
        self.client_storage = client_storage
        self.kwargs = kwargs


def test_create_auth_provider_injects_memory_storage(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "memory")
    info = AuthProviderInfo(provider_type=StorageCapableProvider, parameters=[])
    provider = create_auth_provider(info)
    assert isinstance(provider, StorageCapableProvider)
    assert isinstance(provider.client_storage, MemoryStore)


def test_create_auth_provider_no_injection_when_filetree(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "filetree")
    info = AuthProviderInfo(provider_type=StorageCapableProvider, parameters=[])
    provider = create_auth_provider(info)
    assert isinstance(provider, StorageCapableProvider)
    assert provider.client_storage is None


def test_create_auth_provider_no_storage_for_jwt_verifier(monkeypatch) -> None:
    monkeypatch.setenv(ENV_STORAGE_BACKEND, "memory")
    monkeypatch.setenv(
        exa_parameter_env_name(AuthParameter("jwks_uri")), "http://example.com/jwks"
    )
    monkeypatch.setenv(
        exa_parameter_env_name(AuthParameter("base_url")), "http://example.com"
    )
    info = AuthProviderInfo(
        provider_type=JWTVerifier,
        parameters=[AuthParameter("jwks_uri"), AuthParameter("base_url")],
    )
    provider = create_auth_provider(info)
    assert isinstance(provider, JWTVerifier)


# ---------------------------------------------------------------------------
# OAuthProxy — default storage
# ---------------------------------------------------------------------------


def test_oauth_proxy_default_storage_is_filetree(monkeypatch, tmp_path) -> None:
    from key_value.aio.stores.filetree import FileTreeStore
    from key_value.aio.wrappers.encryption import FernetEncryptionWrapper

    monkeypatch.setattr(oauth_proxy_module.settings, "home", tmp_path)
    token_verifier = IntrospectionTokenVerifier(
        introspection_url="http://my_identity_server.com/introspection",
        client_id="my_client_id",
        client_secret="my_client_secret",
        base_url="http://my_mcp_server.com",
    )
    proxy = OAuthProxy(
        upstream_authorization_endpoint="http://my_identity_server.com/authorize",
        upstream_token_endpoint="http://my_identity_server.com/token",
        upstream_client_id="my_client_id",
        upstream_client_secret="my_client_secret",
        token_verifier=token_verifier,
        base_url="http://my_mcp_server.com",
    )

    assert isinstance(proxy._client_storage, FernetEncryptionWrapper)
    assert isinstance(proxy._client_storage.key_value, FileTreeStore)
    oauth_proxy_dirs = list((tmp_path / "oauth-proxy").iterdir())
    assert len(oauth_proxy_dirs) == 1


# ---------------------------------------------------------------------------
# _build_provider_info_from_type — client_storage excluded
# ---------------------------------------------------------------------------


class ProviderWithClientStorage(AuthProvider):
    # pylint: disable=super-init-not-called
    def __init__(self, name: str, client_storage=None) -> None:
        pass


def test_build_provider_info_skips_client_storage() -> None:
    info = _build_provider_info_from_type(ProviderWithClientStorage)
    param_names = [p.name for p in info.parameters]
    assert "client_storage" not in param_names
    assert "name" in param_names
