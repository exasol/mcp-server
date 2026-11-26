import importlib
import inspect
import logging
import pkgutil
import sys
from typing import (
    Any,
    Literal,
    Optional,
    Union,
    get_args,
    get_origin,
)
from unittest.mock import (
    MagicMock,
    patch,
)

import fastmcp.server.auth.providers as fastmcp_providers
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
from fastmcp.server.auth.providers.keycloak import KeycloakAuthProvider
from fastmcp.server.auth.providers.workos import AuthKitProvider
from key_value.aio.stores.memory import MemoryStore
from pydantic import AnyHttpUrl

from exasol.ai.mcp.server.setup.generic_auth import (
    _SKIP_KINDS,
    _SKIP_PARAMS,
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
    _get_constructor_signature,
    _get_constructor_type_hints,
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
    [
        (" true ", True),
        (" false ", False),
        (" external ", "external"),
        ("EXTERNAL", "external"),
    ],
)
def test__type_to_converter_bool_or_external_values(input_str, expected_value):
    conv = _type_to_converter(bool | Literal["external"])
    assert conv(input_str) == expected_value


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


def test_get_token_verifier_jwt_list_params(monkeypatch) -> None:
    provider_name = exa_provider_name(JWTVerifier)
    monkeypatch.setenv(ENV_PROVIDER_TYPE, provider_name)
    params = _join_params(
        _JWT_params, {"issuer": "iss1, iss2", "audience": "aud1, aud2"}
    )
    for key, value in params.items():
        monkeypatch.setenv(exa_parameter_env_name(AuthParameter(key)), value)
    verifier, _ = get_token_verifier(provider_name)
    assert verifier.issuer == ["iss1", "iss2"]
    assert verifier.audience == ["aud1", "aud2"]


def test_get_token_verifier_introspection_audience(monkeypatch) -> None:
    provider_name = exa_provider_name(IntrospectionTokenVerifier)
    monkeypatch.setenv(ENV_PROVIDER_TYPE, provider_name)
    params = _join_params(_Introspection_params, {"audience": "aud1, aud2"})
    for key, value in params.items():
        monkeypatch.setenv(exa_parameter_env_name(AuthParameter(key)), value)
    verifier, _ = get_token_verifier(provider_name)
    assert verifier.audience == ["aud1", "aud2"]


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
        # Unions with a list member
        (str | list[str] | None, "a, b", ["a", "b"]),
        (list[str] | str | None, "a, b", ["a", "b"]),
        (Optional[Union[str, list[str]]], "a, b", ["a", "b"]),
        (Union[str, list[str]], "a", ["a"]),
        (str | list, "a, b", ["a", "b"]),
        (bool | Literal["external"], "true", True),
        (bool | Literal["remember", "external"], "remember", "remember"),
        (bool | Literal["remember", "external"], " Remember ", "remember"),
        (bool | Literal["remember", "external"], "external", "external"),
        (bool | Literal["remember", "external"], "no", False),
        (bool | Literal["remember"] | Literal["external"], "external", "external"),
        (Optional[Union[bool, Literal["remember"]]], "remember", "remember"),
        # Lists with non-string elements
        (list[int], "1, 2", [1, 2]),
        (list[bool], "yes, false", [True, False]),
        (list[int] | None, "3", [3]),
        (int | list[int] | None, "1, 2", [1, 2]),
        (list[str], 'a, "b,c"', ["a", "b,c"]),
        (list[AnyHttpUrl], 'x, "http://a/b,c"', ["x", "http://a/b,c"]),
        (list[str] | str | None, '"http://a/b,c"', ["http://a/b,c"]),
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
        "str_or_list",
        "list_or_str",
        "optional_union_str_list",
        "union_str_list_single",
        "str_or_bare_list",
        "bool_or_literal_pipe",
        "bool_or_literals_first",
        "bool_or_literals_case",
        "bool_or_literals_second",
        "bool_or_literals_bool",
        "bool_or_two_literals",
        "optional_bool_or_literal",
        "list_int",
        "list_bool",
        "optional_list_int",
        "int_or_list_int",
        "list_str_quoted",
        "list_url_quoted",
        "str_or_list_quoted",
    ],
)
def test__type_to_converter(annotation, input_str, expected) -> None:
    converter = _type_to_converter(annotation)
    assert converter(input_str) == expected


def test__type_to_converter_list_bool_error() -> None:
    converter = _type_to_converter(list[bool])
    with pytest.raises(ValueError, match="Invalid boolean value"):
        converter("true, maybe")


def test__type_to_converter_bool_or_external() -> None:
    annotation = Union[bool, Literal["external"]]
    conv = _type_to_converter(annotation)
    assert conv("true") is True
    assert conv("false") is False
    assert conv("external") == "external"


def test__type_to_converter_bool_or_literal_error() -> None:
    conv = _type_to_converter(bool | Literal["remember", "external"])
    with pytest.raises(ValueError, match="remember"):
        conv("remeber")


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


def test__build_provider_info_from_type_str_or_list() -> None:
    info = _build_provider_info_from_type(KeycloakAuthProvider)
    converters = {p.name: p.conv for p in info.parameters}
    assert converters["required_scopes"]("read, write") == ["read", "write"]
    assert converters["audience"]("aud1, aud2") == ["aud1", "aud2"]


def test__build_provider_info_from_type_azure() -> None:
    """
    AzureProvider annotates a parameter with ``AnyHttpUrl``, which fastmcp imports
    only under ``TYPE_CHECKING``. All other parameters must keep their types.
    """
    info = _build_provider_info_from_type(AzureProvider)
    converters = {p.name: p.conv for p in info.parameters}
    assert converters["required_scopes"]("read, write") == ["read", "write"]
    assert converters["additional_authorize_scopes"]("a, b") == ["a", "b"]
    assert converters["allowed_client_redirect_uris"]("u1, u2") == ["u1", "u2"]
    assert converters["enable_cimd"]("false") is False
    assert converters["forward_resource"]("false") is False
    assert converters["token_expiry_threshold_seconds"]("30") == 30
    assert converters["extra_authorize_params"]("k, v") == {"k": "v"}
    assert converters["resource_base_url"]("https://x.example.com") == (
        "https://x.example.com"
    )


def test__get_constructor_type_hints_resolved() -> None:
    """
    If all hints can be resolved at once, they are returned without the return
    type, and the per-parameter fallback is not used.
    """
    with patch(
        "exasol.ai.mcp.server.setup.generic_auth._resolve_annotation"
    ) as mock_resolve_annotation:
        hints = _get_constructor_type_hints(TypeAnnotatedFakeProvider)
    mock_resolve_annotation.assert_not_called()
    assert hints == {
        "str_param": str,
        "bool_param": bool,
        "int_param": int,
        "list_param": list[str],
        "dict_param": dict[str, str],
        "optional_param": str | None,
    }


class UnresolvedRefFakeProvider(AuthProvider):
    # pylint: disable=super-init-not-called
    def __init__(
        self,
        url_param: "UnknownUrlType | str | None" = None,  # noqa: F821
        generic_param: "UnknownGeneric[int] | None" = None,  # noqa: F821
        module_param: "unknown_module.Timeout | None" = None,  # noqa: F821
        invalid_param: "1 +" = None,  # noqa: F722
        partial_param: list["UnknownType"] | None = None,  # noqa: F821
        list_param: list[str] | None = None,
        bool_param: bool = False,
        client_storage: "UnknownStore | None" = None,  # noqa: F821
        **kwargs: "UnknownKwargs",  # noqa: F821
    ) -> None:
        pass


_UNRESOLVED_PARAMS = ["url_param", "generic_param", "module_param", "invalid_param"]


def test__get_constructor_type_hints_unresolved(caplog) -> None:
    """
    Annotations that cannot be resolved are left out of the hints and logged,
    while all other parameters keep their types. A partially resolved annotation
    is kept and logged.
    """
    with caplog.at_level(logging.WARNING):
        hints = _get_constructor_type_hints(UnresolvedRefFakeProvider)
    assert hints == {
        "partial_param": list["UnknownType"] | None,  # noqa: F821
        "list_param": list[str] | None,
        "bool_param": bool,
    }
    for param in [*_UNRESOLVED_PARAMS, "partial_param"]:
        assert param in caplog.text
    # The warnings include the errors.
    for error_type in ["NameError", "SyntaxError"]:
        assert error_type in caplog.text
    assert "client_storage" not in caplog.text
    assert "kwargs" not in caplog.text


def test__build_provider_info_from_type_unresolved() -> None:
    """
    Parameters with unresolved annotations are parsed as strings. A parameter
    with a partially resolved list annotation is parsed as a list of strings.
    """
    info = _build_provider_info_from_type(UnresolvedRefFakeProvider)
    converters = {p.name: p.conv for p in info.parameters}
    for param in _UNRESOLVED_PARAMS:
        assert converters[param](" v, w ") == "v"
    assert converters["partial_param"]("v, w") == ["v", "w"]
    assert converters["list_param"]("a, b") == ["a", "b"]
    assert converters["bool_param"]("false") is False


_TYPE_CHECKING_PROVIDER_SOURCE = """
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydantic import AnyHttpUrl


def make_provider():
    class LocalType:
        pass

    class TypeCheckingFakeProvider(AuthProvider):
        def __init__(
            self,
            url_param: AnyHttpUrl | str | None = None,
            list_param: list[str] | None = None,
            local_param: LocalType | None = None,
            url_list_param: list[AnyHttpUrl] | None = None,
        ) -> None:
            self.kwargs = {
                "url_param": url_param,
                "list_param": list_param,
                "url_list_param": url_list_param,
            }

    return TypeCheckingFakeProvider, LocalType
"""


@pytest.fixture
def type_checking_provider() -> tuple[type, type]:
    """
    A provider defined in a function of a module without
    ``from __future__ import annotations``, that uses a ``TYPE_CHECKING`` import
    and a local type in unquoted annotations. Only Python 3.14+ allows this.
    """
    namespace: dict[str, Any] = {"AuthProvider": AuthProvider}
    # pylint: disable-next=exec-used
    exec(_TYPE_CHECKING_PROVIDER_SOURCE, namespace)  # nosec B102
    return namespace["make_provider"]()


_LAZY_ANNOTATIONS = pytest.mark.skipif(
    sys.version_info < (3, 14),
    reason="Unquoted TYPE_CHECKING annotations need lazy evaluation (Python 3.14+)",
)


@_LAZY_ANNOTATIONS
def test__get_constructor_type_hints_lazy_annotations(type_checking_provider) -> None:
    """
    Only the annotation that is just the ``TYPE_CHECKING`` name is left out.
    The local type is still resolved, and the list of ``TYPE_CHECKING`` names is
    kept as a list.
    """
    provider_type, local_type = type_checking_provider
    hints = _get_constructor_type_hints(provider_type)
    assert hints.keys() == {"list_param", "local_param", "url_list_param"}
    assert hints["list_param"] == list[str] | None
    assert hints["local_param"] == local_type | None
    assert get_origin(get_args(hints["url_list_param"])[0]) is list


@_LAZY_ANNOTATIONS
def test_create_auth_provider_lazy_annotations(
    monkeypatch, type_checking_provider
) -> None:
    """A provider with lazy annotations is configured from the env variables."""
    provider_type, _ = type_checking_provider
    info = _build_provider_info_from_type(provider_type)
    monkeypatch.setenv(f"{info.env_prefix}URL_PARAM", "https://x.example.com")
    monkeypatch.setenv(f"{info.env_prefix}LIST_PARAM", "a, b")
    monkeypatch.setenv(f"{info.env_prefix}URL_LIST_PARAM", "https://a, https://b")
    provider = create_auth_provider(info)
    assert provider.kwargs == {
        "url_param": "https://x.example.com",
        "list_param": ["a", "b"],
        "url_list_param": ["https://a", "https://b"],
    }


def _fastmcp_provider_types() -> list:
    """
    Returns all AuthProvider classes defined in fastmcp.server.auth.providers.
    A module that cannot be imported is reported as a skipped test parameter.
    """
    params: dict[str, Any] = {}

    def skip_module(module_name: str, error: BaseException | None = None) -> None:
        params[module_name] = pytest.param(
            None,
            id=module_name,
            marks=pytest.mark.skip(reason=f"Cannot import: {error!r}"),
        )

    for module_info in pkgutil.walk_packages(
        fastmcp_providers.__path__,
        f"{fastmcp_providers.__name__}.",
        onerror=skip_module,
    ):
        try:
            module = importlib.import_module(module_info.name)
        except Exception as error:  # pylint: disable=broad-exception-caught
            skip_module(module_info.name, error)
            continue
        for obj in vars(module).values():
            if (
                inspect.isclass(obj)
                and issubclass(obj, AuthProvider)
                and obj.__module__ == module.__name__
            ):
                params[f"{obj.__module__}.{obj.__qualname__}"] = pytest.param(
                    obj, id=obj.__qualname__
                )
    return list(params.values())


# Parameters of the fastmcp providers known to have types that cannot be resolved
# at runtime. They are parsed as strings, which is correct for these parameters.
_KNOWN_UNRESOLVED_PARAMS = {
    "AzureProvider": {"resource_base_url"},
}


@pytest.mark.parametrize("provider_type", _fastmcp_provider_types())
def test__get_constructor_type_hints_fastmcp_providers(provider_type) -> None:
    """
    The type hints of every annotated constructor parameter are resolved, except
    for the known unresolvable ones. Catches fastmcp changes that would make a
    parameter be parsed as a string.
    """
    hints = _get_constructor_type_hints(provider_type)
    annotated = {
        name
        for name, param in _get_constructor_signature(provider_type).parameters.items()
        if param.annotation is not inspect.Parameter.empty
        and param.kind not in _SKIP_KINDS
        and name not in _SKIP_PARAMS
    }
    known = _KNOWN_UNRESOLVED_PARAMS.get(provider_type.__qualname__, set())
    assert annotated - known <= hints.keys()


def _consent_provider_types() -> list:
    """
    Returns the fastmcp AuthProvider classes that have the
    require_authorization_consent constructor parameter.
    """
    return [
        param
        for param in _fastmcp_provider_types()
        if param.values[0] is not None
        and "require_authorization_consent"
        in _get_constructor_signature(param.values[0]).parameters
    ]


@pytest.mark.parametrize("provider_type", _consent_provider_types())
@pytest.mark.parametrize(
    ["value", "expected"],
    [("remember", "remember"), ("external", "external"), ("false", False)],
)
def test_require_authorization_consent_fastmcp_providers(
    provider_type, value, expected
) -> None:
    """
    All values of require_authorization_consent are accepted by every fastmcp
    provider that has this parameter.
    """
    info = _build_provider_info_from_type(provider_type)
    param = next(
        p for p in info.parameters if p.name == "require_authorization_consent"
    )
    assert param.conv(value) == expected


class ConsentFakeProvider(AuthProvider):
    # pylint: disable=super-init-not-called
    def __init__(
        self,
        require_authorization_consent: bool | Literal["remember", "external"] = True,
    ) -> None:
        self.require_authorization_consent = require_authorization_consent


def test_get_auth_provider_dynamic_consent_remember(monkeypatch) -> None:
    qualified_name = (
        f"{ConsentFakeProvider.__module__}.{ConsentFakeProvider.__qualname__}"
    )
    prefix = f"FASTMCP_SERVER_AUTH_{ConsentFakeProvider.__name__.upper()}_"
    monkeypatch.setenv(ENV_PROVIDER_TYPE, qualified_name)
    monkeypatch.setenv(f"{prefix}REQUIRE_AUTHORIZATION_CONSENT", "remember")
    provider = get_auth_provider()
    assert isinstance(provider, ConsentFakeProvider)
    assert provider.require_authorization_consent == "remember"


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


class StrOrListFakeProvider(AuthProvider):
    # pylint: disable=super-init-not-called
    def __init__(
        self,
        required_scopes: list[str] | str | None = None,
        audience: str | list[str] | None = None,
    ) -> None:
        self.kwargs = {"required_scopes": required_scopes, "audience": audience}


def test_get_auth_provider_dynamic_str_or_list(monkeypatch) -> None:
    qualified_name = (
        f"{StrOrListFakeProvider.__module__}.{StrOrListFakeProvider.__qualname__}"
    )
    prefix = f"FASTMCP_SERVER_AUTH_{StrOrListFakeProvider.__name__.upper()}_"
    monkeypatch.setenv(ENV_PROVIDER_TYPE, qualified_name)
    monkeypatch.setenv(f"{prefix}REQUIRED_SCOPES", "a,b")
    monkeypatch.setenv(f"{prefix}AUDIENCE", "aud")
    provider = get_auth_provider()
    assert isinstance(provider, StrOrListFakeProvider)
    assert provider.kwargs == {"required_scopes": ["a", "b"], "audience": ["aud"]}


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
