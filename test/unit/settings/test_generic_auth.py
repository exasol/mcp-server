import pytest
from fastmcp.server.auth import (
    AuthProvider,
    OAuthProxy,
    RemoteAuthProvider,
)
from fastmcp.server.auth.providers.introspection import IntrospectionTokenVerifier
from fastmcp.server.auth.providers.jwt import JWTVerifier

from exasol.ai.mcp.server.setup.generic_auth import (
    ENV_PROVIDER_TYPE,
    AuthParameter,
    AuthProviderInfo,
    create_auth_provider,
    exa_parameter_env_name,
    exa_provider_name,
    get_auth_kwargs,
    get_auth_provider,
    get_token_verifier,
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
def test_get_auth_provider(monkeypatch, provider_type, params) -> None:
    monkeypatch.setenv(ENV_PROVIDER_TYPE, exa_provider_name(provider_type))
    for key, value in params.items():
        monkeypatch.setenv(exa_parameter_env_name(AuthParameter(key)), value)
    provider = get_auth_provider()
    assert isinstance(provider, provider_type)


def test_get_auth_provider_error(monkeypatch) -> None:
    monkeypatch.setenv(ENV_PROVIDER_TYPE, "exa.non_existent_provider")
    assert get_auth_provider() is None


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
