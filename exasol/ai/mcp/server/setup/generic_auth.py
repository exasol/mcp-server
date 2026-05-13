"""
This module restores the FastMCP v2 mechanism of configuring OAuth2 providers through
environment variables. The provider to use is selected by setting FASTMCP_SERVER_AUTH
to the fully qualified class name of the desired provider.

For FastMCP's generic providers (JWTVerifier, IntrospectionTokenVerifier, OAuthProxy,
RemoteAuthProvider) the env vars are prefixed with EXA_AUTH_ and the provider name
must use the exa.<module>.<Class> convention.

For any other importable AuthProvider subclass (FastMCP built-ins and future providers),
this module dynamically inspects the constructor type annotations to determine the
expected parameters and their types. The env var prefix is derived from the class name
as FASTMCP_SERVER_AUTH_<CLASSNAME>_. For the five providers that were previously
hardcoded, the old FastMCP v2 prefix names are also accepted for backward compatibility.
"""

import csv
import importlib
import inspect
import os
import types as _types
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from io import StringIO
from typing import (
    Any,
    Literal,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from fastmcp.server.auth import (
    AuthProvider,
    OAuthProxy,
    RemoteAuthProvider,
)
from fastmcp.server.auth.auth import TokenVerifier
from fastmcp.server.auth.providers.introspection import IntrospectionTokenVerifier
from fastmcp.server.auth.providers.jwt import JWTVerifier

ENV_PROVIDER_TYPE = "FASTMCP_SERVER_AUTH"
_EXA_ENV_PREFIX = "EXA_AUTH_"


def str_to_list(s) -> list[str]:
    s = s.replace("\n", " ")
    reader = csv.reader(StringIO(s), skipinitialspace=True)
    fields = next(reader)
    return [fld.strip() for fld in fields if fld.strip()]


def str_to_str(s: str) -> str:
    fields = str_to_list(s)
    if fields:
        return fields[0]
    return ""


def str_to_bool(s) -> bool:
    s_lower = str_to_str(s).lower()
    if s_lower in ["true", "yes", "y"]:
        return True
    if s_lower in ["false", "no", "n"]:
        return False
    raise ValueError(f"Invalid boolean parameter: {s}")


def str_to_bool_or_external(s) -> bool | Literal["external"]:
    s_lower = str_to_str(s).lower()
    if s_lower == "external":
        return "external"
    return str_to_bool(s)


def str_to_int(s: str) -> int:
    return int(str_to_str(s))


def _type_to_converter(annotation) -> Callable[[str], Any]:
    """
    Returns the string-to-value converter that best matches a type annotation.

    Handles the common scalar types (str, bool, int), generic collections
    (list, dict), Optional variants, and the special ``bool | Literal["external"]``
    union used by several FastMCP providers. Falls back to ``str_to_str`` for any
    annotation that does not match a known pattern.
    """
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        literal_args = [a for a in args if get_origin(a) is Literal]
        non_none_non_literal = [a for a in non_none if get_origin(a) is not Literal]
        if (
            len(non_none_non_literal) == 1
            and non_none_non_literal[0] is bool
            and literal_args
        ):
            return str_to_bool_or_external
        if type(None) in args and len(non_none) == 1:
            return _type_to_converter(non_none[0])
        return str_to_str

    if hasattr(_types, "UnionType") and isinstance(annotation, _types.UnionType):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _type_to_converter(non_none[0])
        return str_to_str

    if annotation is bool:
        return str_to_bool
    if annotation is int:
        return str_to_int
    if annotation is list or origin is list:
        return str_to_list
    if annotation is dict or origin is dict:
        return str_to_dict
    return str_to_str


def _import_type(qualified_name: str) -> type | None:
    """
    Imports and returns the class identified by ``qualified_name`` (e.g.
    ``"fastmcp.server.auth.providers.google.GoogleProvider"``).
    Returns ``None`` if the module cannot be imported or the class does not exist.
    """
    if "." not in qualified_name:
        return None
    module_name, class_name = qualified_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError):
        return None


def str_to_dict(s) -> dict[str, str]:
    fields = str_to_list(s)
    if (len(fields) % 2) != 0:
        raise ValueError(
            f"Invalid dictionary parameter: {s}. "
            "The dictionary must be provided as a plain list of keys and values."
        )
    return {fields[i]: fields[i + 1] for i in range(0, len(fields) - 1, 2)}


@dataclass
class AuthParameter:
    name: str
    conv: Callable[[str], Any] = lambda v: str_to_str(v)
    env_name: str | None = None


@dataclass
class AuthProviderInfo:
    provider_type: type[AuthProvider]
    parameters: list[AuthParameter]
    provider_name: str | None = None
    env_prefix: str = _EXA_ENV_PREFIX
    legacy_env_prefix: str | None = None


_FASTMCP_V2_PREFIXES: dict[str, str] = {
    "fastmcp.server.auth.providers.auth0.Auth0Provider": "FASTMCP_SERVER_AUTH_AUTH0_",
    "fastmcp.server.auth.providers.aws.AWSCognitoProvider": "FASTMCP_SERVER_AUTH_AWS_COGNITO_",
    "fastmcp.server.auth.providers.azure.AzureProvider": "FASTMCP_SERVER_AUTH_AZURE_",
    "fastmcp.server.auth.providers.google.GoogleProvider": "FASTMCP_SERVER_AUTH_GOOGLE_",
    "fastmcp.server.auth.providers.workos.AuthKitProvider": "FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_",
}


def _build_provider_info_from_type(provider_type: type) -> AuthProviderInfo:
    """
    Builds an ``AuthProviderInfo`` by introspecting the constructor of
    ``provider_type``.

    The env var prefix is derived from the class name as
    ``FASTMCP_SERVER_AUTH_<CLASSNAME>_``.  For the five providers that were
    previously hardcoded (Auth0, AWS Cognito, Azure, Google, AuthKit) the
    corresponding FastMCP v2 prefix is stored in ``legacy_env_prefix`` so that
    old deployments continue to work without reconfiguration.

    Each constructor parameter is mapped to a string converter via
    ``_type_to_converter``. Parameters without a type annotation default to
    ``str_to_str``.
    """
    try:
        hints = get_type_hints(provider_type.__init__)
    except Exception:
        hints = {}

    _skip_kinds = {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
    sig = inspect.signature(provider_type.__init__)
    parameters = [
        AuthParameter(name=pname, conv=_type_to_converter(hints.get(pname, str)))
        for pname, param in sig.parameters.items()
        if pname != "self" and param.kind not in _skip_kinds
    ]

    qualified_name = f"{provider_type.__module__}.{provider_type.__qualname__}"
    return AuthProviderInfo(
        provider_type=provider_type,
        parameters=parameters,
        provider_name=qualified_name,
        env_prefix=f"FASTMCP_SERVER_AUTH_{provider_type.__name__.upper()}_",
        legacy_env_prefix=_FASTMCP_V2_PREFIXES.get(qualified_name),
    )


_generic_providers = [
    AuthProviderInfo(
        provider_type=JWTVerifier,
        parameters=[
            AuthParameter("public_key"),
            AuthParameter("jwks_uri"),
            AuthParameter("issuer"),
            AuthParameter("audience", str_to_list),
            AuthParameter("algorithm"),
            AuthParameter("required_scopes", str_to_list),
            AuthParameter("base_url"),
        ],
    ),
    AuthProviderInfo(
        provider_type=IntrospectionTokenVerifier,
        parameters=[
            AuthParameter("introspection_url"),
            AuthParameter("client_id"),
            AuthParameter("client_secret"),
            AuthParameter("timeout_seconds", str_to_int),
            AuthParameter("required_scopes", str_to_list),
            AuthParameter("base_url"),
        ],
    ),
    AuthProviderInfo(
        provider_type=RemoteAuthProvider,
        parameters=[
            AuthParameter("authorization_servers", str_to_list),
            AuthParameter("base_url"),
            AuthParameter("resource_name"),
            AuthParameter("resource_documentation"),
            AuthParameter("scopes_supported", str_to_list),
        ],
    ),
    AuthProviderInfo(
        provider_type=OAuthProxy,
        parameters=[
            AuthParameter("upstream_authorization_endpoint"),
            AuthParameter("upstream_token_endpoint"),
            AuthParameter("upstream_client_id"),
            AuthParameter("upstream_client_secret"),
            AuthParameter("upstream_revocation_endpoint"),
            AuthParameter("base_url"),
            AuthParameter("redirect_path"),
            AuthParameter("issuer_url"),
            AuthParameter("service_documentation_url"),
            AuthParameter("allowed_client_redirect_uris", str_to_list),
            AuthParameter("valid_scopes", str_to_list),
            AuthParameter("forward_pkce", str_to_bool),
            AuthParameter("token_endpoint_auth_method"),
            AuthParameter("extra_authorize_params", str_to_dict),
            AuthParameter("extra_token_params", str_to_dict),
        ],
    ),
]


def exa_provider_name(provider_type: type[AuthProvider]) -> str:
    return f"exa.{provider_type.__module__}.{provider_type.__qualname__}"


def provider_name(provider_info: AuthProviderInfo) -> str:
    if provider_info.provider_name is not None:
        return provider_info.provider_name
    return exa_provider_name(provider_info.provider_type)


def parameter_env_name(provider_info: AuthProviderInfo, param: AuthParameter) -> str:
    env_name = param.env_name if param.env_name is not None else param.name.upper()
    return f"{provider_info.env_prefix}{env_name}"


def exa_parameter_env_name(param: AuthParameter) -> str:
    # We don't use the class name in the environment variable names. In the future,
    # this can potentially create a name clash between the parameters of JWTVerifier
    # and either OAuthProxy or RemoteAuthProvider. This is very unlikely though,
    # and we will deal with if and when it happens.
    env_name = param.env_name if param.env_name is not None else param.name.upper()
    return f"{_EXA_ENV_PREFIX}{env_name}"


@cache
def _get_generic_provider_map() -> dict[str, AuthProviderInfo]:
    """
    Indexes all known providers by their names as they would apper in the
    FASTMCP_SERVER_AUTH envar.
    """
    return {provider_name(provider): provider for provider in _generic_providers}


@cache
def _get_verifier_map() -> dict[str, AuthProviderInfo]:
    """
    Indexes all known Token Verifiers by their names.
    """
    return {
        provider_name(ver_type): ver_type
        for ver_type in _generic_providers
        if issubclass(ver_type.provider_type, TokenVerifier)
    }


def create_auth_provider(
    provider_info: AuthProviderInfo, **extra_kwargs
) -> AuthProvider:
    """
    Instantiates the provider described by ``provider_info`` by reading its
    parameters from environment variables.

    For each parameter the primary env var name (built from ``env_prefix``) is
    checked first. If it is absent and ``legacy_env_prefix`` is set, the
    corresponding legacy name is tried as a fallback, preserving backward
    compatibility with FastMCP v2 env var naming conventions.
    """
    kwargs = {}
    for param in provider_info.parameters:
        env = parameter_env_name(provider_info, param)
        if env in os.environ:
            kwargs[param.name] = param.conv(os.environ[env])
        elif provider_info.legacy_env_prefix is not None:
            legacy_env = (
                f"{provider_info.legacy_env_prefix}"
                f"{param.env_name if param.env_name is not None else param.name.upper()}"
            )
            if legacy_env in os.environ:
                kwargs[param.name] = param.conv(os.environ[legacy_env])
    return provider_info.provider_type(**kwargs, **extra_kwargs)


def _try_create_auth_provider(
    provider_info: AuthProviderInfo, **extra_kwargs
) -> AuthProvider:
    """
    Normalizes constructor validation across FastMCP versions.

    FastMCP v3 raises ``TypeError`` for missing required keyword-only arguments in
    some auth providers, while earlier versions surfaced ``ValueError`` in the same
    flow. The rest of this module expects provider construction failures caused by
    incomplete configuration to be treated uniformly.
    """
    try:
        return create_auth_provider(provider_info, **extra_kwargs)
    except (TypeError, ValueError) as error:
        raise ValueError("Invalid auth provider configuration.") from error


def get_token_verifier(provider_name: str) -> tuple[TokenVerifier, str]:
    """
    Creates one of the known types of a Token Verifier. This can be either
    the requested Auth Provider itself, or a part of the requested Auth Provider.
    In the latter case, the function will create whatever Verifier it can create
    with the information given in the environment variables. Will raise ValueError
    if it can create none.
    Args:
        provider_name: requested Auth Provider.
    """

    # First check if the requested Auth provider is one of the Token Verifier types:
    provider_map = _get_generic_provider_map()
    verifier_map = _get_verifier_map()
    verifier_type = verifier_map.get(provider_name)
    if verifier_type is not None:
        provider = _try_create_auth_provider(provider_map[provider_name])
        return provider, provider_name

    # If the requested provider is not a Token Verifier than create a Token Verifier
    # that sufficient parameters are provided for.
    for ver_name, ver_type in verifier_map.items():
        try:
            verifier = _try_create_auth_provider(provider_map[ver_name])
            return verifier, ver_name
        except ValueError:
            pass
    raise ValueError(
        "Insufficient parameters to create any of the supported "
        f"Token Verifier types: {verifier_map.keys()}"
    )


def get_auth_provider() -> AuthProvider | None:
    """
    Creates an OAuth2 provider based on the class name stored in
    ``FASTMCP_SERVER_AUTH``.

    EXA generic providers (``exa.<module>.<Class>`` names) are constructed using
    the hardcoded ``_generic_providers`` list and ``EXA_AUTH_`` env var prefix.

    All other class names are handled dynamically: the class is imported, its
    constructor is introspected, and the provider is instantiated from env vars
    prefixed with ``FASTMCP_SERVER_AUTH_<CLASSNAME>_``.  For the five previously
    hardcoded FastMCP built-ins the old v2 prefix names are also accepted.

    Returns ``None`` if ``FASTMCP_SERVER_AUTH`` is unset, the named class cannot
    be imported, or the provider cannot be constructed from the available env vars.
    """
    provider_name = os.environ.get(ENV_PROVIDER_TYPE)
    if not provider_name:
        return None

    generic_provider_map = _get_generic_provider_map()
    if provider_name in generic_provider_map:
        verifier, verifier_name = get_token_verifier(provider_name)
        if provider_name == verifier_name:
            return verifier
        return create_auth_provider(
            generic_provider_map[provider_name], token_verifier=verifier
        )

    provider_type = _import_type(provider_name)
    if provider_type is None:
        return None
    provider_info = _build_provider_info_from_type(provider_type)
    try:
        return _try_create_auth_provider(provider_info)
    except ValueError:
        return None


def get_auth_kwargs() -> dict[str, AuthProvider]:
    """
    A helper function that builds kwargs with an optional `auth` parameter,
    to be used in the MCP server constructor.
    """
    provider = get_auth_provider()
    if provider is None:
        return {}
    return {"auth": provider}
