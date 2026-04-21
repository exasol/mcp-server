"""
This module allows to create and use one of generic OAuth providers included in FastMCP.

FastMCP also supports a number of specific identity providers, e.g. Google or Auth0.
It offers a mechanism of configuring one of these providers using a set of defined
environment variables. The selection of the provider the MCP server is configured with
is defined in the environment variable FASTMCP_SERVER_AUTH.

For some reason FastMCP chose not to extend this mechanism on their generic providers.
This module does just that. It allows configuring one the three providers - JWTVerifier,
OAuthProxy, RemoteAuthProvider. To be precise, JWTVerifier can be configured using the
FastMCP environment variables, but because it is a part of the other two it is included
here as well. All optional parameters of these providers are supported, without giving
much thought on their usefulness in our use case.

If at any point in time FastMCP defines environment variables for the generic providers,
this module can be retired.

We deliberately use our own prefixes in the names of environment variables. If FastMCP
defines environment variables for the generic providers they can be used immediately
without waiting for this module to be removed.
"""

import csv
import os
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from io import StringIO
from typing import (
    Any,
    Literal,
)

from fastmcp.server.auth import (
    AuthProvider,
    OAuthProxy,
    RemoteAuthProvider,
)
from fastmcp.server.auth.auth import TokenVerifier
from fastmcp.server.auth.providers.auth0 import Auth0Provider
from fastmcp.server.auth.providers.aws import AWSCognitoProvider
from fastmcp.server.auth.providers.azure import AzureProvider
from fastmcp.server.auth.providers.google import GoogleProvider
from fastmcp.server.auth.providers.introspection import IntrospectionTokenVerifier
from fastmcp.server.auth.providers.jwt import JWTVerifier
from fastmcp.server.auth.providers.workos import AuthKitProvider

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

_builtin_providers = [
    AuthProviderInfo(
        provider_type=Auth0Provider,
        provider_name="fastmcp.server.auth.providers.auth0.Auth0Provider",
        env_prefix="FASTMCP_SERVER_AUTH_AUTH0_",
        parameters=[
            AuthParameter("config_url"),
            AuthParameter("client_id"),
            AuthParameter("client_secret"),
            AuthParameter("audience"),
            AuthParameter("base_url"),
            AuthParameter("resource_base_url"),
            AuthParameter("issuer_url"),
            AuthParameter("required_scopes", str_to_list),
            AuthParameter("redirect_path"),
            AuthParameter("allowed_client_redirect_uris", str_to_list),
            AuthParameter("require_authorization_consent", str_to_bool_or_external),
            AuthParameter("consent_csp_policy"),
            AuthParameter("forward_resource", str_to_bool),
        ],
    ),
    AuthProviderInfo(
        provider_type=AWSCognitoProvider,
        provider_name="fastmcp.server.auth.providers.aws.AWSCognitoProvider",
        env_prefix="FASTMCP_SERVER_AUTH_AWS_COGNITO_",
        parameters=[
            AuthParameter("user_pool_id"),
            AuthParameter("aws_region"),
            AuthParameter("client_id"),
            AuthParameter("client_secret"),
            AuthParameter("base_url"),
            AuthParameter("resource_base_url"),
            AuthParameter("issuer_url"),
            AuthParameter("redirect_path"),
            AuthParameter("required_scopes", str_to_list),
            AuthParameter("allowed_client_redirect_uris", str_to_list),
            AuthParameter("require_authorization_consent", str_to_bool_or_external),
            AuthParameter("consent_csp_policy"),
            AuthParameter("forward_resource", str_to_bool),
        ],
    ),
    AuthProviderInfo(
        provider_type=AzureProvider,
        provider_name="fastmcp.server.auth.providers.azure.AzureProvider",
        env_prefix="FASTMCP_SERVER_AUTH_AZURE_",
        parameters=[
            AuthParameter("client_id"),
            AuthParameter("client_secret"),
            AuthParameter("tenant_id"),
            AuthParameter("required_scopes", str_to_list),
            AuthParameter("base_url"),
            AuthParameter("resource_base_url"),
            AuthParameter("identifier_uri"),
            AuthParameter("issuer_url"),
            AuthParameter("redirect_path"),
            AuthParameter("additional_authorize_scopes", str_to_list),
            AuthParameter("allowed_client_redirect_uris", str_to_list),
            AuthParameter("require_authorization_consent", str_to_bool_or_external),
            AuthParameter("consent_csp_policy"),
            AuthParameter("forward_resource", str_to_bool),
            AuthParameter("base_authority"),
            AuthParameter("enable_cimd", str_to_bool),
        ],
    ),
    AuthProviderInfo(
        provider_type=GoogleProvider,
        provider_name="fastmcp.server.auth.providers.google.GoogleProvider",
        env_prefix="FASTMCP_SERVER_AUTH_GOOGLE_",
        parameters=[
            AuthParameter("client_id"),
            AuthParameter("client_secret"),
            AuthParameter("base_url"),
            AuthParameter("resource_base_url"),
            AuthParameter("issuer_url"),
            AuthParameter("redirect_path"),
            AuthParameter("required_scopes", str_to_list),
            AuthParameter("valid_scopes", str_to_list),
            AuthParameter("timeout_seconds", str_to_int),
            AuthParameter("allowed_client_redirect_uris", str_to_list),
            AuthParameter("require_authorization_consent", str_to_bool_or_external),
            AuthParameter("consent_csp_policy"),
            AuthParameter("forward_resource", str_to_bool),
            AuthParameter("extra_authorize_params", str_to_dict),
            AuthParameter("enable_cimd", str_to_bool),
        ],
    ),
    AuthProviderInfo(
        provider_type=AuthKitProvider,
        provider_name="fastmcp.server.auth.providers.workos.AuthKitProvider",
        env_prefix="FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_",
        parameters=[
            AuthParameter("authkit_domain"),
            AuthParameter("base_url"),
            AuthParameter("resource_base_url"),
            AuthParameter("required_scopes", str_to_list),
            AuthParameter("scopes_supported", str_to_list),
            AuthParameter("resource_name"),
            AuthParameter("resource_documentation"),
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
def _get_builtin_provider_map() -> dict[str, AuthProviderInfo]:
    return {provider_name(provider): provider for provider in _builtin_providers}


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
    kwargs = {
        param.name: param.conv(os.environ[parameter_env_name(provider_info, param)])
        for param in provider_info.parameters
        if parameter_env_name(provider_info, param) in os.environ
    }
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
    Creates one of FastMCP generic OAuth2 providers, if the correspondent type name is
    set in the FASTMCP_SERVER_AUTH environment variable. Will raise ValueError if the
    requested provider type is unknown or no Token Verifier can be created.
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

    builtin_provider_map = _get_builtin_provider_map()
    if provider_name not in builtin_provider_map:
        return None

    return _try_create_auth_provider(builtin_provider_map[provider_name])


def get_auth_kwargs() -> dict[str, AuthProvider]:
    """
    A helper function that builds kwargs with an optional `auth` parameter,
    to be used in the MCP server constructor.
    """
    provider = get_auth_provider()
    if provider is None:
        return {}
    return {"auth": provider}
