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
from io import StringIO
from typing import Any

from fastmcp.server.auth import (
    AuthProvider,
    OAuthProxy,
    RemoteAuthProvider,
)
from fastmcp.server.auth.providers.jwt import JWTVerifier

ENV_PROVIDER_TYPE = "FASTMCP_SERVER_AUTH"


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
    if s_lower == "true":
        return True
    if s_lower == "false":
        return False
    raise ValueError(f"Invalid boolean parameter: {s}")


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


@dataclass
class AuthProviderInfo:
    provider_type: type[AuthProvider]
    parameters: list[AuthParameter]


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


def exa_provider_name(provider_type: type[AuthProvider]) -> str:
    return f"exa.{provider_type.__module__}.{provider_type.__qualname__}"


def exa_parameter_env_name(param: AuthParameter) -> str:
    # We don't use the class name in the environment variable names. In the future,
    # this can potentially create a name clash between the parameters of JWTVerifier
    # and either OAuthProxy or RemoteAuthProvider. This is very unlikely though,
    # and we will deal with if and when it happens.
    return f"EXA_AUTH_{param.name.upper()}"


def create_auth_provider(
    provider_info: AuthProviderInfo, **extra_kwargs
) -> AuthProvider:
    kwargs = {
        param.name: param.conv(os.environ[exa_parameter_env_name(param)])
        for param in provider_info.parameters
        if exa_parameter_env_name(param) in os.environ
    }
    return provider_info.provider_type(**kwargs, **extra_kwargs)


def get_auth_provider() -> AuthProvider | None:
    """
    Creates one of FastMCP generic OAuth2 providers, if the correspondent type name is
    set in the FASTMCP_SERVER_AUTH environment variable.
    """
    provider_name = os.environ.get(ENV_PROVIDER_TYPE)
    if not provider_name:
        return None

    provider_map = {
        exa_provider_name(provider.provider_type): provider
        for provider in _generic_providers
    }
    if provider_name not in provider_map:
        return None

    verifier_name = exa_provider_name(JWTVerifier)
    verifier = create_auth_provider(provider_map[verifier_name])
    if provider_name == verifier_name:
        return verifier
    return create_auth_provider(provider_map[provider_name], token_verifier=verifier)


def get_auth_kwargs() -> dict[str, AuthProvider]:
    """
    A helper function that builds kwargs with an optional `auth` parameter,
    to be used in the MCP server constructor.
    """
    provider = get_auth_provider()
    if provider is None:
        return {}
    return {"auth": provider}
