OpenID Setup
============

A production installation of the MCP server may require proper security configuration.
This is certainly the case if the server is deployed as an HTTP server. Then its tools
must be protected with an authorisation mechanism. This section of the User Guide
provides details on how Exasol MCP server can be configured in the remote deployment
scenario, i.e., as an HTTP server.

The MCP supports and recommends OAuth2 as the authorization framework for protecting
the tools and resources from unauthorized access. OAuth2 and OpenID Connect are modern
and widely used specifications for resource protection. Exasol MCP server supports
OAuth2-based authorization to control access to its own tools, as well as the Exasol
database. The authentication options for the database connection are described in
the :doc:`db_connection_setup` guide. This section focuses
on the configuration of the MCP Server authorization.

It is assumed that the reader has some familiarity with the basic concepts of OAuth2.
Without going into details on how OAuth2 works, let us recap what roles exist in
this specification and how they map onto MCP interaction.

The OAuth2 defines four actors:
- The resource with a certain API.
- The client, usually an application that wants to access the resource.
- The resource owner, usually a human, who can authorize the client to access the resource.
- The identity server, a service used to facilitate the authorization.

In the MCP case, the resource API is the server tools, and the client is the MCP client
application, e.g. Claude Desktop.

OAuth in FastMCP
----------------

The implementation of Exasol MCP Server is based on FastMCP package, hence its
authentication layer is based on `FastMCP Authentication <https://gofastmcp.com/servers/auth/authentication>`__.
Exasol MCP Server supports all authentication mechanisms provided by FastMCP at the
moment of the release. However, the FastMCP authentication is currently in active
development.

FastMCP Integration with Identity Providers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

FastMCP provides integrations with several popular OAuth identity providers. For the
current list of supported providers and the configurable parameters for each, refer to
the `FastMCP v3 documentation <https://gofastmcp.com/servers/auth/authentication>`__.

Exasol MCP Server restores the environment variable-based provider configuration that
was available in FastMCP v2. Any FastMCP ``AuthProvider`` subclass can be selected and
configured entirely through environment variables, without writing Python code.

To activate a provider, set ``FASTMCP_SERVER_AUTH`` to its fully qualified class name.
Each constructor parameter is then configured via an environment variable named
``FASTMCP_SERVER_AUTH_<CLASSNAME>_<PARAM>``, where ``<CLASSNAME>`` is the provider class
name in uppercase and ``<PARAM>`` is the parameter name in uppercase. For example:

.. code-block:: shell

    export FASTMCP_SERVER_AUTH=fastmcp.server.auth.providers.workos.AuthKitProvider
    export FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_AUTHKIT_DOMAIN=https://your-project.authkit.app
    export FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_BASE_URL=https://your-server.com

Use the FastMCP v3 documentation page for the chosen provider to find the available
parameters and which ones are required.

For the following five providers, the FastMCP v2 environment variable prefix names are
also accepted for backward compatibility with existing deployments:

+------------------------------------------------------------------+------------------------------------------+
| Provider class                                                   | Legacy v2 prefix                         |
+==================================================================+==========================================+
| ``fastmcp.server.auth.providers.auth0.Auth0Provider``            | ``FASTMCP_SERVER_AUTH_AUTH0_``           |
+------------------------------------------------------------------+------------------------------------------+
| ``fastmcp.server.auth.providers.aws.AWSCognitoProvider``         | ``FASTMCP_SERVER_AUTH_AWS_COGNITO_``     |
+------------------------------------------------------------------+------------------------------------------+
| ``fastmcp.server.auth.providers.azure.AzureProvider``            | ``FASTMCP_SERVER_AUTH_AZURE_``           |
+------------------------------------------------------------------+------------------------------------------+
| ``fastmcp.server.auth.providers.google.GoogleProvider``          | ``FASTMCP_SERVER_AUTH_GOOGLE_``          |
+------------------------------------------------------------------+------------------------------------------+
| ``fastmcp.server.auth.providers.workos.AuthKitProvider``         | ``FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_`` |
+------------------------------------------------------------------+------------------------------------------+

Note that ``FASTMCP_SERVER_AUTH`` should always be set to the full module path of the
provider's class. The generic providers described below still use the Exasol-specific
``EXA_AUTH_*`` environment variables.

FastMCP Generic OAuth Providers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The FastMCP also provides three generic ways to configure the OAuth2 authentication.

* `Remote OAuth <https://gofastmcp.com/servers/auth/remote-oauth>`__,
    to work with identity servers supporting Dynamic Client Registration (DCR).
* `OAuth Proxy <https://gofastmcp.com/servers/auth/oauth-proxy>`__,
    to work with identity servers that do not support DCR.
* `Token Verification <https://gofastmcp.com/servers/auth/token-verification>`__,
    for the case when the MCP server only verifies a bearer token, not being concerned about
    how this token is acquired.

If the chosen identity provider is not in the above list, it should still be possible to
configure the MCP server authentication using one of those generic providers. Please use
`Remote OAuth <https://gofastmcp.com/servers/auth/remote-oauth>`__ if the provider supports
DCR, and `OAuth Proxy <https://gofastmcp.com/servers/auth/oauth-proxy>`__ if it doesn't.

Currently, FastMCP does not define environment variables for generic providers. Exasol
MCP Server fills the gap by providing its own set of variables in a similar fashion.
As with specific providers, the variable ``FASTMCP_SERVER_AUTH`` should be set to the path
of the chosen generic auth provider class. The required value for this variable can be
found in one of the tables below.

Normally, two sets of variables must be configured - one for a Token Verifier and another
one for the provider - Remote OAuth or OAuth Proxy.

The tables below list all possible variables in each set. Note that the column *Required*
means whether the value is mandatory or not in the corresponding FastMCP module. An
optional (for FastMCP) variable may be required by a particular identity provider. One
need to check the provider's specification to find out what information should be supplied.

The rest of this section provides information about the environment variables defined by
Exasol MCP Server.

Token Verification
^^^^^^^^^^^^^^^^^^

Both *Remote OAuth* and *OAuthProxy* require a `Token Verifier <https://gofastmcp.com/servers/auth/token-verification>`__.
The choice of the token verifier depends on the token verification model supported by a
particular identity provider.

FastMCP v3 offers two types of verification -
`JWT Token Verification <https://gofastmcp.com/servers/auth/token-verification#jwt-token-verification>`__
and `Opaque Token Verification <https://gofastmcp.com/servers/auth/token-verification#opaque-token-verification>`__.
The variation of the former is
`Static Public Key Verification <https://gofastmcp.com/servers/auth/token-verification#static-public-key-verification>`__.

A token verifier provider may be the only module needed if the MCP Authentication uses
an externally supplied bearer token. However, this guide doesn't give any details on how
such a system could be configured. Also, in this scenario, we recommend using FastMCP's
`environment variables <https://gofastmcp.com/servers/auth/token-verification>`__, not the Exasol ones.

JWT Token Verification
++++++++++++++++++++++

+--------------------------+----------+-----------------------------------------------------------------------------+
| Variable Name            | Required | Value                                                                       |
+==========================+==========+=============================================================================+
| EXA_AUTH_PUBLIC_KEY      |    no    | For asymmetric algorithms (RS256, ES256, etc.): PEM-encoded public key.     |
|                          |          | For symmetric algorithms (HS256, HS384, HS512): The shared secret string.   |
+--------------------------+----------+-----------------------------------------------------------------------------+
| EXA_AUTH_JWKS_URI        |    no    | URI to fetch JSON Web Key Set (only for asymmetric algorithms).             |
+--------------------------+----------+-----------------------------------------------------------------------------+
| EXA_AUTH_ISSUER          |    no    | Expected issuer claim.                                                      |
+--------------------------+----------+-----------------------------------------------------------------------------+
| EXA_AUTH_AUDIENCE        |    no    | Expected audience claim(s).                                                 |
+--------------------------+----------+-----------------------------------------------------------------------------+
| EXA_AUTH_ALGORITHM       |    no    | JWT signing algorithm. Supported algorithms:                                |
|                          |          | - Asymmetric: RS256/384/512, ES256/384/512, PS256/384/512 (default: RS256). |
|                          |          | - Symmetric: HS256, HS384, HS512.                                           |
+--------------------------+----------+-----------------------------------------------------------------------------+
| EXA_AUTH_REQUIRED_SCOPES |    no    | Required scopes for all tokens.                                             |
+--------------------------+----------+-----------------------------------------------------------------------------+
| EXA_AUTH_BASE_URL        |   yes    | Base URL for TokenVerifier protocol.                                        |
+--------------------------+----------+-----------------------------------------------------------------------------+

Either of ``EXA_AUTH_PUBLIC_KEY`` or ``EXA_AUTH_JWKS_URI`` must be set.

Opaque Token Verification (Token Introspection)
+++++++++++++++++++++++++++++++++++++++++++++++

+----------------------------+----------+-----------------------------------------------------------------------+
| Variable Name              | Required | Value                                                                 |
+============================+==========+=======================================================================+
| EXA_AUTH_INTROSPECTION_URL |   yes    | URL of the OAuth token introspection endpoint.                        |
+----------------------------+----------+-----------------------------------------------------------------------+
| EXA_AUTH_CLIENT_ID         |   yes    | OAuth client ID for authenticating to the introspection endpoint.     |
+----------------------------+----------+-----------------------------------------------------------------------+
| EXA_AUTH_CLIENT_SECRET     |   yes    | OAuth client secret for authenticating to the introspection endpoint. |
+----------------------------+----------+-----------------------------------------------------------------------+
| EXA_AUTH_TIMEOUT_SECONDS   |    no    | HTTP request timeout in seconds (default: 10).                        |
+----------------------------+----------+-----------------------------------------------------------------------+
| EXA_AUTH_REQUIRED_SCOPES   |    no    | Required scopes for all tokens.                                       |
+----------------------------+----------+-----------------------------------------------------------------------+
| EXA_AUTH_BASE_URL          |   yes    | Base URL for TokenVerifier protocol.                                  |
+----------------------------+----------+-----------------------------------------------------------------------+

Remote OAuth
^^^^^^^^^^^^

+---------------------------------+----------+---------------------------------------------------+
| Variable Name                   | Required | Value                                             |
+=================================+==========+===================================================+
| FASTMCP_SERVER_AUTH             |   yes    | exa.fastmcp.server.auth.auth.RemoteAuthProvider   |
+---------------------------------+----------+---------------------------------------------------+
| EXA_AUTH_AUTHORIZATION_SERVERS  |   yes    | List of identity servers that issue valid tokens. |
+---------------------------------+----------+---------------------------------------------------+
| EXA_AUTH_BASE_URL               |   yes    | Base URL of the MCP server.                       |
+---------------------------------+----------+---------------------------------------------------+
| EXA_AUTH_RESOURCE_NAME          |    no    | Name for the protected resource.                  |
+---------------------------------+----------+---------------------------------------------------+
| EXA_AUTH_RESOURCE_DOCUMENTATION |    no    | Documentation URL for the protected resource.     |
+---------------------------------+----------+---------------------------------------------------+

OAuth Proxy
^^^^^^^^^^^

+------------------------------------------+----------+------------------------------------------------------------------------------+
| Variable Name                            | Required | Value                                                                        |
+==========================================+==========+==============================================================================+
| FASTMCP_SERVER_AUTH                      |   yes    | exa.fastmcp.server.auth.oauth_proxy.OAuthProxy                               |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_UPSTREAM_AUTHORIZATION_ENDPOINT |   yes    | URL of upstream authorization endpoint.                                      |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_UPSTREAM_TOKEN_ENDPOINT         |   yes    | URL of upstream token endpoint.                                              |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_UPSTREAM_CLIENT_ID              |   yes    | Client ID registered with upstream server.                                   |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_UPSTREAM_CLIENT_SECRET          |   yes    | Client secret for upstream server.                                           |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_UPSTREAM_REVOCATION_ENDPOINT    |    no    | Optional upstream revocation endpoint.                                       |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_BASE_URL                        |   yes    | Public URL of the MCP server.                                                |
|                                          |          | Redirect path is relative to this URL.                                       |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_REDIRECT_PATH                   |    no    | Redirect path configured in upstream OAuth app.                              |
|                                          |          | Defaults to ``/auth/callback``.                                              |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_ISSUER_URL                      |    no    | Issuer URL for OAuth metadata. Defaults to EXA_AUTH_BASE_URL.                |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_SERVICE_DOCUMENTATION_URL       |    no    | Optional service documentation URL.                                          |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_ALLOWED_CLIENT_REDIRECT_URIS    |    no    | List of allowed redirect URI patterns for MCP clients.                       |
|                                          |          | Patterns support wildcards                                                   |
|                                          |          | (e.g., ``http://localhost:*``, ``https://*.example.com/*``).                 |
|                                          |          | If not set, only localhost redirect URIs are allowed.                        |
|                                          |          | These are for MCP clients performing loopback redirects,                     |
|                                          |          | NOT for the upstream OAuth app.                                              |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_VALID_SCOPES                    |    no    | List of all the possible valid scopes for a client. These are                |
|                                          |          | advertised to clients through the ``/.well-known`` endpoints.                |
|                                          |          | If not set, defaults to EXA_AUTH_REQUIRED_SCOPES.                            |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_FORWARD_PKCE                    |    no    | Whether to forward PKCE to upstream server. Defaults to True.                |
|                                          |          | Enable for providers that support/require PKCE (Google, Azure, AWS, etc.).   |
|                                          |          | Disable only if upstream provider doesn't support PKCE.                      |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_TOKEN_ENDPOINT_AUTH_METHOD      |    no    | Token endpoint authentication method for upstream server.                    |
|                                          |          | Common values: "client_secret_basic","client_secret_post", "none".           |
|                                          |          | If not set, the provider will use default (typically "client_secret_basic"). |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_EXTRA_AUTHORIZE_PARAMS          |    no    | Additional parameters to forward to the upstream authorization endpoint.     |
|                                          |          | Useful for provider-specific parameters like Auth0's "audience".             |
|                                          |          | Example: {"audience": ``"https://api.example.com"``}                         |
+------------------------------------------+----------+------------------------------------------------------------------------------+
| EXA_AUTH_EXTRA_TOKEN_PARAMS              |    no    | Additional parameters to forward to the upstream token endpoint.             |
|                                          |          | Useful for provider-specific parameters during token exchange.               |
+------------------------------------------+----------+------------------------------------------------------------------------------+

OAuth State Storage Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^

OAuth providers such as `OAuth Proxy <https://gofastmcp.com/servers/auth/oauth-proxy>`__
maintain state between requests — active sessions, authorization codes, registered
clients, and token mappings. FastMCP v3 persists this state using an encrypted
`FileTreeStore <https://gofastmcp.com/servers/storage-backends>`__ by default: one file
per entry with atomic writes, which is safe on network filesystems such as AWS EFS or NFS.

Exasol MCP Server exposes the backend selection through the ``EXA_MCP_STORAGE_BACKEND``
environment variable:

+------------------+------------------------------------------------------------------------------+
| Value            | Behaviour                                                                    |
+==================+==============================================================================+
| (unset) or       | **Default.** FastMCP's encrypted FileTreeStore, persisted under              |
| ``filetree``     | ``~/.local/share/fastmcp/oauth-proxy/<fingerprint>/``.                       |
|                  | Safe on network filesystems; sessions survive server restarts.               |
+------------------+------------------------------------------------------------------------------+
| ``memory``       | In-memory store. No files are written; no corruption risk.                   |
|                  | All OAuth sessions are lost when the server restarts.                        |
|                  | Suitable for stateless or ephemeral deployments.                             |
+------------------+------------------------------------------------------------------------------+

.. code-block:: shell

    # Use in-memory storage (e.g. for ephemeral containers):
    export EXA_MCP_STORAGE_BACKEND=memory

For more information on storage backends and advanced options such as Redis or DynamoDB,
see the `FastMCP storage backends documentation <https://gofastmcp.com/servers/storage-backends>`__.

OpenID with SaaS Backend
------------------------

The information in this guide is mostly relevant to the On-Prem backend. The
authentication in SaaS backend is based on OpenID Connect (OIDC), which is an
authentication layer built on top of OAuth. For details please refer to the Exasol
SaaS `Access Management <https://docs.exasol.com/db/latest/administration/access_mngt/access_management.htm>`__
documentation, and in particular to the `Personal Access Token (PAT) <https://docs.exasol.com/db/latest/administration/access_mngt/access_token.htm>`__
section.

Essentially, an Exasol SaaS user is issued a PAT. They need to pass this token to the
MCP Server. This can be done using an HTTP header, as described in the :doc:`db_connection_setup` guide.

Besides that, the MCP server tools can also be protected using one of the authorization
schemas described earlier in this guide, but in case of SaaS backend this is optional.
