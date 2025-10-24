# OpenID Setup

A production installation of the MCP server may require proper security configuration.
This is certainly the case if the server is deployed as an HTTP server. Then its tools
must be protected with an authorisation mechanism. This section of the User Guide
provides details on how Exasol MCP server can be configured in the remote deployment
scenario, i.e. as an HTTP server.

The MCP supports and recommends OAuth2 as the authorization framework for protecting
the tools and resources from unauthorized access. OAuth2 and OpenID Connect are modern
and widely used specifications for resource protection. Exasol MCP server supports
OAuth2-based authorization to control access to its own tools, as well as the Exasol
database. The authentication options for the database connection are described in
the [Database Connection Setup](db_connection_setup.md) guide. This section focuses
on the configuring MCP Server authorization.

It is assumed that the reader has some familiarity with the basic concepts of OAuth2.
Without going into details on how the OAuth2 works, lets us recap what roles exist in
this specification and how they map onto MCP interaction.

The OAuth2 defines four actors:
- The resource with a certain API.
- The client, usually an application that wants to access the resource.
- The resource owner, usually a human, that can authorize the client to access the resource.
- The identity server, a service used to facilitate the authorization.

In the MCP case, the resource API are the server tools, and the client is the MCP client
application, e.g. Claude Desktop.

The implementation of Exasol MCP Server is based on FastMCP package, hence its
authentication layer is based on [FastMCP Authentication](https://gofastmcp.com/servers/auth/authentication).
Exasol MCP Server supports all authentication mechanisms provided by FastMCP at the
moment of the release. However, at the time of writing, FastMCP authentication is in
active development. Exasol MCP Server will likely be behind at certain points.

The FastMCP provides three generic ways to configure the OAuth2 authentication.
- [Remote OAuth](https://gofastmcp.com/servers/auth/remote-oauth), to work with identity
servers supporting Dynamic Client Registration (DCR).
- [OAuth Proxy](https://gofastmcp.com/servers/auth/oauth-proxy), to work with identity
servers that do not support DCR.
- [Token Verification](https://gofastmcp.com/servers/auth/token-verification), for the
case when the MCP server only verifies a bearer token, not being concerned how this
token is acquired.

The FastMCP also provides an integration with a number of popular OAuth2 providers.
At the time of writing, the following providers are supported:
- [Auth0](https://gofastmcp.com/integrations/auth0)
- [AuthKit](https://gofastmcp.com/integrations/authkit)
- [AWS Cognito](https://gofastmcp.com/integrations/aws-cognito)
- [Azure](https://gofastmcp.com/integrations/azure)
- [Descope](https://gofastmcp.com/integrations/descope)
- [GitHub](https://gofastmcp.com/integrations/github)
- [Scalekit](https://gofastmcp.com/integrations/scalekit)
- [Google](https://gofastmcp.com/integrations/google)
- [WorkOS](https://gofastmcp.com/integrations/workos)

To configure the MCP authentication with any of these providers please set the
environment variables, as described in documentation for a particular provider.
As an example, the following environment variables shall be set when working
with AuthKit:
```bash
export FASTMCP_SERVER_AUTH=fastmcp.server.auth.providers.workos.AuthKitProvider
export FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_AUTHKIT_DOMAIN=https://your-project.authkit.app
export FASTMCP_SERVER_AUTH_AUTHKITPROVIDER_BASE_URL=https://your-server.com
```
Note that the `FASTMCP_SERVER_AUTH` should always be set to the path of the
provider's class.

If the chosen identity provider is not in the list, it should still be possible to
configure the MCP server authentication using one of the generic providers. Please use
[Remote OAuth](https://gofastmcp.com/servers/auth/remote-oauth) if the provider supports
DCR, and [OAuth Proxy](https://gofastmcp.com/servers/auth/oauth-proxy) if it doesn't.
Currently, FastMCP does not define environment variables for generic providers. Exasol
MCP Server fills the gap by providing its own set of variables, in a similar fashion.

| Name          | Required | Default | Meaning |
|---------------|:--------:|---------|---------|
| authorization_servers |   yes    |    |
| base_url      |   yes    |     |
| resource_name |    no    |      $1 |
| resource_documentation |    no    |      $1 |
