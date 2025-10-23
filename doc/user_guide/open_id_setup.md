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
the specification and how they map onto MCP interaction.

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
active development. Exasol MCP Server will inevitably fall behind at certain point.

The FastMCP provides t
