# Unreleased

## Features

* #67: Implemented support for OpenID Connect Authentication
* #69: Added an integration test for the Bearer Token authorization mode.
* #72: Tested the verification of the token audience and issuer.
* #73: Added support for all AuthProviders offered by the FastMCP.
* #76: Implemented extraction of the username from the MCP context, where possible. Queries are executed under the extracted username.
* #78: Added an OIDC integration test for the basic connection option.
* #79: Extracted the server builder into a separate function and added HTTP server CLI.
