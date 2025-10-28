# Database Connection Setup

The MCP server supports two user authentication methods available in Exasol database -
password and an OpenID token.

The MCP server can be deployed in two ways: locally or as a remote HTTP server.
In the latter case, the server works in the multiple-user mode, and its tools must be
protected with OAuth2 authorization. Please refer to [OpenID Setup](open_id_setup.md)
guide for details on the OAuth configuration. The choice of the database connection
parameters will likely depend on the MCP server deployment mode. This section of the
User Guide explains possible deployment options in the context of the database
connection and lists the expected environment variables in each of these cases.

## Local MCP Server

In this mode, the server should be configured to use particular database credentials
(username and either password or an OpenID token). Presumably, these will be
Exasol credentials given to the user of an AI Application hosting the MCP server.
The application is running on the user's machine.

| Variable Name                          | Required |
|----------------------------------------|:--------:|
| EXA_DSN (e.g. demodb.exasol.com:8563") |   yes    |
| EXA_USER                               |   yes    |
| EXA_PASSWORD                           |    no    |
| EXA_ACCESS_TOKEN                       |    no    |
| EXA_REFRESH_TOKEN                      |    no    |

One of the EXA_PASSWORD, EXA_ACCESS_TOKEN, EXA_REFRESH_TOKEN must be provided,
depending on how the EXA_USER is identified.

## HTTP MCP Server

This is the multiuser setup. The MCP Server may or may not be able to identify the
user, depending on how server authorization is configured. The username can be stored
in one of the claims in the access token. Most identity providers allow setting a custom
claim or offer a choice of standard claims that can be used for that. The server needs
to know the name of this claim.

Being able to identify the user gives two possibilities for making the database
connection user special.

### Passthrough Access Token

Under certain conditions, the access token can be extracted from the MCP Auth context
and used to open the database connection on behalf of the user calling an MCP tool.
The following two additional conditions must be met:

- All MCP server users are identified by an access token in the Exasol database,
- The database verifies the token with the same identity provider as the MCP server.
  In which case the subject, the user is identified with in the database, should
  match the subject field in the access token issued to this user.

Note that the user has to be identified in the database by an access token, not a
refresh token. Currently, the refresh token identification is not supported. This
doesn't mean the identity provider cannot use a refresh token. If it does, the MCP client
will request access token regeneration when its validity period expires.

| Variable Name                          | Required |
|----------------------------------------|:--------:|
| EXA_DSN (e.g. demodb.exasol.com:8563") |   yes    |
| EXA_USERNAME_CLAIM                     |   yes    |
| EXA_POOL_SIZE                          |    no    |

EXA_USERNAME_CLAIM is where the name of the username claim should be provided.
EXA_POOL_SIZE is the maximum size of the connection pool, defaults to 5.

### User Impersonation

If the users are not identified by access tokens in the database, but their names can be
made visible through a claim, it is possible to make the connection using a separate MCP
Server credentials, with subsequent impersonation of the user. The database queries
executed by the server tools when serving this user's requests will be subject to this
user permissions. For this to work, the server's own credentials must have the
"IMPERSONATE ANY USER" or "IMPERSONATION ON <user/role>" privileges.

| Variable Name                          | Required |
|----------------------------------------|:--------:|
| EXA_DSN (e.g. demodb.exasol.com:8563") |   yes    |
| EXA_USER                               |   yes    |
| EXA_PASSWORD                           |    no    |
| EXA_ACCESS_TOKEN                       |    no    |
| EXA_REFRESH_TOKEN                      |    no    |
| EXA_USERNAME_CLAIM                     |   yes    |
| EXA_POOL_SIZE                          |    no    |

Here, EXA_USER is the server's own username. One of the EXA_PASSWORD, EXA_ACCESS_TOKEN,
EXA_REFRESH_TOKEN must be provided, depending on how the server's username is identified.

### Delegated database access

This is a backup option for the case when the user cannot be identified or the server's
username cannot be granted the impersonation privilege. In principle, it is identical to
the local deployment case. The server tools should still be protected with OAuth
authorization, but as far as the database connection is concerned, this is irrelevant.
To keep the database access integrity, the server's username must have the permission
that is the least common denominator of the permissions of the users allowed to access
the MCP server.
