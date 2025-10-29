# MCP Server Setup

Essentially, the sever is configured using environment variables.

In case of the MCP Sever running locally, under the control of an MCP client application,
the latter usually provides a convenient way of creating an environment from some kind
of configuration file. Below is an example of Claude Desktop configuration file that
references the Exasol MCP Server.

```json
{
  "mcpServers": {
    "exasol_db": {
      "command": "uvx",
      "args": ["exasol-mcp-server@latest"],
      "env": {
        "EXA_DSN": "my-dsn, e.g. demodb.exasol.com:8563",
        "EXA_USER": "my-user-name",
        "EXA_PASSWORD": "my-password",
        "EXA_MCP_SETTINGS": "{\"schemas\": {\"like_pattern\": \"MY_SCHEMA\"}"
      }
    }
  }
}
```

The `env` section of this file lists the environment variables that will be created in
the environment where the MCP Server is going to run.

The environment variables can be divided into three groups.

- [OpenID settings](open_id_setup.md).
- [Database connection settings](db_connection_setup.md)
- [Tool settings](tool_setup.md)

All settings are described in details in respective sections of the User Guide.

## Tool settings

The tool settings are stored in a single variable - `EXA_MCP_SETTINGS`. The settings
are written in the json format. The json string can be set directly in the environment
variable, as shown in the above example. Note that double quotes in the json string must
be escaped, otherwise the environment variable value will be interpreted, not as text,
but as a part of the outer json.

Alternatively, the settings can be written in a json file. In this case, the `EXA_MCP_SETTINGS`
should contain the path to this file, e.g.

```json
{
  "env": {
    "other": "variables",
    "EXA_MCP_SETTINGS": "path_to_settings.json"
  }
}
```

Please see the [Tool Setup](tool_setup.md) for details on how the MCP Server tools
can be customised.
