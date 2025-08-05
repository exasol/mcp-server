# Exasol MCP Server

<p align="center">

<a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/pypi/l/exasol_mcp_server" alt="License">
</a>
<a href="https://pypi.org/project/exasol_mcp_server/">
    <img src="https://img.shields.io/pypi/dm/exasol_mcp_server" alt="Downloads">
</a>
<a href="https://pypi.org/project/exasol_mcp_server/">
    <img src="https://img.shields.io/pypi/pyversions/exasol_mcp_server" alt="Supported Python Versions">
</a>
<a href="https://pypi.org/project/exasol_mcp_server/">
    <img src="https://img.shields.io/pypi/v/exasol_mcp_server" alt="PyPi Package">
</a>
</p>

Provides an LLM access to the Exasol database via MCP tools. Includes the
tools for reading the database metadata and executing data reading queries.

## Features

- Collects the metadata.
  * Enumerates the existing database objects, including schemas, tables, views, functions and UDF scripts.
  * Provides a filtering mechanisms to use with object enumeration.
  * Describes the database objects: for tables returns the list of columns and constraints; for functions and scripts - the list of input and output parameters.
- Executes provided data reading SQL query. Disallows any other type of query.

## Prerequisites

- [Python](https://www.python.org/) >= 3.10.
- MCP Client application, e.g. [Claude Desktop](https://claude.ai/download).

## Installation

Ensure the `uv` package is installed. If uncertain call
```bash
uv --version
```
To install `uv`, please follow [the instructions](https://docs.astral.sh/uv/getting-started/installation/)
in the `uv` official documentation.

## Using the server with the Claude Desktop.

To enable the Claude Desktop using the Exasol MCP server, the latter must be listed
in the configuration file `claude_desktop_config.json`.

To find the configuration file, click on the Settings and navigate to the
“Developer” tab. This section contains options for configuring MCP servers and other
developer features. Click the “Edit Config” button to open the configuration file in
the editor of your choice.

Add the Exasol MCP server to the list of MCP servers as shown in this configuration
example.
```json
{
  "mcpServers": {
    "exasol_db": {
      "command": "uvx",
      "args": ["exasol-mcp-server"],
      "env": {
        "EXA_DSN": "demodb.exasol.com:8563",
        "EXA_USER": "my-user-name",
        "EXA_PASSWORD": "my-password"
      }
    },
    "other_server": {}
  }
}
```

With these settings, uv will install and run the "exasol-mcp-package" in an
ephemeral environment, using the default `uv` parameters and default server settings.

## Configuration settings:

Most importantly, the server configuration specifies if reading the data using SQL
queries is enabled. Note that reading is disabled by default. To enable the data
reading, the `enable_read_query` property must be set to true (see the
configuration settings json below).

The server configuration settings can also be used to enable/disable or filter the
listing of a particular type of database objects. Similar settings are defined for
the following object types:
```
schemas,
tables,
views,
functions,
scripts
```
The settings include the following properties:
- `enable`: a boolean flag that enables or disables the listing.
- `like_pattern`: filters the output by applying the specified SQL LIKE condition to
the object name.
- `regexp_pattern`: filters the output by matching the object name with the specified
regular expression.

The settings can be specified using another environment variable - `EXA_MCP_SETTINGS`.
They should be written in the json format. The json text can be set directly as a
value of the environment variable, for example
```json
{"EXA_MCP_SETTINGS": "{\"schemas\": {\"like_pattern\": \"my_schemas\"}"}
```
Note that double quotes in the json text must be escaped, otherwise the environment
variable value will be interpreted, not as a text, but as a part of the outer json.

Alternatively, the settings can be written in a json file. In this case, the
`EXA_MCP_SETTINGS` should contain the path to this file, e.g.
```json
{"EXA_MCP_SETTINGS": "path_to_settings.json"}
```

The following json shows the default configuration settings.
```json
{
  "schemas": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "tables": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "views": {
    "enable": false,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "functions": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "scripts": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "enable_read_query": false
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

### Safe Harbor Statement: Exasol MCP Server & AI Solutions

Exasol’s AI solutions (including MCP Server) are designed to enable intelligent,
autonomous, and highly performant access to data through AI and LLM-powered agents.
While these technologies unlock powerful new capabilities, they also introduce
potentially significant risks.

By granting AI agents access to your database, you acknowledge that the behavior of
large language models (LLMs) and autonomous agents cannot be fully predicted or
controlled. These systems may exhibit unintended or unsafe behavior—including but not
limited to hallucinations, susceptibility to adversarial prompts, and the execution of
unforeseen actions. Such behavior may result in data leakage, unauthorized data
generation, or even data modification or deletion.

Exasol provides the tools to build AI-native workflows; however, you, as the implementer
and system owner, assume full responsibility for managing these solutions within your
environment. This includes establishing appropriate governance, authorization controls,
sandboxing mechanisms, and operational guardrails to mitigate risks to your organization,
your customers, and their data.
