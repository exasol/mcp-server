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
To install `uv` on macOS please use `brew`, i.e.
```bash
brew install uv
```
For other operating systems, please follow [the instructions](https://docs.astral.sh/uv/getting-started/installation/)
in the `uv` official documentation.

## Using the server with the Claude Desktop.

To enable the Claude Desktop using the Exasol MCP server, the latter must be listed
in the configuration file `claude_desktop_config.json`. A similar configuration file
would exist for most other MCP Client applications.

To find the Claude Desktop configuration file, click on the Settings and navigate to the
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
      "args": ["exasol-mcp-server@latest"],
      "env": {
        "EXA_DSN": "my-dsn, e.g. demodb.exasol.com:8563",
        "EXA_USER": "my-user-name",
        "EXA_PASSWORD": "my-password"
      }
    },
    "other_server": {}
  }
}
```

With these settings, `uv` will execute the latest version of the `exasol-mcp-server`
in an ephemeral environment, without installing it.

Alternatively, the `exasol-mcp-server` can be installed using the command:
```bash
uv tool install exasol-mcp-server@latest
```
For further details on installing and upgrading the server using `uv` see the
[uv Tools](https://docs.astral.sh/uv/concepts/tools/) documentation.

If the server is installed, the Claude configuration file should look like this:
```json
{
  "mcpServers": {
    "exasol_db": {
      "command": "exasol-mcp-server",
      "env": "same as above"
    }
  }
}
```

Please note that any changes to the Claude configuration file will only take effect
after restarting Claude Desktop.

## Configuration settings:

The sever is configured using environment variables and optionally a json file. In the
above example the server is provided with the database connection parameters, all other
settings left to default. For the information on how to customise server settings
please see the User Guide.

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
