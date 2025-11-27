Exasol MCP Server
=================

Provides an LLM access to the Exasol database via MCP tools. Includes the
tools for reading the database metadata and executing data reading queries.

.. image:: https://img.shields.io/pypi/l/exasol-mcp-server
   :target: https://opensource.org/licenses/MIT
   :alt: License

.. image:: https://img.shields.io/pypi/dm/exasol-mcp-server
   :target: https://pypi.org/project/exasol-mcp-server/
   :alt: Downloads

.. image:: https://img.shields.io/pypi/pyversions/exasol-mcp-server
   :target: https://pypi.org/project/exasol-mcp-server/
   :alt: Supported Python Versions

.. image:: https://img.shields.io/pypi/v/exasol-mcp-server
   :target: https://pypi.org/project/exasol-mcp-server/
   :alt: PyPi Package

🚀 Features
-----------

* Collects the metadata.

  * Enumerates the existing database objects, including schemas, tables, views, functions and UDF scripts.
  * Provides a filtering mechanisms to use with object enumeration.
  * Describes the database objects: for tables returns the list of columns and constraints; for functions and scripts - the list of input and output parameters.
  * Enables keyword search of database objects.

* Executes provided SQL query.

🔌️ Prerequisites
-----------------

* `Python <https://www.python.org/>`__ >= 3.10
* MCP Client application, e.g. `Claude Desktop <https://claude.ai/download>`__

💾 Installation
---------------

Ensure the *uv* package is installed. If uncertain call

.. code-block:: shell

    uv --version

To install *uv* on macOS please use *brew*, i.e.

.. code-block:: shell

    brew install uv

For other operating systems, please follow `the instructions <https://docs.astral.sh/uv/getting-started/installation/>`__
in the *uv* official documentation.

🧠 Using the server with the Claude Desktop
-------------------------------------------

To enable the Claude Desktop using the Exasol MCP server, the latter must be listed
in the configuration file *claude_desktop_config.json*. A similar configuration file
would exist for most other MCP Client applications.

To find the Claude Desktop configuration file, click on the Settings and navigate to the
“Developer” tab. This section contains options for configuring MCP servers and other
developer features. Click the “Edit Config” button to open the configuration file in
the editor of your choice.

Add the Exasol MCP server to the list of MCP servers as shown in this configuration
example.

.. code-block:: json

    {
        "mcpServers": {
            "exasol_db": {
                "command": "uvx",
                "args": ["exasol-mcp-server@latest"],
                "env": {
                    "EXA_DSN": "my-dsn",
                    "EXA_USER": "my-user-name",
                    "EXA_PASSWORD": "my-password"
                }
            }
        }
    }


With these settings, *uv* will execute the latest version of the `exasol-mcp-server`
in an ephemeral environment, without installing it.

Alternatively, the `exasol-mcp-server` can be installed using the command:

.. code-block:: shell

   uv tool install exasol-mcp-server@latest

For further details on installing and upgrading the server using *uv* see the
`uv Tools <https://docs.astral.sh/uv/concepts/tools/>`__ documentation.

If the server is installed, the Claude configuration file should look like this:

.. code-block:: json

    {
        "mcpServers": {
            "exasol_db": {
                "command": "exasol-mcp-server",
                "env": "same as above"
            }
        }
    }

Please note that any changes to the Claude configuration file will only take effect
after restarting Claude Desktop.

🟠 🟢 Running modes
-------------------

The MCP server can be deployed either locally, as described above, or as a remote HTTP
server. To run the server as a Direct HTTP Server execute the command:

.. code-block:: shell

    exasol-mcp-server-http --host <server-host> --port <server-port>

The *host* defaults to 0.0.0.0.

This command provides a simple way to verify the setup for a remote MCP Server deployment.
For the production environment, one might consider using an ASGI server like Unicorn. The
most flexible approach is implementing a wrapper for the Exasol MCP server that will
provide the desired control options. For further information and ideas, please check the
`HTTP Deployment <https://gofastmcp.com/deployment/http>`__ in the FastMCP documentation.

Here is an example code creating the Exasol MCP server from a wrapper.

.. code-block:: python

    from exasol.ai.mcp.server import mcp_server

    exasol_mcp = mcp_server()


🔧 Configuration settings
-------------------------

The server is configured using environment variables and optionally a json file. In the
above example, the server is provided with the database connection parameters, all other
settings left to default. For the information on how to customize server settings
please see the `Server Setup <https://exasol.github.io/mcp-server/main/user_guide/server_setup.html>`_
in the User Guide.

📜 License
----------

This project is licensed under the MIT License - see the LICENSE file for details.

Safe Harbor Statement: Exasol MCP Server & AI Solutions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

📚 Documentation
----------------

For further details, check out the latest `documentation <https://exasol.github.io/mcp-server/>`_.
