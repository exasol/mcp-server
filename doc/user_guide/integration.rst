.. _integration:

Client Integrations
===================

This page describes how to integrate the Exasol MCP Server with specific AI clients.

.. contents::
   :local:
   :depth: 1

Open Code
---------

`Open Code <https://opencode.ai/docs/>`_ is an AI coding agent that uses MCP tools but does not
automatically download FastMCP skills. The steps below explain how to install Exasol skills into
Open Code's skills directory so agents can load them on demand.

Installing Skills
~~~~~~~~~~~~~~~~~

The ``exasol-install-skills`` command copies Exasol skills to any local directory.
Open Code discovers skills placed in ``.opencode/skills/`` (project-level) or
``~/.config/opencode/skills/`` (global).

**Install bundled skills** (no network required):

.. code-block:: console

    exasol-install-skills --target-dir ~/.config/opencode/skills/

**Install from a remote MCP server** (downloads the latest skills over the network):

.. code-block:: console

    exasol-install-skills \
        --target-dir ~/.config/opencode/skills/ \
        --server-url https://<your-exasol-mcp-server>/mcp

Existing skill directories are always overwritten so that re-running the command
picks up any updates.

Automatic Installation via an Open Code Plugin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Open Code `plugins <https://opencode.ai/docs/plugins/>`_ are JavaScript files that hook into
agent events. Place the following file at ``.opencode/plugins/install-exasol-skills.js``
(project-level) or ``~/.config/opencode/plugins/install-exasol-skills.js`` (global) to install
skills automatically at the start of every session:

.. code-block:: javascript

    // .opencode/plugins/install-exasol-skills.js
    export default function (ctx) {
      return {
        "session:start": async () => {
          await ctx.shell(
            "exasol-install-skills --target-dir ~/.config/opencode/skills/"
          );
        },
      };
    }

To pull from a remote server instead, replace the command with:

.. code-block:: javascript

    await ctx.shell(
      "exasol-install-skills " +
      "--target-dir ~/.config/opencode/skills/ " +
      "--server-url https://<your-exasol-mcp-server>/mcp"
    );
