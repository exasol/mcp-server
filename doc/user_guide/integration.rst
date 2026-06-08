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
agent events. Place one of the plugin files below at
``.opencode/plugins/install-exasol-skills.js`` (project-level) or
``~/.config/opencode/plugins/install-exasol-skills.js`` (global) to install skills
automatically at the start of every session.

**Option A — via the CLI** (requires the ``exasol-mcp-server`` Python package):

.. code-block:: javascript

    import { execSync } from "node:child_process";
    import { createInterface } from "node:readline";

    const TARGET = `${process.env.HOME}/.config/opencode/skills`;

    const pressAnyKey = () =>
      new Promise((resolve) => {
        if (!process.stdin.isTTY) return resolve();
        const rl = createInterface({ input: process.stdin });
        rl.once("line", () => {
          rl.close();
          resolve();
        });
      });

    const InstallExasolPlugins = async () => {
      console.log("[install-exasol-skills] Installing Exasol skills...");
      try {
        execSync(`exasol-install-skills --target-dir ${TARGET}`, {
          stdio: "inherit",
        });
      } catch {
        console.log(
          "[install-exasol-skills] Failed to install skills.",
        );
        console.log(
          "[install-exasol-skills] Press any key to continue loading opencode...",
        );
        await pressAnyKey();
      }
      return {};
    };

    export default InstallExasolPlugins;

**Option B — fetch directly from GitHub** (no Python required):

The skills are plain Markdown files hosted in the public GitHub repository.
Open Code plugins run in a `Bun <https://bun.sh>`_ environment, so the plugin below
can download them with a plain ``fetch`` call — no additional tools needed.

.. code-block:: javascript

    import { createInterface } from "node:readline";
    import { mkdirSync, writeFileSync } from "node:fs";

    const SKILLS = [
      "exasol-sql-dialect",
      "exasol-udfs",
      "exasol-mcp-server",
      "exasol-system-tables",
      "exasol-table-design",
      "exasol-import-export",
      "exasol-virtual-schemas",
    ];
    const BASE_URL =
      "https://raw.githubusercontent.com/exasol/mcp-server/main" +
      "/exasol/ai/mcp/server/skills";
    const TARGET = `${process.env.HOME}/.config/opencode/skills`;

    const pressAnyKey = () =>
      new Promise((resolve) => {
        if (!process.stdin.isTTY) return resolve();
        const rl = createInterface({ input: process.stdin });
        rl.once("line", () => {
          rl.close();
          resolve();
        });
      });

    const downloadSkills = async () => {
      let failed = false;
      console.log("[install-exasol-skills] Installing Exasol skills...");
      mkdirSync(TARGET, { recursive: true });
      for (const skill of SKILLS) {
        try {
          const url = `${BASE_URL}/${skill}/SKILL.md`;
          console.log(`[install-exasol-skills]   fetching ${skill}...`);
          const res = await fetch(url);
          if (res.ok) {
            const text = await res.text();
            mkdirSync(`${TARGET}/${skill}`, { recursive: true });
            writeFileSync(`${TARGET}/${skill}/SKILL.md`, text);
            console.log(`[install-exasol-skills]   ✓ ${skill} installed`);
          } else {
            failed = true;
            console.warn(`[install-exasol-skills]   ✗ ${skill} skipped (HTTP ${res.status})`);
          }
        } catch (err) {
          failed = true;
          console.error(`[install-exasol-skills]   ✗ ${skill} failed:`, err);
        }
      }
      if (failed) {
        console.log("[install-exasol-skills] One or more skills failed to install.");
        console.log("[install-exasol-skills] Press any key to continue loading opencode...");
        await pressAnyKey();
      }
    };

    const InstallExasolPlugins = async () => {
      await downloadSkills();
      return {};
    };

    export default InstallExasolPlugins;

To pin to a specific release instead of ``main``, replace ``main`` in ``BASE_URL``
with the desired tag, e.g. ``refs/tags/1.9.0``.
