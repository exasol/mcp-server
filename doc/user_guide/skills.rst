.. _skills:

Skills
======

The Exasol MCP Server exposes domain knowledge as `FastMCP skills`_. Skills are MCP resources
that LLM clients can discover and pull in on demand, providing background context without
consuming tool-call tokens on every request.

.. _FastMCP skills: https://gofastmcp.com/servers/providers/skills

Available Skills
----------------

The following skills are always available. They are discoverable via the MCP
``resources/list`` request and accessible via ``resources/read``.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Skill URI
     - Description
   * - ``skill://exasol-sql-dialect/SKILL.md``
     - Exasol SQL dialect: data types, syntax differences from standard SQL, date/time
       functions, regex functions, MERGE, CONNECT BY, and common pitfalls.
   * - ``skill://exasol-udfs/SKILL.md``
     - User-Defined Functions and scripts: CREATE SCRIPT syntax, Python/Java/Lua/R
       language options, the ExaIterator API, BucketFS access from scripts, and Script
       Language Containers.
   * - ``skill://exasol-mcp-server/SKILL.md``
     - MCP Server tool inventory and usage guide: what each tool does, which tools
       require configuration flags, and step-by-step workflows for schema exploration,
       query debugging, BucketFS management, and more.
   * - ``skill://exasol-system-tables/SKILL.md``
     - System and statistics tables: the ``EXA_ALL_*`` / ``EXA_DBA_*`` / ``EXA_USER_*``
       prefix guide, key metadata and performance tables, example queries, and advice on
       when to query system tables directly vs using MCP tools.
   * - ``skill://exasol-table-design/SKILL.md``
     - Table design for performance: DISTRIBUTE BY and PARTITION BY selection rules,
       data type choices, replication border, surrogate keys, and CREATE TABLE syntax
       including LIKE, IDENTITY, and combined distribution/partitioning patterns.
   * - ``skill://exasol-import-export/SKILL.md``
     - IMPORT and EXPORT SQL statements: CSV/FBV/Parquet formats, cloud storage
       connections (S3, Azure, GCS), wildcard file loading, error handling with REJECT
       LIMIT, and ETL staging workflows.
   * - ``skill://exasol-virtual-schemas/SKILL.md``
     - Virtual Schemas: connecting external data sources, adapter scripts, supported
       adapters, privilege requirements, and performance considerations.

How Clients Discover Skills
----------------------------

MCP clients that support the FastMCP skills protocol call ``resources/list`` to enumerate
available resources. Resources with URIs of the form ``skill://{name}/SKILL.md`` are skills.
The client can then call ``resources/read`` with that URI to fetch the markdown content.

Claude Code discovers and syncs skills automatically when the MCP server is configured
as a provider. Other clients may use ``fastmcp.utilities.skills.sync_skills()`` or the
equivalent protocol calls.

For clients that do not auto-discover skills (such as Open Code), see :ref:`integration`
for installation instructions and automation examples.
