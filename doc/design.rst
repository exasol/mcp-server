.. _design_document:

Design Document: Exasol MCP Server
==================================

.. contents::
   :local:
   :depth: 2

Overview
--------

The Exasol MCP Server is a `Model Context Protocol (MCP)
<https://modelcontextprotocol.io/docs/getting-started/intro>`_ server that gives
Large Language Models (LLMs) structured, read-safe access to an Exasol database.
It exposes database metadata and query execution as MCP tools so that an LLM-powered
agent can explore the database schema and retrieve data without requiring direct
database credentials in the client application.

The server is built on top of `FastMCP <https://gofastmcp.com/getting-started/welcome>`_
and `pyexasol <https://github.com/exasol/pyexasol>`_. It supports both the
on-premises and SaaS Exasol deployments.

High-Level Architecture
-----------------------

.. code-block:: text

    ┌─────────────────────────────────────────────────────────────────┐
    │                        MCP Client (LLM)                         │
    └───────────────────────────────┬─────────────────────────────────┘
                                    │  MCP (stdio / HTTP)
    ┌───────────────────────────────▼─────────────────────────────────┐
    │                     ExasolMCPServer (FastMCP)                   │
    │                                                                 │
    │  ┌──────────────────┐   ┌───────────────────┐   ┌────────────┐  │
    │  │   DB Tools       │   │  BucketFS Tools   │   │ Dialect    │  │
    │  │  (metadata +     │   │  (list/read/write │   │ Tools      │  │
    │  │   query exec.)   │   │   files)          │   │ (built-in  │  │
    │  └────────┬─────────┘   └────────┬──────────┘   │ functions, │  │
    │           │                      │              │ SQL types, │  │
    │  ┌────────▼─────────┐   ┌────────▼──────────┐   │ keywords)  │  │
    │  │   DbConnection   │   │  BucketFS PathLike│   └────────────┘  │
    │  └────────┬─────────┘   └───────────────────┘                   │
    │           │                                                     │
    │  ┌────────▼─────────────────────────────────┐                   │
    │  │         Connection Factory               │                   │
    │  │  (On-Prem / SaaS / OpenID / Impersonate) │                   │
    │  └────────┬─────────────────────────────────┘                   │
    └───────────┼─────────────────────────────────────────────────────┘
                │  pyexasol / exasol-saas-api
    ┌───────────▼─────────────────────────────────────────────────────┐
    │                  Exasol Database (On-Prem or SaaS)              │
    └─────────────────────────────────────────────────────────────────┘

Components
----------

ExasolMCPServer
~~~~~~~~~~~~~~~

``ExasolMCPServer`` (``exasol.ai.mcp.server.tools.mcp_server``) is a
subclass of ``FastMCP``. It owns a ``DbConnection`` and an optional
``BucketFsTools`` instance. Each MCP tool is implemented as a method on this
class and registered via ``FastMCP.tool()`` at startup.

Tool registration is conditional: each tool group can be enabled or disabled
through ``McpServerSettings``. This lets operators expose only the
capabilities that are appropriate for a given deployment.

DbConnection
~~~~~~~~~~~~

``DbConnection`` (``exasol.ai.mcp.server.connection.db_connection``) is a
thin wrapper around ``pyexasol.ExaConnection``. It delegates connection
management to an injected factory and retries transient errors
(``ExaCommunicationError``, ``ExaRuntimeError``, ``ExaAuthError``) up to a
configurable number of attempts.

Metadata queries use ``meta.execute_snapshot`` for consistency; write queries
use the standard ``execute`` path.

Connection Factory
~~~~~~~~~~~~~~~~~~

``get_connection_factory`` (``exasol.ai.mcp.server.connection.connection_factory``)
is the central composition point for database connectivity. It inspects the
environment variables present at startup and selects one of five connection
modes (see `Connection Modes`_ below). It returns a context-manager factory
that ``DbConnection`` calls for every query.

Connections are pooled per user by ``NamedObjectPool`` to avoid the overhead
of re-establishing a database connection for every tool call.

Configuration
~~~~~~~~~~~~~

``McpServerSettings`` (``exasol.ai.mcp.server.setup.server_settings``) is a
Pydantic model read from the ``EXA_MCP_SETTINGS`` environment variable. The
value may be an inline JSON string or a path to a JSON file.

The settings control:

* Which metadata categories are exposed (schemas, tables, views, functions,
  scripts, columns).
* SQL-style and regex name filters applied to metadata listings.
* Whether read queries, write queries, BucketFS reads, and BucketFS writes
  are enabled (all off by default).
* Whether MCP elicitation is used to confirm write queries.
* The natural language used for keyword search.
* Whether object name matching is case-sensitive.

Authentication
~~~~~~~~~~~~~~

HTTP deployments delegate OAuth 2.0 token verification to FastMCP. The module
``exasol.ai.mcp.server.setup.generic_auth`` extends FastMCP's built-in
provider selection to cover the generic providers that FastMCP does not expose
through environment variables: ``JWTVerifier``, ``IntrospectionTokenVerifier``,
``RemoteAuthProvider``, and ``OAuthProxy``.

The provider type is chosen via ``FASTMCP_SERVER_AUTH``; its parameters are
read from ``EXA_AUTH_*`` environment variables.

BucketFS Tools
~~~~~~~~~~~~~~

``BucketFsTools`` (``exasol.ai.mcp.server.tools.bucketfs_tools``) provides
file-system-like access to Exasol BucketFS. The underlying storage is
abstracted by the ``exasol-bucketfs`` library, which supports both on-premises
and SaaS BucketFS backends through the same ``PathLike`` interface.

BucketFS tools are only instantiated when at least one of
``enable_read_bucketfs`` or ``enable_write_bucketfs`` is ``True``.

Dialect Tools
~~~~~~~~~~~~~

The dialect tools (``exasol.ai.mcp.server.tools.dialect_tools``) expose
static knowledge about the Exasol SQL dialect — built-in function categories,
individual function descriptions, SQL data types, system/statistics tables,
and reserved keywords. These tools take information from the database, as well
as from the embedded metadata, and are idempotent.

Connection Modes
----------------

The connection factory supports five modes, selected automatically from the
environment variables that are present:

.. list-table::
   :header-rows: 1
   :widths: 5 20 75

   * - Mode
     - Backend
     - Description
   * - A
     - On-Prem
     - Pre-configured server credentials (username + password or access token).
       Suitable for single-user deployments or when the server's DB user has
       the union of all required permissions.
   * - B
     - On-Prem
     - Username and OpenID access token are extracted from the MCP OAuth
       context. Requires the identity provider to embed the DB username in the
       access token and the database to accept OpenID authentication.
   * - C
     - On-Prem
     - Pre-configured server credentials are used to open the connection; the
       actual user is identified from the token claim and then impersonated via
       ``IMPERSONATE``. Requires the ``IMPERSONATION ON`` privilege.
   * - D
     - SaaS
     - The server's own PAT (Personal Access Token) is pre-configured. The
       SaaS API resolves the PAT to database credentials.
   * - E
     - SaaS
     - The PAT is passed in an HTTP request header on each call. Effectively
       delegates authentication to the SaaS layer.

Metadata Query Design
---------------------

``ExasolMetaQuery`` (``exasol.ai.mcp.server.tools.meta_query``) generates all
metadata SQL using `SQLGlot <https://sqlglot.com/>`_. This ensures that
identifiers are quoted correctly and that the queries conform to the Exasol
SQL dialect. Exasol-specific syntax that SQLGlot does not yet support (e.g.
``GROUP_CONCAT`` with a custom separator) is handled with a targeted
post-processing step.

Metadata results are serialised as lists of Pydantic models
(``exasol.ai.mcp.server.tools.schema.db_output_schema``) before being
returned to the MCP client. This produces stable, self-describing JSON
structures that are easy for an LLM to interpret.

Keyword Search
~~~~~~~~~~~~~~

The ``find_*`` tools rank and filter results by keyword relevance using BM25
(via ``rank-bm25``). Stop words are removed before indexing (via
``stopwords``). The search field combines the object name and its comment.
The language used for stop-word removal is configurable via
``McpServerSettings.language``.

Write Query Safety
~~~~~~~~~~~~~~~~~~

The ``execute_exasol_write_query`` tool uses MCP *elicitation* to present the
proposed DML/DDL statement to the human operator for review and optional
modification before execution. Elicitation can be bypassed for automated
pipelines by setting ``disable_elicitation = true`` in ``McpServerSettings``.

Read queries are validated with SQLGlot to ensure they are ``SELECT``
statements before execution, preventing accidental data modification through
the read-query tool.

Deployment Modes
----------------

Local (stdio)
~~~~~~~~~~~~~

The default mode. The server is launched by the MCP client (e.g. Claude
Desktop) as a subprocess and communicates over standard input/output. No
network port is opened and no authentication is configured. The entry point is
``exasol-mcp-server``.

HTTP Server
~~~~~~~~~~~

The server listens on a TCP port using the Streamable HTTP transport. The
entry point is ``exasol-mcp-server-http``. In this mode:

* Authentication **must** be configured, or the server will refuse to start
  unless ``--no-auth`` is passed explicitly.
* Multiple users can connect concurrently; each user's database connection is
  tracked separately in the connection pool.
* The server exposes a ``/health`` endpoint that executes ``SELECT 1`` to
  confirm database reachability.

For production use, the server should be run behind an ASGI server (e.g.
Uvicorn) or wrapped in a custom application that provides the desired
lifecycle and observability controls.

Key Dependencies
----------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Package
     - Role
   * - ``fastmcp``
     - MCP server framework; handles the MCP protocol, tool routing,
       OAuth 2.0 integration, and HTTP transport.
   * - ``pyexasol``
     - Exasol database driver; executes SQL and streams result sets.
   * - ``exasol-saas-api``
     - Resolves SaaS PATs to database connection parameters via the
       Exasol SaaS REST API.
   * - ``exasol-bucketfs``
     - Unified BucketFS client for on-premises and SaaS backends.
   * - ``sqlglot``
     - SQL generation and parsing; validates read queries and builds
       metadata queries in the Exasol dialect.
   * - ``rank-bm25``
     - BM25 ranking for keyword search across database object names and
       comments.
   * - ``pydantic``
     - Settings validation (``McpServerSettings``) and output schema
       definition.
   * - ``click``
     - CLI argument parsing for the HTTP server entry point.
