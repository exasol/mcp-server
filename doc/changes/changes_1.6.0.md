# 1.6.0 - 2026-03-30

## Features

* #147: Added health endpoint.

## Refactorings

* #151: Sorted out the source files and tests into directories.
* #158: Refined tool's input parameter signatures.
* #160: Changed the return type schema for the DB tools.
* #163: Changed the return type schema for BucketFS tools.
* #164: Changed the return type schema for built-in functions.
* #168: Updated tool names and descriptions.
* #170: Added tool list to the User Guide.
* #172: Made the slow tests running on only one version of Python - 3.12.
* #178: Migrated to Python Toolbox 6.0.

## Dependency Updates

### `main`

* Updated dependency `exasol-bucketfs:2.1.0` to `2.2.0`
* Updated dependency `exasol-saas-api:2.6.0` to `2.9.0`
* Updated dependency `fastmcp:2.13.3` to `2.14.5`

### `dev`

* Updated dependency `exasol-toolbox:4.0.0` to `6.0.0`
* Updated dependency `pytest-exasol-backend:1.2.4` to `1.4.0`
* Updated dependency `pytest-httpserver:1.1.3` to `1.1.5`
