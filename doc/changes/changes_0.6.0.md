# 0.6.0 - 2025-08-28
## Features

* #43: Modified `find_schemas` and `find_tables`, adding extra information about child objects.
* #45: Added object schema to the output of meta queries.
* #48: Improved keyword search.

## Refactoring

* #41: Moved the `main` and `_register_tools` function to a separate file.
* #44: Extracted meta queries into a separated class with added unit tests.

## Dependency Updates

### `main`
* Updated dependency `fastmcp:2.10.4` to `2.11.3`
* Added dependency `pip:25.2`
* Added dependency `spacy:3.8.6`
* Updated dependency `sqlglot:27.2.0` to `27.8.0`

### `dev`
* Added dependency `exasol-integration-test-docker-environment:4.2.0`
* Updated dependency `exasol-toolbox:1.6.1` to `1.8.0`
