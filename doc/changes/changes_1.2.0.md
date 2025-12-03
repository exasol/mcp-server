# 1.2.0 - 2025-12-03
## Features

* #84: Added support for SaaS backend.
* #87: Added support for DML and DDL queries.
* #86: Added TLS/SSL parameters to the connection factory.
* #96: Added a workflow for creating a docker image.
* #92: Added support for OAuth introspection tokens.

## Refactoring

* #90: Disabled OIDC tests with OAuthProxy.

## Documentation

* #94: Moved the documentation from md to rst.
* #98: Added deployment section to the User Guide.

## Dependency Updates

### `main`
* Added dependency `click:8.3.0`
* Added dependency `exasol-saas-api:2.4.0`
* Updated dependency `fastmcp:2.10.4` to `2.13.0.2`
* Added dependency `numpy:2.2.0`
* Updated dependency `pyexasol:0.27.0` to `1.2.2`
* Added dependency `rank-bm25:0.2.2`
* Updated dependency `sqlglot:27.2.0` to `27.29.0`
* Added dependency `stopwords:1.0.2`

### `dev`
* Added dependency `exasol-integration-test-docker-environment:4.4.1`
* Updated dependency `exasol-toolbox:1.6.1` to `1.13.0`
* Added dependency `flask-oidc:2.4.0`
* Added dependency `oidc-provider-mock:0.2.9`
* Added dependency `pytest-exasol-backend:1.2.2`
* Updated dependency `pytest-exasol-extension:0.2.3` to `0.2.4`
