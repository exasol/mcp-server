# 2.1.1 - 2026-09-02

## Summary

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| cryptography | PYSEC-2026-3552 | 49.0.0 | 50.0.0 |
| gitpython | GHSA-9rj7-rf2p-w77r | 3.1.57 | 3.1.58 |
| gitpython | GHSA-4gmw-gg2m-w46p | 3.1.57 | 3.1.58 |
| gitpython | CVE-2026-76217 | 3.1.57 | 3.1.58 |
| gitpython | GHSA-wvpp-8hx9-p66j | 3.1.57 | 3.1.58 |
| gitpython | GHSA-jm78-9fvv-mhgr | 3.1.57 | 3.1.58 |
| pip | PYSEC-2026-3721 | 26.1.2 | 26.2 |
| tornado | GHSA-wwv5-g3v4-889x | 6.5.7 | 6.5.8 |
| tornado | GHSA-8423-8fgw-73vq | 6.5.7 | 6.5.8 |

## Documentation

* #290: Added downloadable example settings files for a minimal metadata
  browsing + select query setup, referenced from `tool_setup.rst`.

## Refactoring

* #276: Batched the metadata queries behind `describe_exasol_tables_and_views`,
  `describe_exasol_custom_functions` and `describe_exasol_user_defined_functions`.

## Security

* Removed `pip` from the Docker image after installing the wheel; its vendored
  SBOM listed `msgpack` and `setuptools` versions flagged by Trivy
  (GHSA-6v7p-g79w-8964, CVE-2025-47273).

## Dependency Updates

### `main`

* Updated dependency `aiofile:3.11.1` to `3.12.3`
* Updated dependency `click:8.3.3` to `8.5.0`
* Updated dependency `exasol-telemetry-client:0.1.5` to `0.1.6`
* Updated dependency `fastmcp:3.4.5` to `3.4.7`
* Updated dependency `numpy:2.5.1` to `2.5.2`
* Updated dependency `redis:8.0.1` to `8.1.0`

### `dev`

* Updated dependency `exasol-toolbox:10.4.0` to `10.5.0`

### `dynamodb-tests`

* Updated dependency `moto:5.2.2` to `5.2.3`
