# 1.10.1 - 2026-06-11

## Bug Fixes

* #237: Fixed the opencode plugin in the user guide.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| aiohttp | CVE-2026-34993 | 3.13.5 | 3.14.0 |
| aiohttp | CVE-2026-47265 | 3.13.5 | 3.14.0 |
| idna | CVE-2026-45409 | 3.13 | 3.15 |
| pip | PYSEC-2026-196 | 26.1.1 | 26.1.2 |
| pyjwt | PYSEC-2026-179 | 2.12.1 | 2.13.0 |
| pyjwt | PYSEC-2026-175 | 2.12.1 | 2.13.0 |
| pyjwt | PYSEC-2026-177 | 2.12.1 | 2.13.0 |
| pyjwt | PYSEC-2026-178 | 2.12.1 | 2.13.0 |
| starlette | PYSEC-2026-161 | 1.0.0 | 1.0.1 |
| starlette | PYSEC-2026-161 | 1.0.0 | 1.0.1 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-141 | 2.6.3 | 2.7.0 |

## Refactoring

* #242: Updated to exasol-toolbox 8.2.0
* #139: Added `# nosec` markers to environment variable name constants to suppress false-positive Sonar security warnings.
* #239: Cleaned up settings removing attributes like `xxx_field` from MetaSettings and derived classes.

## Dependency Updates

### `main`

* Updated dependency `aiofile:3.9.0` to `3.11.1`
* Updated dependency `fastmcp:3.2.4` to `3.4.2`

### `dev`

* Updated dependency `exasol-toolbox:6.4.0` to `8.2.0`

### `dynamodb-tests`

* Updated dependency `moto:5.2.1` to `5.2.2`
