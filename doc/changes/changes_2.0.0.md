# 2.0.0 - 2026-07-30

## Summary

New range of supported Python versions.

## Features

* #240: Added `enable_list_tools` and `enable_find_tools` configuration flags (both default
  to `true`) to hide the paired `list_xxx` or `find_xxx` metadata tools globally.

## Documentation

* #176: Added design document.

## Refactoring

* #243: Re-enabled check-workflows in `checks.yml` and updated to exasol-toolbox 10.0.0

## Dependencies

* #252: Added support for Python3.14 and dropped Python3.10

## Dependency Updates

### `main`

* Updated dependency `fastmcp:3.4.2` to `3.4.5`
* Updated dependency `numpy:2.2.0` to `2.4.6` (Python 3.11) / `2.5.1` (Python >=3.12)
* Updated dependency `click:8.2.1` to `8.3.3`
* Updated dependency `exasol-bucketfs:2.2.0` to `2.3.0`

### `dev`

* Updated dependency `exasol-toolbox:8.2.0` to `10.4.0`
* Updated dependency `pytest-exasol-backend:1.4.1` to `1.5.1`
* Updated dependency `pytest-exasol-extension:1.0.1` to `1.1.0`
