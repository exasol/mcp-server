# 1.3.0 - 2026-01-16

## Features

* #108: Created a server-side log and added the DB connection info there.
* #111: Added an option to direct logging to the console.
* #116: Made the connection_factory fail if the expected username claim cannot be found.
* #121: Allowed editing generated DDL/DML query in elicitation.
* #123: Added creating a BucketFS connection.
* #107: Added BucketFS file system browsing tools.
* #120: Added tools to read, write and download bucketfs files
* #131: Added tools to delete BucketFS files and directories

## Refactoring

* #125: Extracted helper functions from the main integration test into utilities.

## Security

* #113: Disallowed unauthenticated HTTP by default.

## Documentation

* #129: Update the User Guide with information about BucketFS support.

## Internal
* #117: Updated exasol-toolbox to 4.0.0

## Dependency Updates

### `main`
* Added dependency `aiofile:3.9.0`
* Added dependency `exasol-bucketfs:2.1.0`
* Added dependency `pathvalidate:3.3.1`

### `dev`
* Updated dependency `exasol-toolbox:1.13.0` to `4.0.0`
* Added dependency `pytest-httpserver:1.1.3`
