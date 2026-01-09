# Unreleased

## Features

* #108: Created a server-side log and added the DB connection info there.
* #111: Added an option to direct logging to the console.
* #116: Made the connection_factory fail if the expected username claim cannot be found.
* #121: Allowed editing generated DDL/DML query in elicitation.
* #123: Added creating a BucketFS connection.
* #107: Added BucketFS file system browsing tools.

## Refactoring

* #125: Extracted helper functions from the main integration test into utilities.

## Refactoring

* #125: Extracted helper functions from the main integration test into utilities.

## Security

* #113: Disallowed unauthenticated HTTP by default.

## Internal
* #117: Updated exasol-toolbox to 4.0.0
