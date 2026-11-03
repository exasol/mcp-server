# Unreleased

## Summary

## Bug Fixes

* #309: A pooled connection is no longer discarded when the database rejects a
  query
* #303: Fixed BucketFS tools on Windows and the download of binary files
* #322: Declining an elicitation is now reported as declined rather than cancelled

## Refactorings

* #322: Prepared the elicitation code for the FastMCP 4 migration and added an
  explicit `httpx` dependency
