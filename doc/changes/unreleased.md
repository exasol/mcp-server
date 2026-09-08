# Unreleased

## Summary

## Security

* #295: Replaced the Docker image's base with `gcr.io/distroless/python3-debian13`.
  The runtime image no longer includes a shell, package manager, or `pip`,
  reducing the OS-level CVE surface flagged by Trivy scans.
