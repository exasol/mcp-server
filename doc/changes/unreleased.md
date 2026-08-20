# Unreleased

## Summary

## Security

* Removed `pip` from the Docker image after installing the wheel; its vendored
  SBOM listed `msgpack` and `setuptools` versions flagged by Trivy
  (GHSA-6v7p-g79w-8964, CVE-2025-47273).
