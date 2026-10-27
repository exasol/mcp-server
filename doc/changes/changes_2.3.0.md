# 2.3.0 - 2026-10-27

## Summary

Replaced the Docker image base and updated the SSL certificate.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| aiohttp | CVE-2026-111063 | 3.14.3 | 3.14.4 |
| aiohttp | CVE-2026-111064 | 3.14.3 | 3.14.4 |
| aiohttp | CVE-2026-111065 | 3.14.3 | 3.14.4 |
| aiohttp | CVE-2026-111066 | 3.14.3 | 3.14.4 |
| aiohttp | CVE-2026-111748 | 3.14.3 | 3.14.4 |
| aiohttp | CVE-2026-111749 | 3.14.3 | 3.14.4 |
| aiohttp | CVE-2026-111750 | 3.14.3 | 3.14.4 |
| aiohttp | GHSA-2g87-pp6c-29x5 | 3.14.3 | 3.14.4 |
| black | CVE-2026-114134 | 26.5.1 | 26.10.1 |
| certifi | PYSEC-2026-4320 | 2026.7.22 | 2026.10.18 |
| certifi | PYSEC-2026-4320 | 2026.7.22 | 2026.10.19 |
| fastmcp | GHSA-7xgj-8392-5q4r | 3.4.7 | 3.4.10 |
| filelock | CVE-2026-112907 | 3.32.4 | 4.2.1 |
| filelock | CVE-2026-112913 | 3.32.4 | 4.2.1 |
| filelock | CVE-2026-112940 | 3.32.4 | 4.2.1 |
| gitpython | CVE-2026-114145 | 3.1.61 | 3.2.1 |
| mcp | CVE-2026-59951 | 1.29.1 | 1.30.0 |
| multidict | PYSEC-2026-4244 | 6.7.1 | 6.9.1 |
| opentelemetry-api | CVE-2026-114603 | 1.44.0 | 1.45.1 |
| pyjwt | GHSA-pxh4-856f-4h89 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4140 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4141 | 2.13.0 | 2.15.0 |
| pyjwt | PYSEC-2026-4142 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4143 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4144 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4145 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4147 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4148 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4149 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4150 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4151 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4152 | 2.13.0 | 2.14.0 |
| pyjwt | PYSEC-2026-4183 | 2.13.0 | 2.15.0 |
| pymongo | PYSEC-2026-4294 | 4.17.0 | 4.18.1 |
| pymongo | PYSEC-2026-4295 | 4.17.0 | 4.18.2 |
| pymongo | PYSEC-2026-4296 | 4.17.0 | 4.18.2 |
| pymongo | PYSEC-2026-4297 | 4.17.0 | 4.18.2 |
| setuptools | GHSA-xmj3-9gf7-h52w | 84.0.0 | 84.1.0 |
| tornado | GHSA-3hv7-mjh2-fv65 | 6.5.8 | 6.5.9 |
| tornado | GHSA-c2m8-h5v5-343r | 6.5.8 | 6.5.9 |
| tornado | GHSA-chx6-46f5-w4vp | 6.5.8 | 6.5.9 |
| urllib3 | PYSEC-2026-4175 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4176 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4177 | 2.7.0 | 2.8.0 |
| virtualenv | CVE-2026-113714 | 21.7.7 | 21.14.9 |
| virtualenv | CVE-2026-113715 | 21.7.7 | 21.14.9 |
| virtualenv | CVE-2026-113717 | 21.7.7 | 21.14.9 |
| virtualenv | CVE-2026-113718 | 21.7.7 | 21.14.9 |
| virtualenv | CVE-2026-113720 | 21.7.7 | 21.14.9 |
| virtualenv | CVE-2026-113722 | 21.7.7 | 21.14.9 |
| virtualenv | CVE-2026-113723 | 21.7.7 | 21.14.9 |
| virtualenv | CVE-2026-113725 | 21.7.7 | 21.14.7 |
| virtualenv | CVE-2026-113726 | 21.7.7 | 21.14.4 |
| virtualenv | CVE-2026-113732 | 21.7.7 | 21.14.2 |
| virtualenv | CVE-2026-113776 | 21.7.7 | 21.14.2 |
| virtualenv | PYSEC-2026-4011 | 21.7.7 | 21.7.12 |
| virtualenv | PYSEC-2026-4012 | 21.7.7 | 21.7.11 |
| virtualenv | PYSEC-2026-4013 | 21.7.7 | 21.7.13 |
| virtualenv | PYSEC-2026-4014 | 21.7.7 | 21.7.12 |
| werkzeug | PYSEC-2026-4312 | 3.1.8 | 3.1.9 |

## Security

* #295: Replaced the Docker image's base with `gcr.io/distroless/python3-debian13`
* #308: Updated SSL certificates signed with eMudhra root certificate

## Refactorings

* #317: Update to exasol-toolbox 12.0.0

## Dependency Updates

### `main`

* Updated dependency `exasol-bucketfs:2.3.0` to `2.3.1`
* Updated dependency `fastmcp:3.4.7` to `3.4.11`
* Updated dependency `numpy:2.5.2` to `2.5.4`
* Updated dependency `pymongo:4.17.0` to `4.19.0`

### `dev`

* Updated dependency `exasol-toolbox:10.5.0` to `12.0.0`
* Updated dependency `pytest-httpserver:1.1.5` to `1.2.0`

### `dynamodb-tests`

* Updated dependency `moto:5.2.3` to `5.2.4`
