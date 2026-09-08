# Build a virtualenv using a Debian release matching the distroless runtime
# below, so the venv's internal paths and shebangs resolve correctly once
# copied over. See https://github.com/GoogleContainerTools/distroless/blob/main/python3/README.md
FROM python:3.13-slim-trixie AS build

# Symlink the distroless runtime's python path (/usr/bin/python) to the
# builder's path (/usr/local/bin/python) so the venv created below links
# against the path that will actually exist in the final image.
RUN pip install poetry \
    && ln -s /usr/local/bin/python /usr/bin/python \
    && /usr/bin/python -m venv /venv

WORKDIR /app

# Copy project files
COPY pyproject.toml poetry.lock README.rst ./
COPY exasol/ ./exasol/

# Build the wheel and install it, with its extras, into the venv
RUN poetry build \
    && WHEEL=$(ls dist/*.whl) \
    && /venv/bin/pip install --disable-pip-version-check "${WHEEL}[dynamodb,redis,mongodb]"

# Distroless: no shell, no package manager, no pip in the runtime image.
FROM gcr.io/distroless/python3-debian13:nonroot
WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /venv /venv

# Set entrypoint
ENTRYPOINT ["/venv/bin/exasol-mcp-server-http"]
