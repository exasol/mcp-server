# Build a virtual environment using the Debian version that matches the
# distroless image below, so the venv still works once copied over.
# See https://github.com/GoogleContainerTools/distroless/blob/main/python3/README.md
FROM python:3.13-slim-trixie AS build

# The distroless image's Python is at /usr/bin/python, but this builder
# image's Python is at /usr/local/bin/python. Add a matching /usr/bin/python
# link here so the virtual environment we create points to a path that will
# also exist in the final image.
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

# This distroless base image has no shell, no package manager, and no pip.
FROM gcr.io/distroless/python3-debian13:nonroot
WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /venv /venv

# Set entrypoint
ENTRYPOINT ["/venv/bin/exasol-mcp-server-http"]
