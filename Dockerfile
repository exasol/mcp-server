# Use Python base image
FROM python:3.13-slim as build

# Install Poetry
RUN pip install poetry

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml poetry.lock README.rst ./
COPY exasol/ ./exasol/

# Build and install the wheel
RUN poetry build

FROM python:3.13-slim
COPY --from=build dist dist

WORKDIR /app
RUN pip install dist/*.whl

# Remove Poetry
RUN pip uninstall --yes poetry

# Set entrypoint
ENTRYPOINT ["exasol-mcp-server-http"]
