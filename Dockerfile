# Use Python base image
FROM python:3.13-slim AS build

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

WORKDIR /app
COPY --from=build app/dist dist

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

RUN WHEEL=$(ls dist/*.whl) && pip install "${WHEEL}[dynamodb,redis,mongodb]"


# Set entrypoint
ENTRYPOINT ["exasol-mcp-server-http"]
