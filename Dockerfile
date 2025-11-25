# Use Python base image
FROM python:3.13-slim

# Install Poetry
RUN pip install poetry

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml poetry.lock README.rst ./
COPY exasol/ ./exasol/

# Configure Poetry to not create virtual env and install dependencies
RUN poetry install --without dev --no-interaction --no-ansi

# Set entrypoint
ENTRYPOINT ["poetry", "run", "exasol-mcp-server-http"]
