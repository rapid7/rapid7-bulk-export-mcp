FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies first for better layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code
COPY src/ ./src/
COPY run_server.py ./
RUN uv sync --frozen --no-dev

# Data directory for the DuckDB files (mountable volume)
RUN mkdir -p /data && chmod 700 /data

# Default environment for containerized HTTP mode
ENV MCP_TRANSPORT=http
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000

EXPOSE 8000

# Run as non-root user
RUN useradd -r -s /bin/false mcpuser && chown -R mcpuser:mcpuser /app /data
USER mcpuser

ENTRYPOINT ["uv", "run", "run_server.py", "/data/rapid7_bulk_export.db"]
