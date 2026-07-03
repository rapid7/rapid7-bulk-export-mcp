FROM registry.access.redhat.com/ubi10/python-312-minimal:latest

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code and install
COPY src/ ./src/
COPY run_server.py ./
RUN uv sync --frozen --no-dev --no-editable --compile-bytecode

# Default environment for containerized HTTP mode
ENV MCP_TRANSPORT=http
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8000

# Read-only filesystem hardening
ENV DATA_DIR=/data
ENV TMPDIR=/tmp
ENV UV_NO_CACHE=1
ENV PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

# Image already runs as non-root user (UID 1001)

ENTRYPOINT ["uv", "run", "--no-sync", "run_server.py", "/data/rapid7_bulk_export.db"]
