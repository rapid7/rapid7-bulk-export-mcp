FROM registry.access.redhat.com/ubi10/python-312-minimal:latest

# The base image already runs as non-root UID 1001, so every writable directory
# has to be created as root with group-writable ownership before dropping back.
#
# Without this the image builds ONLY under BuildKit, which creates a WORKDIR
# owned by the current USER. The classic Docker builder — still what
# `az acr build` uses — creates it owned by root, so `uv sync` fails with
# "failed to create directory /app/.venv: Permission denied".
#
# /data has the same problem but at RUNTIME, not build time, so it fails later
# and looks unrelated: neither builder creates it, and the server's startup
# mkdir (src/mcp_server.py, _DATA_DIR.mkdir) needs write on / as UID 1001. That
# surfaces as a crash-looping replica rather than a build error.
#
# 1001:0 with g+rwX is the OpenShift-compatible form — an arbitrary assigned UID
# still gets write access through the root group.
USER 0
RUN mkdir -p /app /data \
 && chown -R 1001:0 /app /data \
 && chmod -R g+rwX /app /data
USER 1001

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

# Put the virtualenv on PATH.
#
# The ENTRYPOINT below goes through `uv run`, which resolves the venv itself, so
# the serving container works without this. But the Container Apps JOB overrides
# the entrypoint with command: ['rapid7-refresh'] and is executed directly — and
# the console scripts live in /app/.venv/bin, which is NOT on the base image's
# PATH (that is /opt/app-root/bin:...). Without this the job's command cannot be
# resolved, no process starts, and the failure is close to invisible: the job
# reports Running while producing no log output whatsoever, because our code never
# executes to log anything.
ENV PATH="/app/.venv/bin:$PATH"

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
