FROM ghcr.io/casey/just:1.59.0@sha256:014cbe6e6de678864004e2fd8b0405f77ce43b20b225e816feb330f2fb575c01 AS just
FROM ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded AS uv
FROM python:3.13-slim-bookworm@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72 AS tooling-base
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=0 UV_LINK_MODE=copy PYTHONDONTWRITEBYTECODE=1 RUFF_CACHE_DIR=/tmp/ruff
WORKDIR /app
FROM tooling-base AS checks
COPY --from=just /just /usr/local/bin/just
COPY . /app/
RUN uv sync --locked --all-packages --group dev
COPY docs/assignment/test-data/ /fixtures/
ENV PATH="/app/.venv/bin:$PATH"
RUN mkdir -p /data/exports /data/outputs /data/inputs/.staging /data/inputs/uploads /data/inputs/datasets /data/inputs/snapshots /data/api-state /run/controller-auth /run/guard-auth && chmod 700 /data/api-state && chmod 750 /run/controller-auth /run/guard-auth && chown -R 1000:1000 /data /run/controller-auth /run/guard-auth
USER 1000:1000
CMD ["python", "scripts/check_python.py"]
