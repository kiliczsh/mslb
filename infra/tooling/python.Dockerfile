FROM ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded AS uv
FROM python:3.13-slim-bookworm@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72 AS tooling-base
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=0 UV_LINK_MODE=copy PYTHONDONTWRITEBYTECODE=1 RUFF_CACHE_DIR=/tmp/ruff
WORKDIR /app
FROM tooling-base AS checks
COPY . /app/
RUN uv sync --locked --all-packages --group dev
COPY docs/assignment/test-data/ /fixtures/
ENV PATH="/app/.venv/bin:$PATH"
RUN mkdir -p /data/exports /data/outputs /data/inputs/.staging /data/inputs/uploads /data/inputs/datasets /data/inputs/snapshots /data/api-state && chmod 700 /data/api-state && chown -R 1000:1000 /data
USER 1000:1000
CMD ["python", "scripts/check_python.py"]
