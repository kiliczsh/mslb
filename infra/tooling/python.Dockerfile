FROM ghcr.io/casey/just:1.59.0@sha256:014cbe6e6de678864004e2fd8b0405f77ce43b20b225e816feb330f2fb575c01 AS just
FROM ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded AS uv
FROM node:24-bookworm-slim@sha256:01d566e8d94d8c619591d78652babfccd3c6992a3dffa9ed5ab39e11cf6c70e9 AS pyright
WORKDIR /opt/mslb-pyright
COPY infra/tooling/pyright/package.json infra/tooling/pyright/package-lock.json ./
RUN --mount=type=cache,id=mslb-npm,target=/root/.npm,sharing=locked \
    npm ci --ignore-scripts --no-audit --no-fund

FROM python:3.13-slim-bookworm@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72 AS python-base
ENV UV_COMPILE_BYTECODE=0 UV_LINK_MODE=copy PYTHONDONTWRITEBYTECODE=1 RUFF_CACHE_DIR=/tmp/ruff
WORKDIR /app

FROM python-base AS metadata
COPY pyproject.toml uv.lock /app/
COPY packages/contracts/pyproject.toml /app/packages/contracts/pyproject.toml
COPY packages/engine/pyproject.toml /app/packages/engine/pyproject.toml
COPY packages/application/pyproject.toml /app/packages/application/pyproject.toml
COPY packages/persistence/pyproject.toml /app/packages/persistence/pyproject.toml
COPY apps/api/pyproject.toml /app/apps/api/pyproject.toml
COPY apps/worker/pyproject.toml /app/apps/worker/pyproject.toml
COPY apps/mock-server/pyproject.toml /app/apps/mock-server/pyproject.toml
COPY apps/container-controller/pyproject.toml /app/apps/container-controller/pyproject.toml

FROM metadata AS tooling-dependencies
COPY --from=uv /uv /usr/local/bin/uv
COPY --from=just /just /usr/local/bin/just
RUN --mount=type=cache,id=mslb-uv,target=/root/.cache/uv,sharing=locked \
    uv sync --locked --all-packages --group dev --no-install-workspace

FROM tooling-dependencies AS checks
ENV UV_CACHE_DIR=/tmp/uv NODE_OPTIONS=--max-old-space-size=768
COPY --from=pyright /usr/local/bin/node /usr/local/bin/node
COPY --from=pyright /opt/mslb-pyright/node_modules /opt/mslb-pyright/node_modules
COPY . /app/
RUN --mount=type=cache,id=mslb-uv,target=/root/.cache/uv,sharing=locked \
    uv sync --locked --all-packages --group dev
COPY docs/assignment/test-data/ /fixtures/
ENV PATH="/app/.venv/bin:$PATH"
RUN mkdir -p /data/exports /data/outputs /data/inputs/.staging /data/inputs/uploads /data/inputs/datasets /data/inputs/snapshots /data/api-state /run/controller-auth /run/guard-auth && chmod 700 /data/api-state && chmod 750 /run/controller-auth /run/guard-auth && chown -R 1000:1000 /data /run/controller-auth /run/guard-auth
USER 1000:1000
CMD ["python", "scripts/check_python.py"]

FROM metadata AS runtime-dependencies
RUN --mount=from=uv,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,id=mslb-uv,target=/root/.cache/uv,sharing=locked \
    uv sync --locked --all-packages --no-dev --no-install-workspace
COPY packages/ /app/packages/
COPY apps/api/ /app/apps/api/
COPY apps/worker/ /app/apps/worker/
COPY apps/mock-server/ /app/apps/mock-server/
COPY apps/container-controller/ /app/apps/container-controller/
RUN --mount=from=uv,source=/uv,target=/usr/local/bin/uv \
    --mount=type=cache,id=mslb-uv,target=/root/.cache/uv,sharing=locked \
    uv sync --locked --all-packages --no-dev

FROM python-base AS runtime
COPY --from=runtime-dependencies /app/.venv /app/.venv
COPY --from=runtime-dependencies /app/packages /app/packages
COPY --from=runtime-dependencies /app/apps /app/apps
COPY scripts/validate_run.py /app/scripts/validate_run.py
COPY docs/assignment/test-data/core/ /fixtures/core/
ENV PATH="/app/.venv/bin:$PATH"
RUN mkdir -p /data/exports /data/outputs /data/inputs/.staging /data/inputs/uploads /data/inputs/datasets /data/inputs/snapshots /data/api-state /run/controller-auth /run/guard-auth && chmod 700 /data/api-state && chmod 750 /run/controller-auth /run/guard-auth && chown -R 1000:1000 /data /run/controller-auth /run/guard-auth
USER 1000:1000
CMD ["python", "-m", "mslb_api"]
