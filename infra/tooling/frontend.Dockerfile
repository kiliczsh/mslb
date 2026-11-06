FROM node:24-bookworm-slim@sha256:01d566e8d94d8c619591d78652babfccd3c6992a3dffa9ed5ab39e11cf6c70e9 AS checks
ENV NODE_OPTIONS=--max-old-space-size=1024 NPM_CONFIG_UPDATE_NOTIFIER=false
ENV XDG_DATA_HOME=/tmp/mslb-test-data
RUN --mount=type=cache,id=mslb-npm,target=/root/.npm,sharing=locked \
    npm install --global --allow-scripts=pnpm pnpm@12.16.1
WORKDIR /app
COPY apps/web/package.json apps/web/pnpm-lock.yaml apps/web/pnpm-workspace.yaml /app/
COPY apps/web/patches/ /app/patches/
RUN --mount=type=cache,id=mslb-pnpm,target=/pnpm/store,sharing=locked \
    pnpm install --frozen-lockfile --store-dir /pnpm/store --package-import-method=copy
COPY apps/web/ /app/
RUN pnpm build
USER node
CMD ["sh", "-c", "pnpm check && pnpm test"]
