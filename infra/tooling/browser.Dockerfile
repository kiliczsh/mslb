FROM node:24-bookworm-slim@sha256:01d566e8d94d8c619591d78652babfccd3c6992a3dffa9ed5ab39e11cf6c70e9
ENV NODE_OPTIONS=--max-old-space-size=512 NPM_CONFIG_UPDATE_NOTIFIER=false
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN --mount=type=cache,id=mslb-npm,target=/root/.npm,sharing=locked \
    npm install --global --allow-scripts=pnpm pnpm@12.16.1
WORKDIR /app
COPY apps/web/package.json apps/web/pnpm-lock.yaml apps/web/pnpm-workspace.yaml /app/
COPY apps/web/patches/ /app/patches/
RUN --mount=type=cache,id=mslb-pnpm,target=/pnpm/store,sharing=locked \
    pnpm install --frozen-lockfile --store-dir /pnpm/store --package-import-method=copy
RUN DEBIAN_FRONTEND=noninteractive pnpm exec playwright install --with-deps --only-shell chromium \
    && rm -rf /var/lib/apt/lists/*
COPY apps/web/ /app/
USER node
CMD ["pnpm", "test:browser"]
