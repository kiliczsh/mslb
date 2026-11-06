FROM mcr.microsoft.com/playwright:v1.65.0-noble@sha256:6e3fb709361521eb27161606e6da6d2674e5e89fa16516cb37d6fdd91372273d
ENV NODE_OPTIONS=--max-old-space-size=512 NPM_CONFIG_UPDATE_NOTIFIER=false
RUN npm install --global --allow-scripts=pnpm pnpm@12.16.1
WORKDIR /app
COPY apps/web/ /app/
RUN pnpm install --frozen-lockfile --store-dir /app/node_modules/.pnpm-store
USER pwuser
CMD ["pnpm", "test:browser"]
