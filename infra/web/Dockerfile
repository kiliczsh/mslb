FROM node:24-bookworm-slim@sha256:01d566e8d94d8c619591d78652babfccd3c6992a3dffa9ed5ab39e11cf6c70e9 AS frontend
ENV NODE_OPTIONS=--max-old-space-size=1024 NPM_CONFIG_UPDATE_NOTIFIER=false
RUN npm install --global --allow-scripts=pnpm pnpm@12.16.1
WORKDIR /app
COPY apps/web/ /app/
RUN pnpm install --frozen-lockfile --store-dir /app/node_modules/.pnpm-store
RUN pnpm build
FROM nginx:1.30.6-alpine@sha256:e9b2d7624fd7957f4ed44ae0a272d93fb73aeb8512ba8916f8118ae6143fd115
COPY infra/web/nginx.conf /etc/nginx/nginx.conf
COPY --from=frontend /app/dist /usr/share/nginx/html
USER nginx
ENTRYPOINT ["nginx"]
CMD ["-g", "daemon off;"]
