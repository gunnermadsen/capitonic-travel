# syntax=docker/dockerfile:1
FROM node:22.21.1-bookworm-slim AS build
WORKDIR /workspace
COPY package.json package-lock.json tsconfig.base.json ./
COPY packages/contracts/package.json packages/contracts/package.json
COPY packages/ingester/package.json packages/ingester/package.json
RUN --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export NODE_EXTRA_CA_CERTS=/run/secrets/proxy_ca; fi; \
    npm ci --workspace=@airline/contracts --workspace=@airline/ingester --include-workspace-root
COPY packages/contracts packages/contracts
COPY packages/ingester packages/ingester
RUN npx --no-install tsc -b packages/ingester

FROM node:22.21.1-bookworm-slim AS runtime
WORKDIR /workspace
COPY --from=build --chown=node:node /workspace/package.json ./
COPY --from=build --chown=node:node /workspace/node_modules ./node_modules
COPY --from=build --chown=node:node /workspace/packages/contracts/package.json ./packages/contracts/package.json
COPY --from=build --chown=node:node /workspace/packages/contracts/dist ./packages/contracts/dist
COPY --from=build --chown=node:node /workspace/packages/ingester/package.json ./packages/ingester/package.json
COPY --from=build --chown=node:node /workspace/packages/ingester/dist ./packages/ingester/dist
RUN command -v flock && mkdir -p /var/lib/ingester && chown node:node /var/lib/ingester
USER node
CMD ["flock", "--nonblock", "--no-fork", "/var/lib/ingester/process.lock", "node", "packages/ingester/dist/main.js"]
