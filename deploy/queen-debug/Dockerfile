# The debug MCP server. Build from the repository root, because it imports the
# console's rules from app/src:
#
#   docker build -f deploy/queen-debug/Dockerfile -t queen-debug .
#
# Dockerfile.dockerignore, next to this file, replaces the root .dockerignore
# for this build and lets in only what the server runs.

FROM node:24-alpine AS deps
WORKDIR /srv/deploy/queen-debug
COPY deploy/queen-debug/package.json deploy/queen-debug/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

FROM node:24-alpine
WORKDIR /srv
# At the root, so both this server and the console's rules resolve it.
COPY --from=deps /srv/deploy/queen-debug/node_modules ./node_modules
COPY app/src ./app/src
COPY deploy/queen-debug/package.json ./deploy/queen-debug/package.json
COPY deploy/queen-debug/bin ./deploy/queen-debug/bin
COPY deploy/queen-debug/src ./deploy/queen-debug/src

# Fails the build if a rule starts importing a file this image does not hold.
RUN node -e "import('/srv/deploy/queen-debug/src/server.mjs').then(() => console.log('rules load'))"

ENV NODE_ENV=production
USER node
ENTRYPOINT ["node", "/srv/deploy/queen-debug/bin/queen-debug.mjs"]
