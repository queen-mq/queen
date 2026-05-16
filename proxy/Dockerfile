FROM node:22-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install production dependencies
RUN npm ci --only=production

# Copy application code
COPY src ./src
COPY public ./public

# Expose proxy port
EXPOSE 3000

# Healthcheck — uses wget (already in node:alpine) instead of curl, which is
# not installed. Hits /health which is unauthenticated and never opens DB.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD wget --spider --quiet http://localhost:3000/health || exit 1

# Run the proxy server
CMD ["node", "src/server.js"]

