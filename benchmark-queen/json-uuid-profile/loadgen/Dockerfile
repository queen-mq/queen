# syntax=docker/dockerfile:1
FROM node:24-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY loadgen.js ./
ENTRYPOINT ["node", "loadgen.js"]
