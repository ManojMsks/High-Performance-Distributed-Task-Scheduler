# -----------------------------------------------------------------------------
# Stage 1 — Builder
# Installs all deps (including dev), generates Prisma client, compiles TypeScript.
# -----------------------------------------------------------------------------
FROM node:22-alpine AS builder

WORKDIR /app

# Install deps first (layer-cached unless package*.json changes)
COPY package*.json ./
RUN npm ci --quiet

# Generate Prisma client before compiling (client is referenced by src/)
COPY prisma/ ./prisma/
RUN npx prisma generate

# Compile TypeScript ? dist/
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

# -----------------------------------------------------------------------------
# Stage 2 — Production
# Lean runtime image: only production deps + compiled output.
# -----------------------------------------------------------------------------
FROM node:22-alpine AS production

WORKDIR /app
ENV NODE_ENV=production
# Prisma uses this to find the database
ENV DATABASE_URL=""

# Install only runtime dependencies
COPY package*.json ./
RUN npm ci --omit=dev --quiet

# Copy pre-generated Prisma client (avoids needing prisma CLI at runtime)
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma

# Copy compiled application
COPY --from=builder /app/dist ./dist

# Copy schema for prisma migrate deploy (run by the migrate service)
COPY prisma/ ./prisma/

# Copy static dashboard assets
COPY public/ ./public/

# API server port
EXPOSE 3000

# Default command: API server.
# docker-compose overrides this per service (scheduler, worker, recovery).
CMD ["node", "dist/index.js"]
