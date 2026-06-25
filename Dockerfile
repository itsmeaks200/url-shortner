# Stage 1 — install production dependencies only
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# Stage 2 — lean runtime image (~150 MB vs ~1 GB for node:20)
FROM node:20-alpine
WORKDIR /app

# Copy only production node_modules from the builder stage
COPY --from=builder /app/node_modules ./node_modules
# Copy application source (node_modules excluded via .dockerignore)
COPY . .

# Run as non-root for security
USER node

EXPOSE 3000
CMD ["node", "src/index.js"]
