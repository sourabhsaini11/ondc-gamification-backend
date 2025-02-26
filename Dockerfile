# Stage 1: Build the application
FROM node:18-alpine AS builder

# Set the working directory
WORKDIR /app

# Install dependencies required for native modules
RUN apk add --no-cache python3 make g++

# Copy package.json and package-lock.json
COPY package.json package-lock.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application source code
COPY . .

# Build the TypeScript project
RUN npm run build

# Stage 2: Runtime
FROM node:18-alpine

# Set the working directory
WORKDIR /app

# Install dependencies required for Prisma (optional, if needed)
RUN apk add --no-cache openssl

# Copy the built application from the builder stage
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/prisma ./prisma
COPY --from=builder /app/.env ./.env

# Expose the application port
EXPOSE 8000

# Command to run the application
CMD ["node", "dist/index.js"]