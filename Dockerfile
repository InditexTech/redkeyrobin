# SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
#
# SPDX-License-Identifier: Apache-2.0

### Build stage

# Define the desired Golang version
ARG GOLANG_VERSION=1.24.6

# Use an official Golang image with a specific version based on Debian
FROM golang:${GOLANG_VERSION}-trixie AS builder

# Set environment variables
ENV GOPROXY=https://proxy.golang.org,direct
ENV CGO_ENABLED=0


## Build manager binary

# Create a working directory for the application
WORKDIR /app

# Copy Go module files and download dependencies first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code
COPY / .

# Build the Go application with version information
RUN go build -ldflags "-X main.version=2.0.0" -o robin ./cmd/


## Build the desired redis-client binary version

# Create a working directory for the redis client
WORKDIR /redis-client

# Install git to be able to clone the repository
RUN apt update && apt upgrade -y && apt install -y git

# Define the desired Redis client version
ARG REDIS_CLIENT_VERSION=8.2.3

# Clone the Redis repository, checkout the desired version and build redis-cli
RUN git clone https://github.com/redis/redis.git && \
    cd redis && \
    git checkout ${REDIS_CLIENT_VERSION} && \
    make redis-cli


### Final stage

# Use a smaller base image for the final stage
FROM debian:trixie-slim AS final

# Install some useful tools
RUN apt update && apt upgrade -y && apt install -y curl procps

# Create a non-root user
# RUN addgroup -S robin && adduser -S -u 10000 -G robin robin
RUN addgroup robin && adduser --uid 10000 --ingroup robin robin

# Copy the Go binary from the builder stage
COPY --from=builder /app/robin /robin

# Copy the redis-cli binary from the redis-client stage
COPY --from=builder /redis-client/redis/src/redis-cli /usr/local/bin/redis-cli

# Switch to the non-root user
USER robin

# Set the entry point
ENTRYPOINT ["/robin"]
