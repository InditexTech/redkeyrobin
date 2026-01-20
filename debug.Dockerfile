# SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
#
# SPDX-License-Identifier: Apache-2.0

### Build stage

# Define the desired Golang version
ARG GOLANG_VERSION=1.25.6

# Use an official Golang image with a specific version based on Debian
FROM golang:${GOLANG_VERSION}-trixie AS builder

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


## Final stage

# Define the desired Golang version
ARG GOLANG_VERSION

# Use an official Golang image with a specific version based on Debian
FROM golang:${GOLANG_VERSION}-trixie

# Define the desired Delve version
ARG DELVE_VERSION=1.25

# Install Delve debugger
RUN go install "github.com/go-delve/delve/cmd/dlv@v${DELVE_VERSION}"

# Install some useful tools
RUN apt update && apt upgrade -y && apt install -y curl procps

# Copy the redis-cli binary from the redis-client stage
COPY --from=builder /redis-client/redis/src/redis-cli /usr/local/bin/redis-cli

WORKDIR /
EXPOSE 40000
