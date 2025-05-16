# SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
#
# SPDX-License-Identifier: Apache-2.0

# Use an official Golang image with a specific version
FROM golang:1.24.3 AS builder

# Set environment variables
ENV GOPROXY=https://proxy.golang.org,direct
ENV CGO_ENABLED=0

# Create a working directory for the application
WORKDIR /app

RUN ls
RUN apt-get update && apt-get install -y ca-certificates openssl
ARG cert_location=/usr/local/share/ca-certificates

# Get certificate from "github.com"
RUN openssl s_client -showcerts -connect github.com:443 </dev/null 2>/dev/null|openssl x509 -outform PEM > ${cert_location}/github.crt
# Get certificate from "proxy.golang.org"
RUN openssl s_client -showcerts -connect proxy.golang.org:443 </dev/null 2>/dev/null|openssl x509 -outform PEM >  ${cert_location}/proxy.golang.crt
# Update certificates
RUN update-ca-certificates

# Copy Go module files and download dependencies first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code
COPY / .

# Build the Go application with version information
RUN go build -ldflags "-X main.version=2.0.0" -o robin ./cmd/

# Use a smaller base image for the final stage
FROM debian:bookworm-slim AS final

# Install redis-cli by adding the redis package
RUN apt update -y && apt install -y redis-tools curl procps

# Create a non-root user
# RUN addgroup -S robin && adduser -S -u 10000 -G robin robin
RUN addgroup robin && adduser --uid 10000 --ingroup robin robin

# Copy API rest definition
COPY --chown=robin:robin api /opt/conf/api

# Copy the Go binary from the builder stage
COPY --from=builder /app/robin /robin

# Switch to the non-root user
USER robin

# Set the entry point
ENTRYPOINT ["/robin"]
