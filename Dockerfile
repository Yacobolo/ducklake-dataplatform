# syntax=docker/dockerfile:1
# Multi-stage build for the duck-demo data platform server.

# === Build stage ===
FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the server binary with CGO enabled (required by go-sqlite3 and duckdb-go).
RUN CGO_ENABLED=1 go build -o /bin/server ./cmd/server

# === Runtime stage ===
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata wget libstdc++6 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/server /usr/local/bin/server

# Default data directory for SQLite metastore
RUN mkdir -p /data
WORKDIR /data

EXPOSE 8080

# Default environment
ENV LISTEN_ADDR=:8080 \
    META_DB_PATH=/data/ducklake_meta.sqlite \
    LOG_LEVEL=info

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/healthz || exit 1

ENTRYPOINT ["server"]
