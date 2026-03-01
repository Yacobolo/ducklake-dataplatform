# syntax=docker/dockerfile:1
# Multi-stage build for the duck-demo data platform server.

# === Build stage ===
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache gcc g++ musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the server binary with CGO enabled (required by go-sqlite3 and duckdb-go).
RUN CGO_ENABLED=1 go build -o /bin/server ./cmd/server

# === Runtime stage ===
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata libstdc++

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
