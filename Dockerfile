# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy dependency definitions
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o log-collector ./cmd/log-collector

# Runtime Stage
FROM alpine:3.19

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/log-collector .

# Create directory for logs if needed
RUN mkdir -p /var/log/collector

# Expose syslog ports
EXPOSE 5140/udp 5140/tcp

# Entrypoint
ENTRYPOINT ["./log-collector"]
