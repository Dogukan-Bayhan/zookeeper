# syntax=docker/dockerfile:1

# ---- build: compile every command into static binaries ----
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# ---- runtime: small image with zkapp, zkserver and zkcli ----
FROM alpine:3.20
RUN adduser -D -u 10001 zk && mkdir -p /data && chown zk /data
COPY --from=build /out/ /usr/local/bin/
USER zk
WORKDIR /home/zk
# Colored logs in `docker compose logs`
ENV FORCE_COLOR=1
EXPOSE 8080 2181
# Default: the interactive web lab
CMD ["zkapp", "web", "-addr", "0.0.0.0:8080"]
