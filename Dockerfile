# syntax=docker/dockerfile:1

# meshcore-ninja-api: Go service that downloads network definitions, consumes
# CoreScope analyzer streams through Tangleveil, and exposes live metrics.

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=docker
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/meshcore-ninja-api .

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /app/state
WORKDIR /app
COPY --from=build /out/meshcore-ninja-api /usr/local/bin/meshcore-ninja-api
COPY config.docker.example.toml /app/config.toml
EXPOSE 8080
ENTRYPOINT ["meshcore-ninja-api"]
CMD ["--config", "/app/config.toml"]
