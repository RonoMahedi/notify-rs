# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends cmake zlib1g-dev && rm -rf /var/lib/apt/lists/*
COPY . .
# Cache mounts keep the crate registry and compiled deps between builds (seconds, not ~10 min).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin api --bin worker --bin loadgen --bin campaign && \
    mkdir -p /out && cp target/release/api target/release/worker target/release/loadgen target/release/campaign /out/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
USER nobody
