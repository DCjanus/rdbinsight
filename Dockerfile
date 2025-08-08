# syntax=docker/dockerfile:1
FROM rustlang/rust:nightly-bookworm-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        binutils \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/app

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    cargo build --release --bin rdbinsight && \
    cp target/release/rdbinsight /tmp/rdbinsight

RUN strip /tmp/rdbinsight

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/rdbinsight /usr/local/bin/rdbinsight

WORKDIR /app

ENTRYPOINT ["rdbinsight"]
CMD ["--help"]