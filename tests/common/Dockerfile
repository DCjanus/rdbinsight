# syntax=docker/dockerfile:1

ARG REDIS_VERSION
FROM debian:trixie AS builder

ARG REDIS_VERSION
ARG DEBIAN_FRONTEND=noninteractive

RUN : "${REDIS_VERSION:?build arg REDIS_VERSION is required}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        axel \
        build-essential \
        ca-certificates \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/build

RUN set -eux; \
    axel -n 8 -o "redis-${REDIS_VERSION}.tar.gz" "https://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz"; \
    tar -xzf "redis-${REDIS_VERSION}.tar.gz"; \
    rm -f "redis-${REDIS_VERSION}.tar.gz"; \
    cd "redis-${REDIS_VERSION}"; \
    major="${REDIS_VERSION%%.*}"; \
    set -- -j"$(nproc)"; \
    if [ "${major}" -ge 3 ]; then \
        set -- "$@" MALLOC=jemalloc; \
    fi; \
    if [ "${major}" -ge 6 ]; then \
        set -- "$@" BUILD_TLS=yes; \
    fi; \
    make "$@"; \
    install_prefix="/opt/redis"; \
    if make install PREFIX="${install_prefix}"; then \
        :; \
    else \
        echo "make install not available for redis ${REDIS_VERSION}, copying binaries manually"; \
        install -d "${install_prefix}/bin"; \
        for bin in redis-server redis-cli redis-benchmark redis-check-aof redis-check-rdb redis-check-dump redis-sentinel redis-stat; do \
            for candidate in "${bin}" "src/${bin}"; do \
                if [ -f "${candidate}" ]; then \
                    install -m 0755 "${candidate}" "${install_prefix}/bin/${bin}"; \
                    break; \
                fi; \
            done; \
        done; \
        if [ -f "redis.conf" ]; then \
            install -d "${install_prefix}/etc"; \
            install -m 0644 "redis.conf" "${install_prefix}/etc/redis.conf"; \
        fi; \
    fi; \
    echo "${REDIS_VERSION}" > "${install_prefix}/VERSION"; \
    cd ..; \
    rm -rf "redis-${REDIS_VERSION}"; \
    find /opt/redis -name "*.a" -delete

FROM debian:trixie-slim AS runtime

ARG REDIS_VERSION

RUN : "${REDIS_VERSION:?build arg REDIS_VERSION is required}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/redis /opt/redis

ENV REDIS_VERSION="${REDIS_VERSION}" \
    PATH="/opt/redis/bin:${PATH}"

WORKDIR /data
VOLUME ["/data"]

EXPOSE 6379

ENTRYPOINT ["/opt/redis/bin/redis-server"]
CMD []

LABEL org.opencontainers.image.source="https://download.redis.io/releases/" \
      redis.version="${REDIS_VERSION}"