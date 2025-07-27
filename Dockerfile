# syntax=docker/dockerfile:1
FROM rustlang/rust:nightly-alpine AS builder

RUN apk add --no-cache musl-dev
WORKDIR /usr/src/app

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    cargo build --release --bin rdbinsight && \
    cp target/release/rdbinsight /tmp/rdbinsight

RUN strip /tmp/rdbinsight

FROM alpine:3.20

RUN apk add --no-cache ca-certificates
RUN addgroup -g 1000 rdbinsight && \
    adduser -D -s /bin/sh -u 1000 -G rdbinsight rdbinsight

COPY --from=builder /tmp/rdbinsight /usr/local/bin/rdbinsight
RUN chmod +x /usr/local/bin/rdbinsight

USER rdbinsight
WORKDIR /app

ENTRYPOINT ["rdbinsight"]
CMD ["--help"] 