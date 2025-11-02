# Redis Test Images

English | [中文](README.zh_CN.md)

This directory ships a generic `Dockerfile` that builds a test image for any Redis release.

## Quick Start

```bash
docker build \
  -f tests/image/Dockerfile \
  -t rdbinsight/redis:6.2.11 \
  --build-arg REDIS_VERSION=6.2.11 \
  .
```

- `REDIS_VERSION` is required and controls which tarball is fetched from <https://download.redis.io/releases/>.
- The resulting image tag is up to you; matching the Redis version keeps Testcontainers configuration readable.

- When compiling Redis 1.x, `make install` might be unavailable. The Dockerfile automatically falls back to copying binaries into `/opt/redis/bin`.

## Integration Tests

After building, reference the tag directly in Testcontainers:

```rust
GenericImage::new("rdbinsight/redis", "2.4.18")
```

Make sure all required versions are built locally before running tests; otherwise the container startup will fail when the image pull falls back to a missing tag.

## Testcontainers Reference

Rust Testcontainers can build custom Dockerfiles like this one via `GenericBuildableImage` and `BuildImageOptions`. Refer to the official guide for usage examples and optional flags such as `skip_if_exists` or `no_cache`:

<https://rust.testcontainers.org/features/building_images/>
