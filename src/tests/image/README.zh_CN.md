# Redis 测试镜像

[English](README.md) | 中文

这个目录提供一个通用的 `Dockerfile`，可以为任意 Redis 版本构建测试镜像。

## 快速开始

```bash
docker build \
  -f src/tests/image/Dockerfile \
  -t localhost/rdbinsight/redis:6.2.11 \
  --build-arg REDIS_VERSION=6.2.11 \
  .
```

- `REDIS_VERSION` 为必填构建参数，用于从 <https://download.redis.io/releases/> 下载对应版本的源码压缩包。
- 建议保留 `localhost/` 前缀，这样本地构建的镜像就不会与 Docker Hub 上的标签冲突。
- 镜像标签可自行指定；建议与 Redis 版本号保持一致，方便在 Testcontainers 中引用。

- 构建 1.x 版本时，若 `make install` 不可用，Dockerfile 会自动回退到手动复制二进制到 `/opt/redis/bin`。

## 集成测试对接

构建完成后，可以在 Testcontainers 中直接使用对应标签：

```rust
GenericImage::new("localhost/rdbinsight/redis", "2.4.18")
```

请确保在运行测试前，本地已构建好所需版本的镜像，否则启动容器时会因为找不到镜像而失败。

## Testcontainers 参考

Rust 版 Testcontainers 可通过 `GenericBuildableImage` 与 `BuildImageOptions` 动态构建本目录的 Dockerfile；详细示例与可选参数（如 `skip_if_exists`、`no_cache`）可参考官方文档：

<https://rust.testcontainers.org/features/building_images/>
