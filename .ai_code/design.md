## 背景与思路

目标：为 `rdbinsight dump` 命令增加一个可选参数以启用 Prometheus 指标导出。在启用时，启动一个仅在 `/metrics` 路径提供指标的 HTTP 端点，监听地址来自用户提供的 HTTP URL（仅支持 `http://`）。

约束与取舍：
- 仅接受 HTTP URL，不支持裸地址与其他 scheme。
- 作为 `dump` 的全局选项，对所有子命令生效。
- 指标内容暂时仅暴露当前工具版本号，后续再扩展。
- 需要提供环境变量支持以便无侵入配置。
- 服务生命周期与一次 `dump` 任务一致；未启用时行为保持不变。

核心思路：
- 在 CLI 层为 `dump` 增加全局 `--prometheus <HTTP_URL>` 可选参数（同时支持 `RDBINSIGHT_PROMETHEUS` 环境变量）。
- 启用时，解析并校验 URL（仅 `http`）；派生出监听的 `SocketAddr`；使用已有 `tokio` + `hyper` 启动轻量 HTTP 服务，仅处理 `GET /metrics`。
- `/metrics` 输出符合 Prometheus 文本格式的静态内容（仅包含版本信息），`Content-Type: text/plain; version=0.0.4; charset=utf-8`。
- 服务与主 `dump` 流程并发运行；`dump` 结束后触发优雅关停。

---

## 关键设计

1) CLI 结构调整（clap）
- 现状：`Command::Dump(DumpCommand)` 直接将 `DumpCommand` 作为子命令载体，无法为 `dump` 本身添加全局选项。
- 方案：引入新的 `DumpArgs`，包含：
  - 全局选项：`prometheus: Option<String>`（从环境变量 `RDBINSIGHT_PROMETHEUS` 读取）。
  - 子命令：`#[command(subcommand)] cmd: DumpCommand`。
- 解析策略：
  - 接受形如 `http://0.0.0.0:9901` 的 URL 字符串。
  - 校验 scheme 必须是 `http`；拒绝其他 scheme 或无效 URL。
  - 如需更严格校验，可使用 `url` crate 进行解析与提取 host/port。

2) 监听地址与解析
- 使用 `url` 解析：
  - 仅允许 `http`，提取 host 和 port（必需）；path/query/fragment 必须为空（或忽略但不用于绑定）。
  - host 支持 IPv4、IPv6、或主机名（可选：通过 `tokio::net::lookup_host` 解析主机名，取第一条解析结果；若仅需覆盖示例，可先支持 IP 字面量）。
- 生成 `SocketAddr` 用于 `hyper::Server::bind`。
- 绑定失败（端口占用/权限问题）时应返回用户可读错误并终止 `dump`。

3) HTTP 服务与路由
- 依赖与运行时：复用现有 `tokio` 与 `hyper`，无需引入 web 框架；新增 `url` 依赖用于安全解析。
- 路由：
  - 仅实现 `GET /metrics`；其它路径返回 `404 Not Found`。
  - 响应头：`Content-Type: text/plain; version=0.0.4; charset=utf-8`。
- 响应内容：仅包含版本号信息，例如：
  - `# HELP rdbinsight_build_info Build and version information` 
  - `# TYPE rdbinsight_build_info gauge`
  - `rdbinsight_build_info{version="<x.y.z>"} 1`
- 版本来源：`env!("CARGO_PKG_VERSION")` 编译期注入，或通过 `clap` 元信息获取，两者任选其一（推荐 `env!`）。

4) 并发与生命周期
- 在进入 `dump` 主流程前，若启用 `--prometheus`：
  - 启动 `hyper` 服务器为后台任务，使用 `tokio::task::spawn`。
  - 使用 `tokio::sync::oneshot` 提供关停信号；`dump` 主流程结束后发出关停信号，并等待服务器优雅退出（设定适度超时，例如 2-5s）。
- Ctrl-C/信号处理（若项目已有）：复用既有优雅关停逻辑，同步关闭 metrics 服务。

5) 错误处理与日志
- URL 无效/非 HTTP：在参数解析或早期校验阶段直接报错退出。
- 绑定失败：记录错误并退出；不影响未启用 `--prometheus` 的默认行为。
- 启动成功：记录监听地址与访问路径的 info 日志，便于排障。

6) 依赖与配置
- 复用：`tokio`、`hyper`（仓库已包含）。
- 新增：`url` crate（用于解析与校验）。
- 环境变量：`RDBINSIGHT_PROMETHEUS` 映射到 `--prometheus`，与现有 `clap(env)` 风格保持一致。

7) 兼容性与扩展性
- 未提供 `--prometheus` 时，行为完全不变，零额外开销。
- 后续可在不破坏兼容的前提下扩展更多指标（处理计数、字节数、错误数等），或切换到 `prometheus` crate 注册器（当前不引入以保持最小实现）。

---

## 实施建议

- 调整 CLI：
  1. 将 `Command::Dump(DumpCommand)` 重构为 `Command::Dump(DumpArgs)`，其中 `DumpArgs` 持有全局选项与子命令。
  2. 在 `DumpArgs` 中新增 `#[clap(long, env = "RDBINSIGHT_PROMETHEUS")] prometheus: Option<String>`。
  3. 在参数校验阶段解析并验证 HTTP URL，仅允许 `http`。

- 启动/关停服务：
  1. 在进入 `dump` 主流程前，根据 `prometheus` 的 URL 解析为 `SocketAddr`；若失败则报错退出。
  2. 启动 `hyper` 服务器（后台任务），实现 `GET /metrics` 返回静态文本指标。
  3. `dump` 结束后发送关停信号，等待服务器优雅退出（设置超时，上报日志）。

- 指标内容：
  1. 基于 `env!("CARGO_PKG_VERSION")` 构造 `rdbinsight_build_info{version="<x.y.z>"} 1`。
  2. 输出包含 `# HELP` 与 `# TYPE` 元注释；`Content-Type` 按 Prometheus 文本协议设置。

- 依赖与文档：
  1. 在 `Cargo.toml` 新增 `url`（直接 `cargo add url`）。
  2. 保持现有 README 不变（无需更新）。

---

## 建议标题

``` 
feat(cli): add optional Prometheus metrics endpoint for dump via --prometheus
```
