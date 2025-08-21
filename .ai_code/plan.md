# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

*   **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
*   **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：重构 dump CLI 以支持全局 --prometheus 选项

### 实现步骤
- [x] 在 `Cargo.toml` 添加依赖：执行 `cargo add url`（仅新增依赖，不改其他逻辑）。
- [x] 在 `src/bin/rdbinsight.rs` 中新增 `DumpArgs` 结构体：包含 `#[clap(long, env = "RDBINSIGHT_PROMETHEUS")] prometheus: Option<String>` 与 `#[command(subcommand)] cmd: DumpCommand`。
- [x] 将 `enum Command` 中的 `Dump(DumpCommand)` 替换为 `Dump(DumpArgs)`；调整 `main()` 中对 `Command::Dump(...)` 的匹配逻辑，保持原有子命令行为不变。
- [x] 保持 `dump_records(dump_cmd: DumpCommand)` 现有签名与逻辑不变，仅由新的入口在调用时传入 `dump_args.cmd`。
- [x] 更新 `dump_command_to_config(...)` 的调用处以适配 `DumpArgs` 包装。

### 验证步骤
- [x] 运行 `cargo build`，应成功编译。
- [x] 执行 `rdbinsight dump --help | cat`，应在 dump 级别看到 `--prometheus <HTTP_URL>` 选项且说明从 `RDBINSIGHT_PROMETHEUS` 读取。
- [x] 执行 `RDBINSIGHT_PROMETHEUS=http://0.0.0.0:9901 rdbinsight dump from-file --help | cat`，确保不会因新增选项影响子命令帮助展示。

---

## 阶段二：解析并校验 --prometheus HTTP URL（仅限 http）

### 实现步骤
- [x] 在 `src/bin/rdbinsight.rs` 中添加解析函数（或内联逻辑）：当 `prometheus` 为 `Some(url)` 时，使用 `url::Url::parse` 解析；仅接受 `http` scheme。
- [x] 从 URL 提取 host 与 port；若缺失或无效则返回用户可读错误。暂不支持从路径中读取任何信息（路径固定后续使用 `/metrics`）。
- [x] 将解析结果转换为 `SocketAddr`（如为主机名可使用 `tokio::net::lookup_host` 解析，取首个结果）；解析失败时提前报错退出。
- [x] 该阶段不启动 HTTP 服务，仅完成参数校验与解析，确保无效输入能尽早失败。

### 验证步骤
- [x] 执行 `rdbinsight dump --prometheus http://0.0.0.0:9901 from-file --path /dev/null`，应通过参数阶段（后续会因业务输入失败也可）。
- [x] 执行 `rdbinsight dump --prometheus 0.0.0.0:9901 from-file --path /dev/null`，应提示仅支持 HTTP URL。
- [x] 执行 `rdbinsight dump --prometheus https://0.0.0.0:9901 from-file --path /dev/null`，应提示不支持非 http scheme。
- [x] 运行 `cargo clippy -D warnings`，应无新增告警。

---

## 阶段三：实现 Prometheus /metrics 端点与后台服务

### 实现步骤
- [x] 在 `src/` 下新增 `metrics.rs` 模块（或在 `src/bin/rdbinsight.rs` 内实现一组函数），提供：
  - [x] 构建响应文本的函数：基于 `env!("CARGO_PKG_VERSION")` 生成以下文本：
    - `# HELP rdbinsight_build_info Build and version information`
    - `# TYPE rdbinsight_build_info gauge`
    - `rdbinsight_build_info{version="<x.y.z>"} 1`
  - [x] 使用 `hyper` 实现仅处理 `GET /metrics` 的服务，其它路径返回 `404`；设置 `Content-Type: text/plain; version=0.0.4; charset=utf-8`。
  - [x] 支持 `tokio::sync::oneshot` 的关停信号，优雅退出。
- [x] 在 `src/bin/rdbinsight.rs` 的 `Command::Dump(DumpArgs)` 分支中：
  - [x] 若已解析出监听地址，`tokio::spawn` 启动服务后台任务，并持有关停 sender。
  - [x] 调用原有 `dump_records(...)` 执行业务流程。
  - [x] 业务流程结束后，发送关停信号并等待后台任务优雅退出（设置合理超时）。
- [x] 为关键启动/错误路径添加日志（info/ warn），记录监听地址、绑定失败等。

### 验证步骤
- [x] 执行示例：`rdbinsight dump --prometheus http://0.0.0.0:9901 from-cluster --nodes 127.0.0.1:7000,127.0.0.1:7001 --cluster test`（或使用任一可运行的子命令与参数）。
- [x] 在另一个终端 `curl -v http://0.0.0.0:9901/metrics`，应返回 `200`，`Content-Type` 正确，且正文包含 `rdbinsight_build_info{version="..."} 1`。
- [x] 访问其他路径（如 `/healthz`）应返回 `404`。
- [x] 结束 `dump` 主流程后，确认后台服务退出（无悬挂任务/端口占用）。
- [x] 运行 `cargo test` 与 `cargo clippy -D warnings`，应通过。

---

## 阶段四：边界与回归验证

### 实现步骤
- [ ] 处理端口占用与权限错误路径：当绑定失败时输出清晰错误并终止执行；确保不启用 `--prometheus` 时行为零改动。
- [ ] 审查 `--help` 输出与 `RDBINSIGHT_PROMETHEUS` 环境变量交互，确保与 README 风格一致（无需更新 README）。
- [ ] 复查 `Dockerfile`（如有需要，通常无需改动）以确认运行时不受新增依赖影响。

### 验证步骤
- [ ] 在已占用端口上启动：确认进程快速失败并输出易懂错误。
- [ ] 使用环境变量方式：`RDBINSIGHT_PROMETHEUS=http://0.0.0.0:9901 rdbinsight dump from-file --path /dev/null`，确保端点正常开启。
- [ ] 未提供 `--prometheus`：运行常规 `dump` 流程，确认无任何回归行为或额外日志。
- [ ] 运行一次完整的 `cargo build && cargo test && cargo clippy -D warnings`，应全部通过。
