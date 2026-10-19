## 贡献指南（中文）
[English](CONTRIBUTING.md) | 中文

感谢你对 RDBInsight 的关注与贡献！在提交代码前，请阅读并遵循本指南，以保持项目质量与一致性。

### 快速开始

- 在仓库根目录安装并使用 `just`（已提供 `justfile`）。
- 在提交前，执行：

```bash
just before_commit
```

该命令会自动执行格式化、静态检查与测试，确保改动满足项目要求。

### 代码风格与质量

- 统一使用 `rustfmt`（nightly 通道）进行格式化。
- 保持依赖声明有序（`cargo sort`）。
- Clippy 警告视为错误（`-D warnings`），请在提交前清零。
- 避免不必要的复杂度，优先清晰、可读的实现；仅对复杂业务逻辑添加注释。
- 尽量避免过度嵌套的代码，这在 Rust 初学者身上比较常见。
- 在编写新集成测试前，请先查阅 `test/` 目录中的现有用例，并优先复用已有的测试基础设施。
- 每次修改文档时，确保 `CONTRIBUTING.md` 与 `CONTRIBUTING.zh_CN.md` 内容保持一致；同样需保持 `README.md` 与 `README.zh_CN.md` 的一致性。

### 提交信息规范（Conventional Commits）

- 提交信息需使用简洁英文，遵循 Conventional Commits 规范。
- 消息格式：`<type>(<scope>): <subject>`
  - `<type>`：`feat`、`fix`、`docs`、`style`、`refactor`、`perf`、`test`、`build`、`ci`、`chore`、`revert`
  - `<scope>`：可选，用于标明影响范围（如模块名）
  - `<subject>`：一句话祈使句，简洁明了，不以句号结尾
- 示例：

```text
feat(parser): support LFU stats extraction
fix(report): correct memory flame graph color legend
chore(ci): bump actions/checkout to v4
```

### 版本一致性检查

- 使用 `just verify_version` 校验当前 Git 标签与项目版本元数据是否一致。
- 未显式传入标签时，命令会自动读取当前 HEAD 的标签。
- 若 HEAD 没有标签，命令会报错并提示显式指定标签。

示例：

```bash
# 自动从 HEAD 读取标签
just verify_version

# 显式指定标签
just verify_version tag=v1.2.3
```

### GitHub Actions 依赖

- 使用 `gh extension install github/gh-actions-lock` 安装官方锁文件工具。
- 新增或修改 Action 引用后，在仓库根目录执行 `gh actions-lock --no-narrow`，同时审查 workflow 和锁文件的变化。不要手工编辑 `.github/workflows/actions.lock`。
- 保留 `@v1` 等兼容的大版本引用；工具链和安装器频道引用保留原有语义。生成器会将同仓库 Action 迁移为按运行提交解析的 `$/` 引用。
- 执行 `just verify_actions` 离线检查锁文件覆盖范围，或执行 `gh actions-lock --verify` 进行与 CI 相同的在线校验。
- 每周依赖维护时审查移动引用的上游变化，确认后通过 `gh actions-lock --relock --no-narrow` 刷新锁定。Dependabot 提议 Action 版本变更时会更新已有锁定，但不会首次接入新 workflow。

### 测试与覆盖率

- 单元测试：使用 `cargo nextest`（通过 `just test`）。
- 覆盖率：通过 `just coverage` 生成报告并在本地查看 `target/coverage/html/index.html`。
- 若增加新功能或修复缺陷，请同步新增或完善测试用例。

### 本地开发环境（可选）

- 使用 Docker 启动本地依赖：

```bash
just up_dev    # 启动
just down_dev  # 停止
```

- 演示流程：

```bash
just demo
```
