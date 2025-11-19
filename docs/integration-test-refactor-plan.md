# 集成测试内嵌化重构计划

## 背景
- 目前 `tests/` 目录中的集成测试以独立 crate 形式存在，无法访问 `rdbinsight` 内部模块，只能通过将实现标记为 `pub` 来暴露接口。
- 随着代码演进，大量仅服务测试的 `pub` API 缺乏编译器的未使用警告，增加了 API 面及维护成本。
- 目标是将集成测试迁移为 `rdbinsight` crate 内部的一个仅在测试场景编译的 `mod`，从而让测试能够直接访问 `pub(crate)` 级别的实现。
- 旧版测试依赖 Redis 容器写入本地 RDB 文件再读取，这在实现了通过 SYNC 协议直接获取 RDB 之后显得多余，新的测试模块应直接走网络流程以降低开销并更贴合实际逻辑。
- 过往用例拆分过细，跨版本覆盖薄弱；由于依赖官方镜像（最低 2.8），导致早期版本缺失。当前通过 `.github/workflows/publish-test-images.yml` 自建镜像，可覆盖精确到 patch 的版本。

## 重构目标
1. 在 `src/` 中引入 `#[cfg(test)] mod integration;`，作为新的测试承载模块，并提供共享测试设施（helpers、fixtures、mock drivers等）。
2. 将现有集成测试逐步从 `tests/` 目录迁移到该模块中，保持测试覆盖度与断言语义。
3. 重写/适配测试基础设施，使其能够在 crate 内部直接复用内部类型，而无需通过 `pub` API。
4. 在迁移完成后，系统性收紧对外可见性，确保公共接口最多为 `pub(crate)`，减少公共 API 面。

## 整体计划
1. 基础设施搭建
   - 创建新的 `integration` 模块，抽取通用测试工具、测试入口、便捷宏。
   - 初期基础设施仅面向 Redis 相关的 Parser 测试，暂不引入 ClickHouse 等额外容器依赖，保持启动路径简单。
   - 提供一组结构体封装，支持“一键”拉起 Redis 容器、自动写入数据并通过 SYNC 获取 RDB。
   - 配置 `cargo test` 运行路径，确保新旧测试可并存。
   - 关键输出：`src/integration/mod.rs`、通用 helper、示例测试。
2. 渐进迁移
   - 以功能域为单位迁移 `tests/` 中的文件，必要时重构内部 API 以便直接调用。
   - 迁移后记录差异与依赖，保证每次提交可回溯。
   - 关键输出：迁移后的测试文件、迁移记录、配套工具函数。
3. 验证与清理
   - 移除旧 `tests/` 目录、更新文档、确保 CI 全量通过。
   - 收紧可见性到 `pub(crate)`，并验证集成测试通过 Redis SYNC 获取 RDB、无需本地临时文件。
   - 关键输出：清理提交、README/USAGE 更新、可见性审计报告、SYNC 流程验证。

## 测试策略改进
- 采用“单一大用例 + 多 fixture”模式：每次运行选定一个 Redis 版本，批量写入覆盖各类型数据，再统一校验解析结果，降低维护开销并提升版本对比一致性。
- 初期仅聚焦 Redis Parser 测试，所有基础设施（容器启动、数据准备、SYNC 拉取）围绕 Redis 进行封装，后续如需扩展到 ClickHouse 等再评估。
- 定义 `TestFixture` trait，抽象“如何写入”“版本是否支持”“如何校验”：
  - `fn load(&self, conn: &mut RedisClient)`：向指定版本的 Redis 写入本 fixture 所需数据。
  - `fn supported(&self, version: &semver::Version) -> bool`：利用 semver 精确判断某类型在目标版本是否可用。
  - `fn assert(&self, parsed: &[crate::parser::Item]) -> crate::helper::AnyResult<()>`：复用 `src/helper.rs` 中的 `AnyResult`，对解析结果进行结构化校验并返回标准化错误。
- 根据 Redis 版本特性维护 payload 清单（示例：模块化 Redis 类型、特定编码 only-on-older-version），集中管理哪些 payload 需要在特定版本启用。
- 构建版本矩阵时，优先使用自建镜像（publish-test-images workflow）提供的版本标签，保证可控且具备 patch 级粒度。

## 近期成果
- `src/integration` 模块已创建，包含 fixtures/helpers/redis/smoke 子模块，可在 crate 内直接访问内部实现。
- `TestFixture` trait 已投入使用，当前提供 `RawStringRecordFixture`、`IntStringRecordFixture`、`SetRecordFixture`、`SmallZSetRecordFixture`、`SkipListZSetRecordFixture` 等，覆盖 string/set/zset 三类基础类型。
- `redis_smoke_suite` 使用 rstest 驱动 Redis 8.0.5/7.0.15/6.0.20/2.8.24 镜像，按 fixture 顺序写入 Redis、通过 SYNC 拉取 RDB 并断言解析结果，验证基础流程可运行。
- fixture 负载刻意包含长 key/member，确保命中 RAW / SkipList 等常见编码路径，并为扩展 list/hash/stream 等类型提供模板。

## 当前进度
- [x] 阶段 1：初始 `integration` 模块 —— 已完成模块骨架、Redis 容器 helper、基础 smoke suite 以及首批 fixture；仍需补充更丰富的共享 helper 与编码覆盖。
- [ ] 阶段 2：迁移现有集成测试 —— 尚未从 `tests/` 目录迁移具体用例，需要在现有基础上挑选代表场景并逐步搬迁。
- [ ] 阶段 3：收尾与可见性收紧 —— 待迁移完成后启动，包括移除旧目录与收紧 `pub` API。

## 下一步建议
1. 按类型扩展 fixture：补充 list/hash/stream/expiry 等，针对旧版本特有编码实现 `supported` 过滤，并沉淀常用断言 helper。
2. 基于新的 fixture 套件迁移 `tests/` 目录的代表性用例（优先 Parser 相关），验证断言语义与内部依赖是否在 crate 内可达。
3. 梳理当前暴露为 `pub` 但仅被旧测试使用的 API，记录在案，为阶段 3 的可见性收紧提供输入。
4. 在迁移趋于稳定后，调整 CI / just 任务，去掉对 `tests/` 目录的特殊处理，确保新旧路径切换顺滑。

## TODO
- [ ] 在新架构稳定后，移除 `justfile` 中 `test` 任务以及 `.github/workflows/ci.yml` 覆盖步骤里针对 `cargo nextest` 的 `-E 'not kind(test)'` 过滤器，恢复对 `tests/` 目录的全量执行。

## 附注
- 迁移过程中，可借助 `#[cfg(test)]` gate 将新旧测试并存，保证 CI 稳定。
- 若测试依赖耗时初始化（如数据库），考虑在 `integration` 模块内提供复用的全局设施，避免每个测试重复搭建。
