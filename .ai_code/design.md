## 背景与思路

当前输出侧缺少统一抽象：上层流程需要根据不同后端（ClickHouse/Parquet）编排不同逻辑；同时 ClickHouse 路径中“获取+解析 RDB 與 写入 ClickHouse”在同一协程内，导致解析 CPU 利用率受下游写入阻塞影响。为提升可扩展性与吞吐，我们计划：

- 提供统一的 Output 抽象，屏蔽具体后端差异，主流程只面向同一接口编排。
- 将“生产（获取+解析 RDB，多协程并发）”与“消费（写入输出端，单实例）”解耦，通过有界异步 Channel 连接，形成 MPSC（multi-producer single-consumer）流水线。
- 统一批次上下文为“cluster + batch_ts（UTC）”两字段，避免额外的强类型耦合，以便在不同后端与消息层直传直用。
- 保持批次一致性：所有实例成功后再进行最终提交/收尾（ClickHouse 的 commit，Parquet 的 finalize 目录 rename 等）。

该改动应在不改变 CLI 与配置体验的前提下，提升 CPU 利用率、降低新增后端的接入成本，并为全局进度统计提供统一位置。


## 关键设计

### 1) 统一类型与抽象
- 批次上下文字段（公共）
  - 使用 `String` 表示 cluster
  - 使用 `OffsetDateTime` 表示 batch_ts（UTC）

- Chunk（公共消息/批量单元）
  - `struct Chunk { cluster: String, batch_ts: OffsetDateTime, instance: String, records: Vec<Record> }`
  - 表达一次“同实例、同批次”的记录批；用于生产者-消费者通道传输和 Output 写入的最小单元。

- Output（统一输出抽象，使用枚举而非 Trait 对象）
  - 采用与 `ItemParser` 相同风格的枚举派发，避免 `Box<dyn Trait>` 带来的动态分发与堆分配开销：
    - `enum Output { ClickHouse(ClickHouseOutput), Parquet(ParquetOutput) }`
  - 在枚举上实现一致的方法集：
    - `async fn write_chunk(&mut self, chunk: Chunk) -> AnyResult<()>`
    - `async fn finalize_instance(&mut self, instance: &str) -> AnyResult<()>`（默认 no-op）
    - `async fn finalize_batch(self) -> AnyResult<()>`  // 消费 self，防止重复 finalize
  - 设计要点：
    - 写入与实例收尾基于 `&mut self`，允许内部维护缓冲与状态。
    - 批次收尾通过消费 `self` 来一次性完成提交/rename，编译期约束防止二次调用；各变体内部执行资源释放（如 take/close 操作）。
    - 未来新增后端时向 `Output` 增加一个变体，并在匹配分支中实现对应逻辑。

- Output 工厂（配置到实现的创建）
  - 为 `OutputConfig` 增加工厂函数：`fn create_output(&self, cluster: &str, batch_ts: OffsetDateTime) -> AnyResult<Output>`。
  - ClickHouse：构造底层客户端、校验/创建表、保存 `cluster + batch_ts`（用于最终 commit）。
  - Parquet：创建批次临时目录，记录 `cluster + batch_ts` 与目标路径。


### 2) 生产者-消费者流水线（解耦解析与写入）
- 输出消息模型（Channel 传输单元）
  - `OutputMsg::Chunk(Chunk)`
  - `OutputMsg::InstanceDone { instance: String }`
  - 说明：
    - 解析协程按批量将记录聚合为 `Chunk` 发送，降低发送与锁争用开销；`Chunk` 携带 `cluster + batch_ts + instance + Vec<Record>`，便于输出端直接落地。

- 通道与回压
  - 使用 Tokio 有界 `mpsc::channel<OutputMsg>(capacity)`。
  - 容量建议：与并发度、批量大小相关（例如 `capacity = concurrency * 4`），保证适度缓冲又能提供回压，避免内存膨胀。

- 生产者（多协程并发）
  - 对每个 `RDBStream` 启动解析任务：
    - `prepare()` → 通过 `RecordStream` 解析 → 累积 `Vec<Record>` 达到批量或流结束 → 封装为 `Chunk` 发送；结束后发送 `InstanceDone`。
    - 解析错误：记录错误与上下文，终止该实例任务，并通过等待/汇总机制向上返回错误。
  - 并发调度：沿用 `try_for_each_concurrent(concurrency, ...)` 或 `JoinSet + Semaphore` 控制并发数量。

- 消费者（单协程，单输出实例）
  - 持有一个 `Output`（枚举实例）；循环接收 `OutputMsg`：
    - `Chunk(c)` → 调用 `output.write_chunk(c)`；失败使用 `backoff` 重试（延续现有策略）。
    - `InstanceDone { instance }` → 调用 `output.finalize_instance(&instance)`；失败使用 `backoff` 重试，若最终失败则终止整体批次。
  - 结束条件：
    - 所有生产者完成并关闭发送端后，消费者读尽 channel；随后调用 `output.finalize_batch()`（该操作会消费 `output`；ClickHouse = 执行批次 commit；Parquet = 目录 rename）。
  - 进度统计：
    - 消费者端集中统计累计记录数（基于 `Chunk.records.len()`）、各实例完成数/总数，周期性输出日志；便于统一进度汇报与 ETA 估计。

- 失败与一致性策略
  - 任一生产者失败：立即传播错误，停止接受新消息（关闭发送端），消费者退出并跳过 `finalize_batch()`，从而避免错误批次被提交。
  - 写入失败：消费者侧以 `backoff` 重试，超过重试上限则失败退出，仍跳过 `finalize_batch()`。
  - Parquet 在失败场景保留临时目录（`tmp_...`），便于排查；成功时才 rename 为最终目录。


### 3) 后端对齐（ClickHouse / Parquet）
- ClickHouseOutput → 作为 `Output::ClickHouse` 变体
  - 初始化：基于 `ClickHouseConfig + cluster + batch_ts`；内部持久化这两个字段用于最终 commit。
  - `write_chunk(chunk)`：沿用现有批量 insert 逻辑（从 `chunk.records` 构造行，使用 `chunk.instance`、`chunk.cluster`、`chunk.batch_ts`）。
  - `finalize_instance(instance)`：no-op。
  - `finalize_batch(self)`：使用初始化时保存的 `cluster + batch_ts` 执行 commit；方法消费 `self`，内部完成资源释放和提交。

- ParquetOutput → 作为 `Output::Parquet` 变体
  - 初始化：`new(dir, compression, cluster, batch_ts)`，用以构造批次临时目录与最终目录。
  - `write_chunk(chunk)`：保留“每实例独立 WriterHandle，按批写入”的策略。
  - `finalize_instance(instance)`：关闭对应 writer（等价于现有 `finalize_instance`）。
  - `finalize_batch(self)`：在关闭所有 writer 后，rename 批次临时目录为最终目录；方法消费 `self`，通过所有权语义防止重复 finalize。


### 4) 与现有代码的集成
- `OutputConfig` 保持不变，新增 `create_output()` 工厂方法；`rdbinsight` 主逻辑替换为：“创建 output（枚举）→ 启动生产者-消费者流水线（统一函数）”。
- 不再使用 `BatchInfo` 类型；统一以 `cluster + batch_ts` 作为上下文，在 Output 初始化时注入；`Chunk` 携带这两个字段用于写入。
- Backoff 策略统一封装在消费者循环中，以便所有后端一致复用。
- 保持 CLI、配置验证、日志格式、测试语义不变；对 Parquet/ClickHouse 的单元测试仅需微调 import 与初始化方式。


### （补充）Chunk 抽象与批次上下文

- 不再定义单独的 `BatchInfo` 类型以降低模块耦合与概念数量；批次上下文由 `cluster + batch_ts` 两字段表示，并贯穿消息（`Chunk`）与输出端初始化。
- `instance` 仅随 `Chunk` 传递，不进入批次级语义；批次统一收尾只依赖 `cluster + batch_ts`。
- 迁移策略（两阶段）：
  - 阶段一：保留 `RDBStream::instance()/source_type()`，生产者读取并填充到 `Chunk` 中，`Output` 端不再直接依赖 `RDBStream`。
  - 阶段二：将 `get_rdb_streams()` 的返回调整为包含上下文的条目（如 `StreamItem { reader, instance, source_type }`），解析使用 `StreamItem`。`RDBStream` 可简化为纯 `AsyncRead` 与可选 `prepare()` 钩子，或改由 `StreamItem` 提供。


## 实施建议

- 新增/重构
  - 新建 `output::types`（或在 `output::mod` 顶层）定义 `Chunk`。
  - 新建 `output::sink`（或在 `output::mod` 顶层）定义 `enum Output { ClickHouse(...), Parquet(...) }` 及其方法集。
  - 在 `config::OutputConfig` 上实现 `create_output(&self, cluster: &str, batch_ts: OffsetDateTime) -> AnyResult<Output>` 工厂。
  - 为 `ClickHouseOutput` 与 `ParquetOutput` 实现相应的枚举方法分支（迁移必要的状态与初始化逻辑）。

- 流水线改造
  - 在主二进制入口新增统一函数：`process_streams_with_output(streams, mut output, cluster, batch_ts, concurrency, channel_capacity, batch_size)`：
    - 启动单消费者任务，持有 `Output`（枚举）实例，循环接收消息并写入/收尾/提交；
    - 使用 `try_for_each_concurrent` 启动生产者，解析后按批封装为 `Chunk` 并发送 `OutputMsg::Chunk`，结束发送 `InstanceDone`；
    - 任一生产者失败时，关闭通道并传播错误，消费者收到关闭后退出，不调用 `finalize_batch()`；
    - 成功路径：生产者全部完成、通道关闭 → 消费者 drain 完 → `output.finalize_batch()`（消费 `output`）。
  - 统一 backoff：
    - `write_chunk()`、`finalize_instance()`、`finalize_batch()` 包裹重试；保留现有参数（初始间隔、最大间隔、最长总时长）。

- 进度与日志
  - 消费者侧集中统计：累计记录数（基于 `Chunk.records.len()`）、实例完成数/总数、吞吐（records/s），定期 `info!(operation = "progress_update", ...)` 输出统一格式日志。
  - 沿用现有日志规范（`operation` 字段优先，最后是描述字符串），字段按现实现有偏好输出（例如实例用 IP:Port）。

- 兼容性与测试
  - CLI、配置结构不变；
  - 移除对 `BatchInfo` 的引用；
  - 增加一个集成测试用例：并发多实例 → 生产者-消费者通路（基于 `Chunk`）→ 校验成功提交/收尾与统计。


## 建议标题

```text
feat(output): introduce unified Output trait and single-sink producer-consumer pipeline
```
