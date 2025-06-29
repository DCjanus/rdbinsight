# 开发计划

本计划旨在指导 AI 与人类协作完成开发任务。请遵循以下协作流程：

- **AI**：负责执行【实现步骤】，并在完成后勾选对应的复选框 (`- [x]`)。AI **绝不能**勾选【验证步骤】。
- **人类**：负责审核【实现步骤】的产出，并执行【验证步骤】以确保质量。

---

## 阶段一：基础设置与依赖项

本阶段旨在为 ClickHouse 输出功能建立基础，包括添加必要的依赖库、创建模块文件以及定义配置结构。

### 实现步骤

- [x] **依赖添加**: 在 `Cargo.toml` 文件的 `[dependencies]` 部分，添加 `clickhouse` (例如 `clickhouse-rs`) 和 `clap` (确保已启用 `derive` 和 `subcommand` 特性)。
- [x] **模块创建**:
  - 创建 `src/output/` 目录。
  - 创建 `src/output/mod.rs` 文件，并声明 `clickhouse` 模块。
  - 创建 `src/output/clickhouse.rs` 空文件。
- [x] **配置定义**:
  - 在 `src/config.rs` 中，定义一个新的 `ClickHouseConfig` 结构体，包含 `url`, `cluster`, `instance` 等字段。
  - 更新顶层的 `Config` 结构体，添加一个 `clickhouse: Option<ClickHouseConfig>` 字段。
  - 确保 `Config::from_file` 逻辑能够正确解析 TOML 文件中的 `[clickhouse]` 部分。

### 验证步骤

- [x] **编译检查**: 运行 `just test` 或 `cargo check`，确认项目可以成功编译，没有依赖或模块声明错误。
- [x] **配置解析**: 准备一个包含 `[clickhouse]` 配置的 `test.toml` 文件，编写一个单元测试，确认 `Config::from_file` 能成功解析并填充 `ClickHouseConfig` 结构体。

---

## 阶段二：重构命令行界面 (CLI)

本阶段将根据设计方案，将原有的命令行参数模式重构为更清晰的子命令模式 (`dump` 和 `misc`)。

### 实现步骤

- [x] **CLI 结构定义**:
  - 在 `src/bin/rdbinsight.rs` 中，修改 `Cli` 结构体，使用 `clap::{Parser, Subcommand}`。
  - 定义一个 `Command` 枚举作为子命令，包含 `Dump(DumpArgs)` 和 `Misc(MiscCommand)` 两个变体。
  - 定义 `DumpArgs` 结构体，包含一个 `config: PathBuf` 字段。
  - 定义 `MiscCommand` 结构体，它本身也使用 `Subcommand` 来包含 `PrintClickhouseSchema` 变体。
- [x] **子命令逻辑实现**:
  - 在 `main` 函数中，添加 `match` 语句来处理不同的子命令。
  - 实现 `misc print-clickhouse-schema` 的逻辑：当匹配到此命令时，程序打印出设计文档中定义的三条 SQL DDL 语句，然后退出。
  - 为 `dump` 命令设置一个临时的占位逻辑，例如打印 "Dump command executed with config: [path]"。

### 验证步骤

- [x] **帮助信息**: 运行 `target/debug/rdbinsight --help`，确认输出中包含 `dump` 和 `misc` 子命令。
- [x] **子命令帮助**: 运行 `target/debug/rdbinsight misc --help`，确认输出中包含 `print-clickhouse-schema` 子命令。
- [x] **Schema 打印**: 运行 `target/debug/rdbinsight misc print-clickhouse-schema`，确认终端输出了格式正确的三条 SQL 语句。
- [x] **Dump 命令测试**: 运行 `target/debug/rdbinsight dump --config /tmp/dummy.toml`，确认程序打印了预期的占位信息。

---

## 阶段三：实现 ClickHouse 输出模块

本阶段将实现与 ClickHouse 交互的核心逻辑，包括连接、表验证和数据写入方法。

### 实现步骤

- [x] **`ClickHouseOutput` 结构体**: 在 `src/output/clickhouse.rs` 中，定义 `ClickHouseOutput` 结构体，它将持有一个 `clickhouse::Pool`。
- [x] **构造函数与验证**:
  - 实现 `ClickHouseOutput::new(config: ClickHouseConfig)` 函数。
  - 在 `new` 函数中，建立到 ClickHouse 的连接池。
  - 连接成功后，立即查询 `system.tables` 或 `information_schema.tables`，验证 `redis_records_raw` 和 `import_batches_completed` 表是否存在。如果任一表不存在，应返回错误，提示用户执行 `print-clickhouse-schema`。
- [x] **记录写入方法**:
  - 实现 `async fn write(&self, records: &[Record], batch_info: &BatchInfo)` 方法。(`BatchInfo` 是一个包含 `cluster`, `batch`, `instance` 的辅助结构体)。
  - 该方法需要将 `&[Record]` 切片转换为 ClickHouse `Row` 的集合。重点处理 `Record` 字段到 ClickHouse 列的映射关系。
  - 使用 `client.insert("redis_records_raw")` 执行批量写入。
- [x] **批次提交方法**:
  - 实现 `async fn commit_batch(&self, batch_info: &BatchInfo)` 方法。
  - 该方法向 `import_batches_completed` 表插入一行记录，包含当前的 `cluster` 和 `batch`。

### 验证步骤

- [x] **单元测试**: 编写单元测试，验证从 `Record` 到 `clickhouse::Row` 的转换逻辑是否正确无误。
- [x] **集成测试 (需要 ClickHouse 环境)**:
  - 编写一个集成测试，它首先手动创建表，然后初始化 `ClickHouseOutput`，调用 `write` 写入一批模拟数据，最后查询数据库确认数据已正确写入 `redis_records_raw`。
  - 编写另一个测试，调用 `commit_batch` 并验证 `import_batches_completed` 表中出现了对应的记录。
  - 编写一个测试，尝试在缺少表的环境中初始化 `ClickHouseOutput`，并断言它返回了预期的错误。

---

## 阶段四：端到端集成与完成

本阶段将所有组件串联起来，在 `dump` 命令中实现完整的端到端数据处理流程。

### 实现步骤

- [x] **`dump` 逻辑实现**: 在 `src/bin/rdbinsight.rs` 中，充实 `dump` 命令的处理逻辑。
  - 生成唯一的 `batch` 时间戳。
  - 初始化 `Source`、`RecordStream` 和 `ClickHouseOutput`。
  - 创建主处理循环，从 `RecordStream` 中拉取 `Record`。
  - 将 `Record` 存入一个 `Vec<Record>` 缓冲区。
  - 当缓冲区大小达到 `1,000,000` 或流结束时，调用 `clickhouse_output.write(...)`。
- [x] **错误处理与重试**:
  - 使用 `backoff` 或类似的库，为 `clickhouse_output.write` 和 `clickhouse_output.commit_batch` 调用包裹上重试逻辑（线性退避，1分钟超时）。
- [x] **最终提交**:
  - 在主处理循环成功结束后（即所有记录都已写入），调用 `clickhouse_output.commit_batch()` 来"发布"这个数据快照。

### 验证步骤

- [x] **端到端测试**:
  - 使用一个真实的 RDB 文件作为输入。
  - 运行 `dump` 命令，并让其完整执行。
  - 结束后，通过 ClickHouse 客户端查询 `redis_records_view`，确认可以看到刚刚导入的数据。
  - 直接查询 `redis_records_raw` 和 `import_batches_completed` 表，验证 `batch` 和 `cluster` 等元数据均正确无误。
- [ ] **中断测试**:
  - 运行 `dump` 命令，并在其执行中途（例如，写入了部分数据后）手动中断进程 (Ctrl+C)。
  - 查询 `import_batches_completed` 表，确认**没有**为这个被中断的 `batch` 留下任何记录。
  - 查询 `redis_records_view`，确认**看不到**任何属于这个被中断批次的不完整数据。
