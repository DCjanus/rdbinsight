# 背景与思路

本次目标为在未引入外部排序的前提下，先支持将解析到的 Redis 记录输出为多个 Parquet 文件：每个 Redis 实例一个文件，全部置于同一目录层级内，作为后续“外部排序/多路归并”的地基能力。

核心点：
- 目录布局：`/<dir>/<cluster>/<batch_dir>/<instance>.parquet`。
- 批次目录命名采用“安全可读”的 UTC 格式，并在批次未完成前以 `tmp_` 前缀表示未完成状态。
- 实例文件采用临时后缀 `.parquet.tmp` 写入，完成后原子重命名为 `.parquet`。
- 文件名安全：实例标识中的冒号 `:` 在文件名中替换为 `-`，但记录字段 `instance` 保持原始值（例如 `127.0.0.1:6379`）。
- Parquet 字段类型与现有 ClickHouse 表语义对齐，利于后续查询与对账。
- 并发模型沿用“按实例并发处理”，每实例一个独立的 Parquet writer，互不竞争。

该能力完成后，将为后续“外部排序（external merge sort）/run 合并”的实现提供已分桶（按实例）且可恢复的落地产物。

## 范围界定（本阶段）
- 仅引入 Parquet 输出能力（per-instance 文件、临时文件与目录原子重命名、基本压缩配置）。
- 不实现：外部排序/多路归并、断点续写/覆盖策略、额外统计/benchmark、行组/页大小等高级可配项、额外完成标记文件。
- CLI 仅包含本阶段最小参数：`--dir` 与 `--compression`；其余参数为后续阶段可选项。

# 关键设计

## 目录与命名规则
- 根目录：通过 CLI 参数 `--dir` 指定。
- 路径结构：`/<dir>/<cluster>/<batch_dir>/<instance_sanitized>.parquet`
  - `<cluster>`：使用配置中的集群名，原样作为目录名（需保证只包含安全字符；若未来存在特殊字符，可追加轻量转义规则）。
  - `<batch_dir>`：UTC 安全可读格式，采用 `YYYY-MM-DD_HH-mm-ss.SSSZ`，例如 `2025-08-14_12-34-56.123Z`。
    - 进行中目录名：`tmp_YYYY-MM-DD_HH-mm-ss.SSSZ`
    - 全部实例完成后，将 `tmp_...` 原子重命名为最终目录名（去掉 `tmp_`）。
  - `<instance_sanitized>`：由实例标识派生的文件名，将 `:` 替换为 `-`；例如 `127.0.0.1:6379` → `127.0.0.1-6379.parquet`。
- 临时文件：写入时以 `.parquet.tmp` 后缀命名，writer 关闭后原子重命名为 `.parquet`。

## 原子性与可恢复性
- 批次级：运行开始即创建 `tmp_<batch>` 目录，所有实例文件都在该目录内生成；当全部实例成功完成后，统一 `rename(tmp_<batch> -> <batch>)`。
- 实例级：每个实例独立 writer 写到 `*.parquet.tmp`；当实例结束时 `close + rename(*.parquet.tmp -> *.parquet)`。
- 失败恢复：
  - 若进程异常退出，磁盘上会残留 `tmp_<batch>` 目录与若干 `*.parquet.tmp` 文件，可用于诊断。
  - 后续运行遇到同一 `tmp_<batch>`：默认失败并提示人工决策（清理或改批次）。

## 并发与资源管理
- 并发沿用现有“按实例并发”的模型，每个实例仅打开一个 writer，文件句柄总数受并发度控制。
- 写入缓冲：沿用现有批量缓冲策略，避免频繁 flush；行组/页大小等参数暂不开放配置。

## Parquet Schema（与 ClickHouse 语义对齐）
- 字段与类型：
  - `cluster`: Utf8
  - `batch`: Timestamp(Nanosecond, UTC) — 与 ClickHouse 的 `DateTime64(9, 'UTC')` 对齐
  - `instance`: Utf8 — 记录原始值（仍为 `ip:port`）
  - `db`: Int64（语义≥0）
  - `key`: Binary（保留二进制，避免 base64 膨胀）
  - `type`: Utf8
  - `member_count`: Int64（语义≥0）
  - `rdb_size`: Int64（语义≥0）
  - `encoding`: Utf8
  - `expire_at`: Timestamp(Millisecond, UTC) nullable — 与现有毫秒精度一致
  - `idle_seconds`: Int64 nullable
  - `freq`: Int32 nullable（源为 `u8`）
  - `codis_slot`: Int32 nullable（源为 `u16`）
  - `redis_slot`: Int32 nullable（源为 `u16`）

说明：Parquet/Arrow 常用有符号整型；保持有符号类型以兼容生态，但在业务层确保不出现负值。

## 压缩与编码
- 压缩算法：允许用户通过 CLI 指定 `--compression <zstd|snappy|none>`，默认 `zstd`。
- 其他编码（字典、页大小等）：不在本阶段范围。

## CLI 与配置表面（与 ClickHouse 输出并列）
- 新增输出子命令：如 `IntoParquet`。
- 本阶段参数：
  - `--dir <path>`：输出根目录（必选）
  - `--compression <zstd|snappy|none>`：压缩算法（可选，默认 `zstd`）

## 与现有流水线的衔接
- 保持与 `ClickHouseOutput` 相同的“写入接口形状”，便于最小侵入式接入 CLI 主流程（只在分支选择输出目标处切换）。
- 继续沿用“按实例并发”的 `process_records_*` 主循环，新增 Parquet 写入路径：
  - 每实例首次写入时创建 writer（指向 `*.parquet.tmp`）
  - record → Arrow 行的转换与批量追加
  - 实例结束时关闭并 `rename(.tmp -> .parquet)`
  - 全部实例完成后 `rename(tmp_<batch> -> <batch>)`

## 日志与观测性
- 日志遵循项目规范：`operation` 字段优先、上下文字段其后、描述字符串最后。例如：
  - `info!(operation = "parquet_writer_open", instance = %instance, file_path = %path, "Open parquet writer")`
- 监控计数点（基础）：
  - 每实例写入的记录数、完成耗时
  - writer 打开/关闭、重命名成功/失败
  - 批次目录创建/重命名

# 实施建议

高层步骤（不含业务代码）：
1. CLI 与配置：
   - 新增 Parquet 输出子命令（与 ClickHouse 并列），增加 `--dir`、`--compression` 参数，完成基本校验（路径存在/可写、压缩枚举合法）。
2. 路径与命名工具：
   - 提供批次目录名生成（UTC 格式 `YYYY-MM-DD_HH-mm-ss.SSSZ` + `tmp_` 前缀）、实例文件名转义（`:`→`-`）、最终/临时路径构造；确保逐层 `mkdir -p`。
3. Schema 与 Writer 管理：
   - 基于 Arrow 定义 Parquet schema 与字段映射；实现“每实例一个 writer”的生命周期管理（首次写→复用→关闭→重命名）。
4. 写入流程整合：
   - 复用现有“按实例并发 + 批量缓冲”的主循环，新增 Parquet 写入路径；确保错误传播与资源清理。
5. 批次收尾与原子重命名：
   - 全部实例成功后，关闭所有 writer 并重命名 `tmp_<batch>` → `<batch>`；失败时保留现场，清晰报错。
6. 基本验证：
   - 单实例与多实例完整性校验（记录数一致、字段映射正确）；
   - 压缩算法切换的可用性验证（zstd/snappy/none）。

# 建议标题

``` 
feat(output): add per-instance Parquet sink with atomic tmp dirs/files and configurable compression
```
