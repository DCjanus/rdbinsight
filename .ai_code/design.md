### 背景与思路

- 现状：工具已支持将 RDB 解析结果输出到 ClickHouse 与 Parquet。报告生成功能目前仅基于 ClickHouse 查询（`report::querier::ClickHouseQuerier`），HTML 模板在 `templates/report.html`，数据结构为 `report::querier::ReportData`。
- 目标：在不破坏现有模板与数据结构的前提下，让 Parquet 也能生成同样口径的报告。同时对“报告生成过程”做最小标准化：为每种 output 提供一个“单一方法、产出完整报告数据结构体”的能力，避免拆分多个细粒度查询接口。
- 思路：
  - 定义极简的统一接口（如 `ReportDataProvider` trait，仅一个 `generate_report_data` 方法），返回统一的 `ReportData`。ClickHouse 与 Parquet 各自实现内部细节。
  - Parquet 报告实现采取“离线文件扫描 + 多路归并”的单遍策略：通过全局 Iterator 合并各实例文件，在一次扫描中完成聚合、Top100、大 Key、槽倾斜与动态前缀发现。动态前缀发现维护“活跃前缀列表”，依据预先获取的全局阈值（1% 总体积，向上取 1）进行过滤；扫描同时累加 `total_size` 由元数据直接获得。

### 关键设计

- 统一接口（单方法、单结构体）
  - 统一数据模型：将 `ReportData` 及其子结构（`DbAggregate/TypeAggregate/InstanceAggregate/TopKeyRecord/PrefixAggregate/ClusterIssues/BigKey`）上移至公共位置（例如 `report::model`），供两种实现共享，保持 HTML 模板不变。
  - 接口定义：`trait ReportDataProvider { async fn generate_report_data(&self) -> AnyResult<ReportData>; }`（仅此一个方法）。
  - ClickHouse 实现：用现有 `ClickHouseQuerier::generate_report_data` 作为实现；`ReportGenerator` 改为接收任意 `ReportDataProvider`，模板替换逻辑复用。
  - Parquet 实现：新增 `ParquetReportProvider`，参数包含 `base_dir`、`cluster`、`batch`（可选）。

- Parquet 批次定位与文件组织
  - 约定目录：`<base_dir>/cluster=<cluster>/batch=<slug>/*.parquet`。与现有写入路径保持一致（`path.rs`）。
  - 批次选择：
    - 指定 `--batch` 时：接受 RFC3339 字符串；解析并转为 slug 以匹配目录（或直接要求传入 slug）。
    - 未指定：在 `cluster=<cluster>` 下按 slug 的时间含义选择“最新”目录。主方案：按 slug 字典序排序取末尾；兜底：比较目录 mtime。

- 写入端元数据增强（特异化，Versioned Metadata）
  - 设计目的：将常用聚合与 Top 信息预先存入 Parquet 文件的 key-value metadata，显著降低报告生成时的扫描与解码量。
  - 版本与兼容：
    - 写入键：`rdbinsight.meta.version = "1"`（字符串形式），遇到非 "1" 版本或缺失时，报告生成直接报错并退出，提示用户使用由本工具生成的 Parquet（后续如支持降级/回退再扩展）。
    - 元数据主体键：`rdbinsight.meta.summary.b64_msgpack`（MessagePack 二进制经 Base64 编码后的 UTF-8 字符串）。
  - 元数据结构（每个实例文件内，按 MessagePack 编码）：
    - `cluster`: String（冗余存储）
    - `batch_unix_nanos`: i64（UTC，Unix 纪元纳秒，用于批次标识与一致性校验）
    - `instance`: String（冗余存储，便于交叉校验）
    - `total_key_count`: u64
    - `total_size_bytes`: u64（`SUM(rdb_size)`）
    - `per_db`: Array<{ `db`: i64, `key_count`: u64, `total_size`: u64 }>
    - `per_type`: Array<{ `type`: String, `key_count`: u64, `total_size`: u64 }>
    - `top_keys_full`: Array<Record>（最多 100 条，结构与 `crate::record::Record` 一致，经 MessagePack 无损编码）
    - `dbs`: Array<i64>（该实例文件包含的 db 去重集合）
    - `slots`（可选，减少槽倾斜扫描开销）：
      - `codis_slots`: Array<u16>（该实例文件涉及到的 codis 槽去重集合）
      - `redis_slots`: Array<u16>（该实例文件涉及到的 redis 槽去重集合）
  - 写入位置与时机：在最终合并写入（`merge`）过程中边读边累计上述统计，`writer.close()` 前通过 Parquet Writer 的 key-value metadata 接口写入（主体使用 MessagePack 编码后 Base64）。

  - Page Index 与统计（强制要求）：
    - 写入：启用 Page/Column/Offset Index 的写入，确保 `db` 列在 row-group 与 page 级别都包含 min/max 统计；其他关键列可按需开启统计。
    - 读取：报告生成阶段严格校验这些索引与统计存在；若缺失或不完整，则报错退出（当前版本不做降级回退）。
    - 作用：支撑读取端的行组剪枝（Row Group Pruning），在打开 reader 前排除不含目标 `db` 的 row groups，减少 IO。

- 列投影与按需扫描（Projection）
  - 原则：尽可能依赖元数据；主扫描仅服务于“动态前缀发现”，其它统计尽量由元数据合成。
  - 主扫描最小列（仅用于动态前缀发现）：`{ key, rdb_size }`。通过对 Reader 应用 `db == <value>` 的行过滤获得“单一 db 的按 `key` 有序子流”，因此无需读取 `db` 列。
  - 槽倾斜：若元数据含 `slots`，则无需扫描 `codis_slot/redis_slot` 列；否则退回读取 `{ codis_slot, redis_slot }`（当前版本默认依赖元数据，故不读取）。
  - 实例聚合：不读取 `instance` 列，直接从元数据读取实例名称并求和。
  - 读取方式：使用 Parquet Arrow 读取器的列投影与行过滤（Row Filter）能力进行物理列裁剪与按 db 过滤；每个文件基于其元数据中的 `dbs` 集合生成多个带固定过滤条件的 reader。
  - 行组剪枝（Row Group Pruning）：
    - 在打开 reader 之前，使用 `ParquetMetaDataReader` 读取文件元数据，依次获取每个 row group 的 `db` 列 chunk metadata 与 statistics（min/max），据此推断该行组可能包含的 db 值。
    - 为每个目标 db 生成候选 row groups 列表；构建 `ArrowReaderBuilder` 时调用 `with_row_groups([...])` 仅载入候选行组，再叠加 `db == <value>` 行过滤，最大化减少无关 IO。
    - 参考链接：`ParquetMetaDataReader`（[docs.rs 链接](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html)）；`ArrowReaderBuilder::with_row_groups`（`https://docs.rs/parquet/latest/parquet/arrow/arrow_reader/struct.ArrowReaderBuilder.html#method.with_row_groups`）。

- Parquet 扫描与多路归并（全局 `(key)` 有序流，重复 key 分组处理）
  - 写入阶段保持既有排序 `(cluster,batch,instance,db,key)` 与 `sorting_columns` 不变；允许单个 row group 内包含多个 `db` 的数据。读取阶段不再依赖 per-db 段索引。基于文件元数据中的 `dbs` 集合，为每个文件按每个 `db` 创建一个应用了 `db == <value>` 行过滤的 reader，从而得到“固定 db 的按 `key` 递增子序列”。
  - 打开所有实例文件的多个 Arrow/Parquet 读取器：每个 reader 绑定一个 `db` 过滤条件；以 RecordBatch 流读取并维护各 reader 的当前指针；Reader 端启用“主扫描投影” `{ key, rdb_size }`。
  - 行组选择与跳过：在为每个 `db` 构建 reader 时，使用基于统计推断的候选 row groups，通过 `with_row_groups([...])` 跳过与该 db 无关的行组，减少数据页解压与解码。
  - 使用小根堆进行 K 路归并：堆元素为 `(key_bytes, stream_id, rdb_size)`，其中 `stream_id` 唯一标识“某文件的某个 db 子流”（可由 `stream_id` 映射回 `db`，用于 LCP 重置判断）。比较仅按 `key_bytes`。每次 `pop` 得到当前全局最小 `key_min` 后，继续“抽干”堆顶所有 `key == key_min` 的元素，形成“重复 key 分组”。对每个被抽出的子流，从对应 reader 续取下一条并压回堆。
  - 顺序处理：对该分组聚合 `sum_rdb_size` 与 `dup_count`，然后以“单个 key 分组”的视角推进 LCP 前缀算法：
    - 与上一分组的 `prev_key` 计算 LCP，按固定阈值规则封闭与扩展活跃前缀链；
    - 将本分组的 `sum_rdb_size` 累加到活跃链，`dup_count` 累加到各节点的 `key_count`；
    - 处理完成后将 `prev_key = key_min`，进入下一轮。

  附注：关于 tie-breaker
  - tie-breaker（如 `(reader_id, local_seq)`）常用于保证全序稳定性，但本场景只需确保“相同 key 的记录被连续处理”，故采用“重复 key 分组”策略即可，无需额外 tie-breaker。`reader_idx` 仅用于从对应 reader 拉取下一条记录。

- 统计项合成与流式处理
  - 基于元数据的直接合成（无需扫描）：
    - `db_aggregates`：聚合各实例 `per_db` 逐项求和。
    - `type_aggregates`：聚合各实例 `per_type` 逐项求和，末尾按 `total_size` 降序排序。
    - `instance_aggregates`：直接由各实例 `{ total_key_count, total_size_bytes }` 求和，实例名取自元数据。
    - `top_keys`：将各实例 `top_keys_full` 并集后用容量 100 的小根堆（按 `rdb_size`）取全局 Top100；由于条目是完整 `Record`，无需补全扫描。
    - `total_size` 与 `total_key_count`：跨文件直接求和，用于阈值与校验。
    - `codis_slot_skew` / `redis_cluster_slot_skew`：若元数据包含 `slots`，即可直接判定“某槽是否出现在两个及以上实例集合”。
  - 需要主扫描的部分（单遍）：
    - 动态前缀发现：
      - 预先阈值：由元数据求得全局 `total_size` 后计算 `threshold = max(1, total_size/100)`。
      - 活跃前缀列表：在全局 `(key)` 有序的“重复 key 分组”流上，按单遍算法维护当前 key 的前缀链；当某前缀被“封闭”时，若其累计体积 `>= threshold` 则收集到结果列表；扫描结束时对仍活跃的前缀执行同样判断；最终按前缀字典序排序输出。

- 动态前缀发现（LCP，单遍 + 固定阈值过滤）
  - 阈值规则：使用 `threshold = max(1, total_size/100)`，`total_size` 由元数据汇总获得。
  - 全局 Iterator：复用 K 路归并得到全局 `(db,key)` 有序记录流。
  - 活跃前缀列表：
    - 对每条记录的 `key`（字节序列），构造从长度 1 到 `len(key)` 的前缀链；当 `db` 变化时（由 `stream_id` 对应的 db 标识判断，无需读取 `db` 列），视为与上一条的 LCP 长度为 0。
    - 计算与上一条 `key` 的 LCP 长度 `l`：
      - 若当前活跃链长度 > `l`，则从链尾开始将多余前缀逐个“封闭”；对每个被封闭的前缀，若累计体积 `>= threshold`，则加入结果集合。
      - 若当前活跃链长度 < `l`，则表示需要按差额扩展活跃链（逐级创建新前缀节点，初始累计为 0）。
    - 对当前记录，将其 `rdb_size`（与 `key_count += 1`）累加到活跃链的所有节点（或仅累加到最深节点，并在封闭时向上聚合，择一实现以平衡复杂度）。
  - 扫描结束：将活跃链中剩余的前缀同样“封闭”，并按阈值过滤后加入结果；最终按前缀字典序排序输出。

- 一致性与边界处理
  - 与 ClickHouse 输出一致：
    - 时间序列化使用 RFC3339（UTC）。
    - Null/Option 字段保持一致的空值语义。
    - 排序规则：`type_aggregates`、`instance_aggregates` 按 `total_size` 降序；`top_prefixes` 按前缀字典序；Top 100 按大小降序。
  - 性能：
    - 借助元数据避免对非必要列与非必要文件范围的扫描；先用元数据统计进行行组剪枝，仅选取候选 row groups；主扫描仅 `{ key, rdb_size }`，通过 Row Filter 按 db 过滤，无需读取 `db` 列；动态前缀使用固定阈值过滤无需维护额外堆结构。

- 版本管理与错误处理
  - 读取报告时严格校验 `rdbinsight.meta.version == "1"` 且存在 `rdbinsight.meta.summary.b64_msgpack`；不满足则报错并退出（当前版本不做降级回退）。
  - 同时强校验 Parquet 文件包含 Page/Column/Offset Index 及 `db` 列的 row-group/page 级 min/max 统计；缺失则报错退出。
  - 如后续扩展元数据结构，版本递增；读取端按版本做严格匹配。
  - 未来可能的降级路径（暂不实现）：在缺失元数据或版本不匹配时，使用通用 OLAP 引擎（如 DuckDB）执行等价扫描逻辑作为回退。

- CLI 扩展
  - 新增 `report from-parquet`