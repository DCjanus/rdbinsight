# Parser Trace Points Coverage

本文件记录当前解析器中已插桩的 `parser_trace!` 事件以及对应的集成测试覆盖情况，方便后续维护与补充。

| Trace Path | 触发场景 / 源位置 | 覆盖测试 |
|------------|------------------|-----------|
| `quicklist.ziplist.raw` | `src/parser/record_list.rs:125`<br/>QuickList (Redis 6.0) 解析 ziplist 节点走 *raw* 分支 | `list_quicklist_ziplist_raw_node_test` |
| `quicklist.ziplist.lzf` | `src/parser/record_list.rs:142`<br/>QuickList 解析 ziplist 节点走 *LZF* 解压分支 | `list_quicklist_lzf_compressed_test` |
| `intset.raw` | `src/parser/record_set.rs:104`<br/>IntSet (RDB type 11) 原始解析分支 | `set_intset_encoding_test` |
| `quicklist2.plain` | `src/parser/record_list.rs:402`<br/>QuickList2 解析 *plain* 节点 | `list_quicklist2_plain_node_test` |
| `quicklist2.packed.raw` | `src/parser/record_list.rs:407`<br/>QuickList2 解析 *listpack* 节点（未压缩） | `list_quicklist2_listpack_raw_node_test` |
| `quicklist2.packed.lzf` | `src/parser/record_list.rs:481`<br/>QuickList2 解析 LZF 压缩的 *listpack* 节点 | `list_quicklist2_listpack_lzf_node_test` |
| `zset.skiplist` | `src/parser/record_zset.rs:31`<br/>ZSet 解析 *skiplist* 路径 | `zset_skiplist_encoding_test` |
| `zset2.skiplist` | `src/parser/record_zset2.rs:30`<br/>ZSet2 解析 *skiplist* 路径 | `zset2_skiplist_encoding_test` |
| `hash.zipmap.raw` | `src/parser/record_hash.rs:303`<br/>Hash/ZipMap 走 *raw/LZF* 路径 | `hash_zipmap_fixture_raw_test`<br/>`hash_zipmap_fixture_lzf_test` |
| `module2.raw` | `src/parser/record_module.rs:63`<br/>Module2 记录写入完成 | `module2_encoding_test` |

当前 `tests/parser_test.rs` 已覆盖上述所有十个追踪点。若未来新增 `parser_trace!` 事件，请同时在此表补充并编写对应测试。

# 支持的 RDB 类型与编码概览

以下内容基于 `src/parser/definitions.rs` 中的 **RDBType** 枚举及各 `record_*` 解析器（见 `src/parser/` 目录）整理而得。

| RDBType (十进制) | 对应数据结构 | 已实现的编码 / 容器 | 解析实现 | 集成测试 |
|-----------------|--------------|--------------------|-----------|-----------|
| 0  (`String`)   | String 键值  | Raw / Int / LZF | `record_string.rs` | `string_raw_encoding_test`<br/>`string_int_encoding_test`<br/>`string_lzf_encoding_test` |
| 1  (`List`)     | List (原始)  | Plain List | `record_list.rs` (`ListRecordParser`) | `list_raw_encoding_test` |
| 2  (`Set`)      | Set (原始)   | Raw Hash-Table | `record_set.rs` (`SetRecordParser`) | `set_raw_encoding_test` |
| 3  (`ZSet`)     | Sorted Set   | SkipList | `record_zset.rs` (`ZSetRecordParser`) | `zset_skiplist_encoding_test` |
| 4  (`Hash`)     | Hash         | Raw / ZipList / ListPack | `record_hash.rs` (`HashRecordParser`) | `hash_raw_encoding_test`<br/>`hash_ziplist_encoding_test`<br/>`hash_listpack_encoding_test` |
| 5  (`ZSet2`)    | Sorted Set (double score) | SkipList | `record_zset2.rs` (`ZSet2RecordParser`) | `zset2_skiplist_encoding_test` |
| 6  (`ModulePreGA`) | Module (pre-GA) | – | **不支持** | – |
| 7  (`Module2`)  | Module       | Raw | `record_module.rs` (`Module2RecordParser`) | `module2_encoding_test` |
| 9  (`HashZipMap`) | Hash (ZipMap) | ZipMap raw / LZF | `record_hash.rs` (`HashZipMapRecordParser`) | `hash_zipmap_fixture_raw_test`<br/>`hash_zipmap_fixture_lzf_test` |
| 10 (`ListZipList`) | List (ZipList) | ZipList / *ZipList (raw/LZF)* | `record_list.rs` (`ListZipListRecordParser`) | `list_ziplist_encoding_test`<br/>`list_ziplist_scan_path_test` |
| 11 (`SetIntSet`) | Set (IntSet) | IntSet | `record_set.rs` (`SetIntSetRecordParser`) | `set_intset_encoding_test` |
| 12 (`ZSetZipList`) | ZSet (ZipList) | ZipList | `record_zset.rs` (`ZSetZipListRecordParser`) | `zset_ziplist_encoding_test` |
| 13 (`HashZipList`) | Hash (ZipList) | ZipList | `record_hash.rs` (`HashZipListRecordParser`) | `hash_ziplist_encoding_test` |
| 14 (`ListQuickList`) | List (QuickList) | QuickList-ZipList raw / LZF | `record_list.rs` (`ListQuickListRecordParser`) | `list_quicklist_encoding_test`<br/>`list_quicklist_lzf_compressed_test`<br/>`list_quicklist_ziplist_raw_node_test` |
| 15 (`StreamListPacks`) | Stream (v1) | – | – | – |
| 16 (`HashListPack`) | Hash (ListPack) | ListPack raw / LZF | `record_hash.rs` (`HashListPackRecordParser`) | `hash_listpack_encoding_test` |
| 17 (`ZSetListPack`) | ZSet (ListPack) | – | – | – |
| 18 (`ListQuickList2`) | List (QuickList2) | Plain / ListPack raw / LZF | `record_list.rs` (`ListQuickList2RecordParser`) | `list_quicklist2_encoding_test`<br/>`list_quicklist2_plain_node_test`<br/>`list_quicklist2_listpack_raw_node_test`<br/>`list_quicklist2_listpack_lzf_node_test` |
| 19 (`StreamListPacks2`) | Stream (v2) | – | – | – |
| 20 (`SetListPack`) | Set (ListPack) | ListPack raw / LZF | `record_set.rs` (`SetListPackRecordParser`) | `set_listpack_encoding_test`<br/>`set_listpack_scan_path_test`<br/>`set_listpack_large_string_variants_test`<br/>`set_listpack_integer_variants_test` |
| 21 (`StreamListPacks3`) | Stream (v3) | – | – | – |
| 22 (`HashMetadataPreGA`) | Hash Meta (pre-GA) | – | – | – |
| 23 (`HashListPackExPreGA`) | Hash LP-Ex (pre-GA) | – | – | – |
| 24 (`HashMetadata`) | Hash Meta | – | – | – |
| 25 (`HashListPackEx`) | Hash LP-Ex | – | – | – |
| – | Meta 信息 (Aux / SelectDB / ResizeDB) | – | `src/parser/item.rs` 及状态机 | `empty_rdb_test` |

说明：表中 "–" 表示当前尚未支持的解析逻辑与测试。后续若新增支持，请补充相应列的数据。

> 注：如上表中"编码 / 容器"列出现 *raw / LZF*，表示对应分支已考虑到 Redis 的 LZF 压缩选项并在测试中覆盖。 