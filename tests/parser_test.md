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

当前 `tests/parser_test.rs` 已覆盖上述所有六个追踪点。若未来新增 `parser_trace!` 事件，请同时在此表补充并编写对应测试。 