# Parser Test Coverage

| RDB Type | Encoding Variant | Test Case(s) | Covered? |
|----------|-----------------|--------------|----------|
| String   | Raw             | `string_raw_encoding_test` | ✅ |
| String   | Int             | `string_int_encoding_test` | ✅ |
| String   | LZF             | `string_lzf_encoding_test` | ✅ |
| List     | List (legacy)   | `list_raw_encoding_test` | ✅ |
| List     | ZipList         | `list_ziplist_encoding_test`<br/>`list_ziplist_scan_path_test` | ✅ |
| List     | QuickList       | `list_quicklist_encoding_test`<br/>`list_quicklist_lzf_compressed_test` | ✅ |
| List     | QuickList2      | `list_quicklist2_encoding_test` | ✅ |
| Set      | Raw             | – | ❌ |
| Set      | IntSet          | – | ❌ |
| Set      | ListPack        | – | ❌ |
| ZSet     | Raw             | – | ❌ |
| ZSet     | ZipList         | – | ❌ |
| ZSet     | ListPack        | – | ❌ |
| Hash     | ZipMap          | – | ❌ |
| Hash     | ZipList         | – | ❌ |
| Hash     | ListPack        | – | ❌ |
| Stream   | ListPacks       | – | ❌ |
| Module   | --              | – | ❌ |
| Auxiliary | --              | `empty_rdb_test` | ✅ | 