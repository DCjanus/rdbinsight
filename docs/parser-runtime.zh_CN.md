# Parser Runtime 设计

本文档描述当前 parser runtime 的最终抽象边界。它不追求兼容旧的内部接口，目标是在保持增量解析和性能特征的前提下，让 parser 的消费语义更明确、更容易维护。

## 核心结论

Parser runtime 保留两阶段模型：

- `ParserInit::init(&mut View) -> ParseResult<Self>`：只做可回溯的初始化探测，不允许消费 `Buffer`。
- `Parser::call(&mut Buffer) -> AnyResult<Output>`：执行不可回溯的提交式解析，允许推进 `Buffer`。

`Cursor` 不再作为 call 阶段的通用包装层。`Buffer` 本身已经是提交式数据游标；再把 call 包进 `Cursor` 会增加样板代码，却不能提供额外的回溯安全性。因此 `Cursor` 只负责一件事：把 `View` 上成功完成的 init 结果提交到 `Buffer`。

## 角色分工

### Buffer

`Buffer` 是真实数据窗口，也是 call 阶段唯一的提交式游标。它持有当前可读字节和全局 offset，`Parser::call` 可以通过它读取、跳过、消费数据。

call 阶段的约束是：

- 成功时可以消费任意已经确认属于当前 parser 的字节。
- `NeedMoreData` 表示当前 parser 暂时无法完成，已经消费的状态由 parser 自己维护。
- 其它错误是 fatal，不参与 branch 回溯。

### View

`View` 是 init 阶段的只读探测游标。它借用 `Buffer`，只维护一个本地 `consumed` offset，不会调用 `Buffer::consume`。

`View` 提供两类能力：

- `parse_init`/`read`：复用现有 byte-level reader，并把 `NeedMoreData` 转成 `ParseResult::NeedMore`。
- `init_parser`：在同一个 `View` 上初始化子 parser；子 parser 失败或需要更多数据时回滚本地 offset，成功时推进本地 offset。

这让 init 代码可以像组合器一样顺序描述“我要读什么”，同时保证分支失败不会污染真实 buffer。

### Cursor

`Cursor` 是 init commit 控制器，不是第二套数据读取接口。

它的职责是：

1. 从 `Buffer` 当前窗口和指定 offset 创建 `View`。
2. 调用 `ParserInit::init`。
3. 只有 init 成功时，按 `View::consumed()` 推进真实 `Buffer`。
4. init 返回 `NeedMore` 或错误时，不消费真实 `Buffer`。

因此 `Cursor` 只出现在“需要把 init 探测结果提交”的地方，例如 RDB item dispatch、`Seq`/`Reduce` 组合器的子 parser 初始化、以及 call 阶段动态初始化下一个子状态。

## 错误语义

`ParseResult` 只用于 init 阶段：

- `Ok(T)`：初始化成功，`View` 本地 offset 已推进。
- `NeedMore`：输入不足，真实 `Buffer` 不变。
- `Err(ParseError)`：初始化失败，真实 `Buffer` 不变。

`ParseError` 区分 recoverable/fatal，为后续 branch parser 保留语义空间。当前大部分 RDB parser 没有复杂分支，因此主要使用 fatal；但边界已经不再依赖 `anyhow::Result<(&[u8], T)>` 这种旧形状。

call 阶段继续使用 `AnyResult`，原因是 call 错误不可回溯。它需要的是直接、低样板、低开销的 fatal/need-more 传播，而不是再包一层 `ParseResult`。

## 维护收益

新边界相比旧设计的收益：

- init 不再接收 `&Buffer + &[u8]` 这组容易错配的参数，offset 推进集中在 `View`。
- 真实 `Buffer` 的消费集中由 `Cursor` 提交，init 成功前不会误消费。
- 子 parser 初始化统一使用 `view.init_parser::<T>()`，减少手写 `remaining` 和 `consume_to`。
- call 阶段保留原来的提交式 `Buffer` 语义，避免为了形式统一引入额外 wrapper 和大量样板代码。
- `Seq`、`Reduce`、`RDBStrBox` 等组合器成为 parser 组合的主要入口，上层 record parser 更接近业务结构描述。

## Breaking Change

这是内部 parser 抽象的 breaking change：

- `ParserInit::init` 从旧的 `(&Buffer, &[u8]) -> AnyResult<(&[u8], Self)>` 改为 `(&mut View) -> ParseResult<Self>`。
- 旧的 `parser::state` 抽象已被 `parser::runtime` 和 `parser::core::{View, Cursor, ParseResult}` 取代。
- 内部 parser 组合代码需要使用 `view.parse_init`、`view.init_parser` 或 `Cursor::init_commit` 表达初始化和提交语义。
