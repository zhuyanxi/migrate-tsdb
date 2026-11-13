# tsdb-reader 公开发布（crates.io）就绪度评估

> 评估对象：`crates/tsdb-reader`（3156 行，其中 src 2234 行 / tests 402 行）
> 结论：**代码质量足以内部使用，但离「可公开发布」还差一轮工程化收尾。** 有 3 个阻塞项（其中 2 个是实证的功能缺陷），其余为 API 收敛、文档与发布流程工作。
>
> **2026-11-13 更新：P0 五项已全部修复并通过打包沙箱验证**，结果见 §0.1。剩余工作为 P1 / P2。

## 0.1 P0 完成情况

| 项 | 状态 | 验证方式 |
|----|------|----------|
| P0-1 `BitReader::read_bits` 位下溢 | ✅ 已修复 | 新增 6 个单测，覆盖截断分裂读、恰好够用的尾部、跨整字重填、`read_bits(0)`、`read_bits(65)`、全长度遍历不 panic；debug 与 release 双 profile 通过 |
| P0-2 `open_info` 丢失 `tenant`/`remote` | ✅ 已修复 | 新增 5 个单测：保留调用方元数据、空 ULID 回退、与 `open` 等价、非 block 目录报错、`block_dir` |
| P0-3 打包产物测试不可运行 | ✅ 已修复 | 最小 fixture 随包（`tests/data`，817 KiB / 117 KiB 压缩）；新增 fixture 漂移检测；沙箱内 41 测试全通过 |
| P0-4 元数据 / README / LICENSE | ✅ 已修复 | `cargo package` 与 `cargo publish --dry-run` 零告警；新增 4 个打包元数据断言 |
| P0-5 `chunks.rs` 文档与实现矛盾 | ✅ 已修复 | 模块文档改为 `data(len)` + CRC 覆盖 `encoding‖data`；新增 6 个布局单测（含 CRC 覆盖编码字节、截断记录、零长度记录、越界引用） |

测试总量：`tsdb-reader` 由 17 项（13 单测 + 4 集成）增至 **41 项**（30 单测 + 9 集成 + 2 文档测试）。

### 0.2 2026-11-13 发布前复核

在 P0 修复之后重新做了一轮发布核对，发现并修正一处**会误导使用者的元数据错误**：

| 项 | 结论 |
|----|------|
| `rust-version = "1.70"` | ❌ 低于依赖下界。`thiserror 2.0.21` 要求 1.77，`serde_json 1.0.154` / `serde_derive 1.0.229` / `proc-macro2` 要求 1.71。1.70–1.76 的用户会拿到构建失败。已改为 **1.77**，并在 1.77.0 工具链上实测 `cargo build` + 41 项测试全部通过 |
| crate 名 | ✅ `tsdb-reader`、`tsdb_reader`、`prom-tsdb` 在 crates.io 均 404（可用）；`tsdb` 已存在但不构成同名/规范化冲突 |
| 版本号 | ✅ 0.1.0 从未发布，无需避让 |
| 许可证 | ✅ `MIT`（2026-11-13 由 Apache-2.0 切换），LICENSE 正文随包；`src/chunkenc/**` 为 Prometheus 移植，仍适用 Apache-2.0，随包附带 `LICENSE-APACHE` 与 `NOTICE` |
| 打包 | ✅ 30 文件 / 817.7 KiB（116.7 KiB 压缩），零告警；解包后 41 项测试通过 |
| 文档注释 | ❌ 仍有 **144 处** `missing_docs`（meta 34 / index 31 / chunkenc 28 / block 12 / chunks 11 / error 10 / tombstones 8 / 其余 10）。不阻塞发布，但 `rustdoc` 会告警，且 crates.io 文档页会大量留白 |
| 编译特性 | ⚠️ 无 `[features]`。`memmap2` / `crc32c` / `serde_json` 为硬依赖，无法裁剪，也不支持 wasm/`no_std`。不阻塞发布 |

结论：**修正 `rust-version` 后即可发布**；144 处文档缺口建议在 0.2.0 之前补齐。

### 0.3 2026-11-13 P1 部分落地：文档与特性开关

| 项 | 结果 |
|----|------|
| `missing_docs` | 144 → **0**。`lib.rs` 开启 `#![deny(missing_docs)]` 与 `#![deny(rustdoc::broken_intra_doc_links)]` |
| 公开项文档批次 | ①`meta.rs`(34) + `index.rs`(31)：数据结构、TOC/series 语义、外键说明；②`chunkenc/*`(36)：位流、varbit、编码常量、直方图样本生命周期；③句柄与错误 `block.rs`(12) + `chunks.rs`(11) + `error.rs`(10) + `tombstones.rs`(8) + `varint.rs`(1)。每批编译 + `cargo rustdoc` 验证后再推进 |
| feature 开关 | 新增 `default = ["mmap", "crc", "meta"]`；`mmap → dep:memmap2`、`crc → dep:crc32c`、`meta → dep:serde_json`。三者相互独立，8 种组合均可编译并通过测试 |
| 条件编译边界 | `mmap`：`Segment::{Mapped,Owned}` 抽象 + `IndexReader::from_mmap` 仅在开启时存在；`crc`：`checksum::verify` 空实现；`meta`：`read_meta`/`discover_blocks`/`BlockReader::open` 整体移除，新增 `open_with_meta` 兜底 |
| 非文件后端 | 新增 `IndexReader::from_bytes` 与 `ChunkReader::from_segments`，两种特性配置下都可用 |
| clippy | 5 条历史告警清零（`too_many_arguments` 显式 allow、`SymbolCache` 类型别名、两处 `x - 1 >=` 化简、测试签名 `&Path`） |

测试：`tsdb-reader` 41 → **44** 项（默认特性），无特性构建 33 项；8 种特性组合 0 失败 0 告警。
打包：32 文件 / 839.7 KiB（123.1 KiB 压缩），零告警；解包后默认特性 44 项、`--no-default-features` 33 项全部通过。

至此 §12 的 P1-6（`#[non_exhaustive]` 部分）、P1-8、P1-9、P1-11（CI 除外）已完成；P1-7（错误类型重构）、P1-10（`Sync` 化）、P1-12（chunkenc 畸形输入测试）仍待办。




---

## 0. 体检数据

| 项 | 现状 | 判定 |
|----|------|------|
| crate 名 `tsdb-reader` | crates.io 返回 404，**可用** | ✅ |
| `cargo package` | 通过（20 文件，97 KiB / 23.7 KiB 压缩） | ✅ |
| 打包产物内 `cargo test` | 41 项全部通过（fixture 随包发布） | ✅ |
| `cargo test --doc` | 2 个（README 示例，随 `meta` 特性条件编译） | ✅ |
| `missing_docs` 警告 | **0**（`#![deny(missing_docs)]` 已开启） | ✅ |
| `clippy` | **0** 条（`--all-targets`，含 `--no-default-features`） | ✅ |
| `cargo fmt --check` | 通过 | ✅ |
| Cargo.toml 元数据 | description/repository/homepage/documentation/readme/keywords/categories/rust-version/docs.rs 均齐备 | ✅ |
| 包内 README / LICENSE | 均已随包，`readme = "README.md"` | ✅ |
| features | 无任何 feature 声明 | ⚠️ |
| CI | 无 `.github/` | ❌ |
| `unsafe` | 2 处（`MmapOptions::map`），无 `unwrap`/`panic!` | ✅ |
| 单元测试 | 30 个（新增 bstream 边界、chunk 布局、block 元数据） | ✅ |

---

## 1. 阻塞项（P0）—— 已全部修复

### P0-1 `BitReader::read_bits` 在截断输入上 debug panic / release 静默错值

`src/chunkenc/bstream.rs:89`：

```rust
if !self.load_next(need) { return Err(Error::Eof(self.pos)); }
v |= (self.buffer >> (self.valid - need)) & mask(need);   // valid < need 时下溢
self.valid -= need;                                       // 同上
```

`load_next` 在缓冲区剩余字节少于 `need/8+1` 时会**成功返回但只填充部分位**，此时 `valid < need`。

实证（9 字节输入，先读 56 位再读 17 位）：

```
debug   : panicked at bstream.rs:89: attempt to subtract with overflow
release : read_bits(17) returned Ok(0x0)   ← 静默返回错误数据
```

影响：`verify_crc=false` 或 CRC 恰好匹配的畸形/截断 chunk 会得到错误样本值而不是 `Error`。对一个解析不可信输入的公开库，这是**数据正确性**问题。
修复：`load_next` 返回实际加载位数，不足 `need` 时返回 `Err(Error::Eof)`；`bits_left_hint` 用 `saturating_sub`。补 1 个边界单测 + 1 个 fuzz 目标。

### P0-2 `BlockReader::open_info` 丢失 `tenant` / `remote`

```rust
pub fn open_info(info: BlockInfo, opts: ReaderOptions) -> Result<Self> {
    let mut reader = Self::open(&info.path, opts)?;   // 重新构造 BlockInfo
    reader.info.warnings = info.warnings;             // 只回填 warnings
    Ok(reader)
}
```

`Self::open` 内部把 `tenant` 置空、`remote` 置 false。传入的 `info` 只有 `warnings` 被保留，`tenant`/`remote` 静默丢失。仓库内引擎走 `planned.info` 因而未暴露，但这是公开 API 的契约缺陷。
修复：`open_info` 以传入的 `info` 为准，仅刷新 `index_bytes`/`chunks_bytes`/`meta`。

### P0-3 打包产物的测试跑不起来

`tests/fixtures.rs` 读 `../../fixtures/gen/data`，该目录不在包内（`cargo package --list` 只含 `src/` + `tests/`）。

```
target/package/tsdb-reader-0.1.0$ cargo test
---- block_matches_prometheus_ground_truth panicked: NotFound
---- chunk_encodings_match_prometheus_ground_truth panicked: NotFound
```

`cargo publish` 只做 build 校验、不跑测试，因此会**静默发出一个测试必然失败的包**。
修复三选一：(a) 把最小 fixture（1 个 block + chunks.json，约 300 KB）纳入 `crates/tsdb-reader/tests/data/`；(b) 改为 `#[ignore]` + 环境变量开关，包内默认跳过；(c) 把 fixture 生成脚本写成 `build.rs` 的 dev-only 步骤。推荐 (a)+(b) 组合：小 fixture 随包发布，大 fixture 保持外部。

---

## 2. 公开 API 的稳定性与命名

| 问题 | 位置 | 建议 |
|------|------|------|
| 所有字段 `pub` 且无 `#[non_exhaustive]` | `BlockInfo`、`BlockMeta`、`BlockStats`、`ReaderOptions`、`Toc`、`HistogramSample`、`Error` | 加 `#[non_exhaustive]`，或改为私有字段 + accessor；否则任何加字段都是 breaking |
| `ReaderOptions` 只有两个 bool，无 builder | `block.rs` | 保留 struct 但加 `#[non_exhaustive]`，后续加字段不破 SemVer |
| 内部实现被导出成公共 API | `pub mod varint`、`chunkenc::{bstream,varbit,xor,xor2,histogram}` | 降为 `pub(crate)`；只保留 `chunkenc::decode`、`SampleSink`、`HistogramSample`、`Span`、编码常量。`BitReader` 若要公开则单独 `pub mod` 并在文档中承诺稳定 |
| 同名不同义 | `IndexReader::bytes() -> &[u8]` vs `ChunkReader::bytes() -> u64` | 前者改 `as_bytes()`，后者改 `size_bytes()` |
| 名不符实 | `SeriesRef` 实际是「series 记录 + 标签 + chunk 列表」 | 改名 `Series`，`id`/`offset` 补文档说明 v1 与 v2/v3 差异 |
| 两套叫法 | `BlockReader::block_id()` vs `BlockInfo::ulid()` | 统一为 `ulid()` |
| 未导出的返回类型 | `chunk_reader() -> &ChunkReader`，但 `ChunkReader` 未在根导出 | 根 `pub use chunks::{ChunkReader, ChunkSlice}` |
| 可有可无的自由函数 | `block_dir(root, ulid)` | 删除或标 `#[doc(hidden)]` |
| 同义 API 并存 | `Tombstones::is_deleted()` 与 `tombstones::is_deleted()` | 保留方法，自由函数标 `#[doc(hidden)]` 或合并 |
| crate 内泄入上层概念 | `BlockInfo.tenant` / `remote` / `key()` 是 Mimir 迁移工具的概念 | 移到 `migrate-core`，或用 `metadata: BTreeMap<String, String>` 之类的通用扩展位；`key()` 明确标注为工具侧便利方法 |
| 线程模型未声明 | `IndexReader` 含 `RefCell` → `Send` 但 **`!Sync`**；`BlockReader` 同样 `!Sync`；`ChunkReader` 是 `Send + Sync` | 在 crate 文档写明：index 单线程、chunk 段可跨线程共享（`chunk_reader()` / `into_parts()`）。若想 `Sync`，把 `Symbols` 的 `RefCell` 换成无锁缓存或去掉缓存 |

---

## 3. 错误处理与类型设计

| 问题 | 影响 | 建议 |
|------|------|------|
| `Error` 无 `#[non_exhaustive]` | 加变体即 breaking | 加 |
| `Format(String)` / `Decode(String)` 丢失结构 | 调用方只能字符串匹配，无法区分「版本不支持」与「数据损坏」 | 引入 `kind()` 或细分变体（`Index`/`Chunk`/`Meta` + `offset` 字段） |
| `Checksum(&'static str)` 无法带位置 | 报错只说 "chunk"，不知道哪个 segment/offset | 改 `Checksum { section: &'static str, offset: u64, expected: u32, actual: u32 }` |
| `Eof(usize)` 语义过载 | 有时是绝对文件偏移、有时是相对位置 | 拆 `Eof { offset: u64 }` 与 `Truncated { need: usize, have: usize }` |
| `Io` 无路径上下文 | 报 `io error: No such file`，不知是哪个文件 | 包一层 `path` 字段，或用 `with_context` 风格 helper |
| 无 `From<serde_json::Error>` | `read_meta` 手工映射成字符串 | 加变体 `Meta(#[from] serde_json::Error)` |
| `Error` 未实现 `PartialEq`（合理）但与 `Result` 别名混用 | — | 保持，文档说明错误不适合被程序化分类 |

---

## 4. 缺失 / 边界功能

| 缺口 | 说明 | 优先级 |
|------|------|--------|
| **无 label matcher / postings 支持** | 只能顺序扫全部 series，无法按 `{job="api"}` 过滤。这是同类库（`prometheus-parse`、`tsdb`）用户的第一需求 | 高（1.0 前） |
| 无 `open_from_bytes` / `Read + Seek` 后端 | 现在只接受 `&Path` + mmap，无法在 wasm、网络流、内存测试中使用 | 高 |
| 无「series + samples」高层迭代器 | 用户必须自己组合 `SeriesIter` + `ChunkReader::chunk` + `chunkenc::decode` + `is_deleted`，约 40 行样板 | 中 |
| `ChunkSlice` 无 `decode()` 便捷方法 | `chunkenc::decode(enc, data, sink)` 需手动传三个参数 | 中 |
| `discover_blocks` 重叠检测只看相邻块 | `A[0,100] B[50,60] C[70,200]`：B 报冲突，C 与 A 的实际重叠**漏报**（应维护 running max） | 中（仅影响告警质量） |
| 无 `chunks_head/` / WAL 支持 | 文档已声明 out of scope，但需在 crate 级文档明确写出「只支持持久化 block，不含 head/WAL」 | 中（文档） |
| 无 index v4 前瞻 | 目前 v1–v3，Prometheus 3.x 仍是 v3 | 低 |
| 无 stale marker 判定 helper | `STALE_NAN_BITS` 已导出，但没有 `is_stale_nan(f64)` | 低 |
| `Tombstones::open` 全量读入内存 | 与其余部分的 mmap 策略不一致；tombstones 通常很小，可接受但要说明 | 低 |

---

## 5. features 与平台兼容性

当前**零 feature**，`memmap2`/`crc32c`/`serde`/`serde_json` 全是硬依赖。

| 建议 feature | 默认 | 作用 |
|--------------|------|------|
| `mmap` | on | 关闭后去掉 `memmap2`，只保留 `open_from_bytes`（配合 §4 的后端抽象） |
| `crc` | on | 关闭后去掉 `crc32c` 依赖；保留 `ReaderOptions::verify_crc=false` 路径 |
| `serde` | on | 关闭后 meta 类型不带 `Serialize`（只读场景只需 `Deserialize`） |
| `meta` | on | 关闭后去掉 `serde_json`，调用方自行解析 `meta.json` 后构造 `BlockMeta` |

平台事实：

- `memmap2` 仅支持 unix/windows，**不支持 `wasm32-unknown-unknown`**；`std::fs` 遍布 → 无法 `no_std`。crate 文档要写明目标平台。
- `ReaderOptions::mmap_advise` 只在 `#[cfg(unix)]` 生效，Windows 上静默忽略 → 文档说明。
- 端序安全：全部用 `from_be_bytes`/`from_le_bytes`，无 transmute ✅。
- Windows 上 mmap 会持有文件句柄，导致 Prometheus 无法删除该 block → 文档提示「读完即 drop」。
- 32 位平台：index 单文件大于 2 GB 时 `map` 失败；建议文档标注 64 位目标。

---

## 6. 依赖与版本管理

| 项 | 现状 | 建议 |
|----|------|------|
| 依赖数量 | 5 个直接依赖（memmap2/crc32c/serde/serde_json/thiserror） | 合理；按 §5 收敛 |
| `dev-dependencies` 重复声明 `crc32c` | 与 `[dependencies]` 完全重复 | 删除 |
| 版本约束 | 全部宽松主版本（`0.9`/`0.6`/`1`/`2`） | 保持；建议加 `Cargo.lock` 到 CI 的 `-Z minimal-versions` 检查 |
| MSRV | 已声明 `rust-version = "1.77"` | 由依赖下界决定：`thiserror 2.0.21` 要求 1.77，`serde_json`/`serde_derive`/`proc-macro2` 要求 1.71。已在 1.77.0 工具链上实测 `build` + 41 项测试通过 |
| workspace 继承 | `version.workspace = true` 等 | 发布时 cargo 会自动展开，无需改动；但 `description` 等元数据建议写在 crate 本地而非 workspace |
| 许可合规 | 依赖均为 Apache-2.0/MIT 兼容 | CI 加 `cargo deny check licenses advisories` |

---

## 7. 文档与示例

- **145 条 `missing_docs`**（`cargo rustdoc -- -W missing_docs`）。公开库应开 `#![deny(missing_docs)]`，逐条补齐。
- 模块级文档质量不错（`index.rs`、`tombstones.rs`、`varint.rs` 都解释了格式来源与坑），可直接提升为 crate 文档的一部分。
- **0 个文档测试**。crate 文档应至少包含 3 个可编译示例：
  1. `discover_blocks` + 打开单个 block；
  2. 遍历 series 并逐 chunk 解码浮点样本；
  3. 应用 tombstones 过滤。
- 无 `README.md`（`readme = false`），crates.io 页面会**空白**。README 需含一句定位（"零拷贝、只读、面向全量导出的 Prometheus TSDB block 解析器"）、支持范围（v1–v3、6 种 chunk 编码）、限制（无 head/WAL、无 postings）。
- `chunks.rs` **模块文档与实现矛盾**：文档写 `len` 包含 encoding 字节、数据为 `data(len-1)`；实现是 `len` 不含 encoding（`data_end = enc_start + 1 + len`）。应更正文档。
- 建议补 `docs.rs` 元数据：

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

---

## 8. 测试覆盖

| 层次 | 现状 | 缺口 |
|------|------|------|
| 单元测试 | 13 个：varint 3、bstream 3、varbit 2、index 2、meta 1、tombstones 2 | **6 种 chunk 解码器没有单元级测试**，仅靠 fixture 集成测试；`Symbols` 缓存、`scan_stats`、`SeriesIter` 提前终止、v1 索引路径均无覆盖 |
| 集成测试 | 9 个：真实 block 对拍 2、chunk 编码对拍 1、fixture 漂移 1、CRC 2、打包元数据 4 | `chunkenc` 仍缺畸形输入单测（P1-12） |
| 文档测试 | 2 个（README 两个示例，`no_run` 编译通过） | 可继续补充 tombstone 过滤示例 |
| 模糊测试 | 无 | 解析不可信字节的库应有 `cargo-fuzz` 目标：`varint`、`parse_series`、`chunkenc::decode`、`Tombstones::open` |
| 属性测试 | 无 | `uvarint`/`varint` 的 round-trip（用 `proptest`）成本极低 |
| 基准 | 无（仓库有 `scripts/bench.sh` 但在 crate 外） | 若以「快」为卖点，`benches/` 需要一组可复现数据 |

优先补：`chunkenc` 的畸形输入单测（尤其 P0-1 的边界）、fuzz 目标、doc test。

---

## 9. clippy 与格式化

- `cargo fmt --check`：通过 ✅
- `cargo clippy --all-targets`：13 条，其中 lib 5 条：
  - `histogram.rs:161` / `xor2.rs:139`：`n - 1 >= x` → `n > x`（2 条）
  - `histogram.rs:132`：`decode_st` 8 个参数（建议引入 `HistogramState` 结构体）
  - `index.rs:88`：`RefCell<Vec<Option<(u32, Arc<str>)>>>` 类型过复杂（建议 `type SymbolCache = ...` 或改设计）
  - `index.rs:128`：手写 `% SYMBOL_FACTOR == 0` → `is_multiple_of`
- 建议在 `lib.rs` 顶部固定 lint 策略：

```rust
#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::cast_possible_truncation)]
```

- CI 至少跑：`fmt --check`、`clippy -- -D warnings`、`test`、`test --doc`、`doc --no-deps`、`package`、MSRV 构建。

---

## 10. 许可证与元数据

当前 `Cargo.toml` 只有 4 个字段（name/version/edition/license）。`cargo package` 的原话：

> `warning: manifest has no description, documentation, homepage or repository`

补齐建议：

```toml
[package]
name = "tsdb-reader"
description = "Zero-copy, read-only parser for Prometheus TSDB blocks (index v1-v3, all chunk encodings)"
repository = "https://github.com/<org>/migrate-tsdb"
homepage = "https://github.com/<org>/migrate-tsdb/tree/main/crates/tsdb-reader"
documentation = "https://docs.rs/tsdb-reader"
readme = "README.md"
keywords = ["prometheus", "tsdb", "metrics", "parser", "zero-copy"]
categories = ["parser-implementations", "database", "encoding"]
rust-version = "1.77"
license = "MIT"
include = ["src/**", "tests/**", "README.md", "LICENSE", "Cargo.toml"]
```

- 包内**必须自带许可正文**：`license = "MIT"` 只满足 crates.io 的字段校验，tarball 仍需实拷 `LICENSE`。移植自 Prometheus 的 `chunkenc/**` 另需 `LICENSE-APACHE` 与 `NOTICE`，三者都要进 `include` 列表。
- `keywords` 最多 5 个、`categories` 必须取自 crates.io 白名单。
- 若发布到组织账号，`authors` 可省略（crates.io 用账号信息）。

---

## 11. 语义化版本与发布前检查清单

### 版本策略（0.x 阶段）

- `0.1.0` 首发即公开 API 冻结的起点。0.x 允许 minor 版本 breaking，但应在 README 明确「1.0 之前 minor 可能破坏 API」。
- 建议流程：`0.1.x` 只修 bug → `0.2.0` 收敛 API（P1 全部完成）→ `0.3.0` 加 matcher/后端抽象 → `1.0.0` 冻结。
- 工具：`cargo semver-checks check-release`（对比上一版 API）、`cargo public-api`（人工审阅 diff）。

### 发布前清单

```bash
# 1. 静态检查
cargo fmt --all -- --check
cargo clippy -p tsdb-reader --all-targets -- -D warnings
cargo rustdoc -p tsdb-reader --lib -- -D missing_docs

# 2. 测试
cargo test -p tsdb-reader
cargo test -p tsdb-reader --doc
cargo test -p tsdb-reader --release        # 上溢/下溢行为与 debug 不同，必须两种 profile 都跑

# 3. 打包沙箱验证（关键：这一步当前会失败）
cargo package -p tsdb-reader --allow-dirty
cd target/package/tsdb-reader-<ver> && cargo test --offline

# 4. 元数据与许可
cargo publish -p tsdb-reader --dry-run
cargo deny check licenses advisories

# 5. API 稳定性
cargo semver-checks check-release -p tsdb-reader   # 0.1.0 之后每次发布前

# 6. 文档预览
cargo doc -p tsdb-reader --no-deps --open
```

### CHANGELOG

无 `CHANGELOG.md`。建议 `git-cliff` 或 `keep-a-changelog` 格式，发布前把 `Unreleased` 段落定版。

---

## 12. 待办清单（按优先级）

### P0 — 阻塞发布（已全部完成）

| # | 事项 | 验收标准 | 状态 |
|---|------|----------|------|
| 1 | 修 `BitReader::read_bits` 位下溢 | debug 与 release 均返回 `Err(Eof)`；新增边界单测 | ✅ 6 个单测；fuzz 目标留给 P2-18 |
| 2 | 修 `BlockReader::open_info` 丢失 `tenant`/`remote` | 单测断言 `open_info(info).info().tenant == info.tenant` | ✅ 5 个单测 |
| 3 | 让打包产物的测试可运行 | `cd target/package/tsdb-reader-*; cargo test` 全绿 | ✅ 41/41 通过 |
| 4 | 补齐 Cargo.toml 元数据 + README + LICENSE | `cargo package` 无元数据告警；crates.io 页面有描述与 README | ✅ 零告警，`--dry-run` 通过 |
| 5 | 修正 `chunks.rs` 模块文档中关于 `len` 的错误描述 | 文档与 `data_end = enc_start + 1 + len` 一致 | ✅ 6 个布局单测钉住语义 |

### P1 — 发布前强烈建议

| # | 事项 | 验收标准 |
|---|------|----------|
| 6 | 公开 API 收敛：`#[non_exhaustive]`、私有化 `varint` 与 `chunkenc` 子模块、统一命名 | `cargo public-api` 输出中不再出现内部解析原语 |
| 7 | 错误类型重构：带位置/路径、`non_exhaustive`、可分类 | 每个变体都有文档与构造示例 |
| 8 | 补 rustdoc 至 0 警告 + 3 个 crate 级 doc test | `cargo test --doc` ≥ 3 passed；`-D missing_docs` 通过 |
| 9 | 引入 features：`mmap` / `crc` / `serde` / `meta` | `--no-default-features` 可编译并通过单测 |
| 10 | 明确线程模型（`Sync` 化或文档化） | crate 文档含线程模型章节；或 `IndexReader: Sync` 静态断言测试 |
| 11 | clippy 清零 + CI 流水线 | `-D warnings` 通过；CI 覆盖 fmt/clippy/test/doc/package/MSRV |
| 12 | 补 `chunkenc` 畸形输入单测 | 6 种编码各至少 1 条截断/越界用例 |

### P2 — 1.0 之前完成

| # | 事项 | 验收标准 |
|---|------|----------|
| 13 | label matcher / postings 支持 | 可按 `{name=value}` 过滤 series，附基准 |
| 14 | 后端抽象：`open_from_bytes` / `Read + Seek` | 无 mmap 也能读，wasm 可编译（无 fs） |
| 15 | 高层 `samples()` 迭代器（含 tombstone 过滤） | 文档示例从 ~40 行降到 ~10 行 |
| 16 | `discover_blocks` 重叠检测改用 running max | 嵌套 block 场景有回归测试 |
| 17 | 补 `benches/` 与可复现数据 | 至少 index 扫描 / chunk 解码两组基准 |
| 18 | fuzz 目标（`cargo-fuzz`） | CI 每日跑 5 分钟，无 crash |
| 19 | CHANGELOG + SemVer 策略 + `cargo semver-checks` | 发布流程文档化 |
| 20 | 删除冗余的 `dev-dependencies.crc32c` | — |
