# TSDB 格式实现笔记

> 本文记录 `tsdb-reader` 实际依赖的磁盘格式细节，以及实现时容易踩错的点。每一条都对应 `prometheus/tsdb` 中的源码；与官方 `docs/format` 不一致时，以源码为准（文末列出差异）。
>
> 参考源码：
> - `tsdb/docs/format/index.md`、`chunks.md`、`tombstones.md`
> - `tsdb/index/index.go`（Reader / Symbols / Decoder.Series）
> - `tsdb/chunks/chunks.go`（Reader.ChunkOrIterable / Writer.writeChunks）
> - `tsdb/tombstones/tombstones.go`（ReadTombstones / Decode）
> - `tsdb/chunkenc/{xor,xor2,bstream,varbit,histogram,float_histogram,histogram_st,st,histogram_meta}.go`

---

## 1. Block 目录与 meta.json

```
<data-dir>/
├── 01HXXXXXXXXXXXXXXXXXXXXXXX/
│   ├── meta.json
│   ├── index
│   ├── tombstones
│   └── chunks/000001, 000002, ...
├── wal/                # 不读取
└── chunks_head/        # 不读取
```

`meta.json`（`tsdb/block.go` 的 `BlockMeta`）：

```json
{
  "ulid": "01HXXXXXXXXXXXXXXXXXXXXXXX",
  "minTime": 1700000000000,
  "maxTime": 1700022485001,
  "stats": {
    "numSamples": 4951,
    "numFloatSamples": 4831,
    "numHistogramSamples": 120,
    "numSeries": 42,
    "numChunks": 100,
    "numTombstones": 41
  },
  "compaction": { "level": 1, "sources": ["..."], "parents": [], "deletable": false },
  "version": 2
}
```

- 时间单位毫秒，Block 范围是**半开区间** `[minTime, maxTime)`。
- `stats` 可能缺失或为 0（老版本/自定义工具）；此时通过一次 **只扫 index 结构、不解析 symbol、不解码样本** 的预扫描得到精确的 series/chunk 总数。
- `numSamples` 是**应用 tombstones 之前**的样本数；迁移行数会小于它。
- 发现规则：根目录若自身含 `meta.json` 则视为单 Block；否则遍历子目录，要求同时存在 `meta.json`、`index`、`chunks/`。按 `minTime` 排序。
- 重叠 Block（崩溃后可能出现）：只告警，不自动去重；输出保留 `block` 分区，便于上层按时间去重。

---

## 2. index 文件

### 2.1 头部与 TOC

```
magic 0xBAAAD700 (4B BE) | version (1B) | sections... | TOC (52B)
```

- 支持 version 1、2、3。
- TOC 固定在文件**末尾 52 字节**：6 个 BE64 + 4B CRC32C（Castagnoli）。
- TOC 字段顺序：`symbols, series, label_indices, label_indices_table, postings, postings_table`。
- CRC 覆盖前 48 字节（`index.go: NewTOCFromByteSlice`）。
- 顺序全量导出只需要 `symbols` 与 `series` 两段；postings 完全不解析。

### 2.2 Symbol Table

```
len(4B BE) | count(4B BE) | (len(uvarint) + bytes)* | CRC32C(4B BE)
```

- `len` 是 `count` 字段起、到 CRC 前的字节数；CRC 覆盖这 `len` 字节。
- **符号引用语义随版本不同**：
  - v1：引用是符号字符串在文件中的**绝对字节偏移**。
  - v2/v3：引用是**序号**（0,1,2,…）。为降低内存，读取时只记录每 32 个符号的绝对偏移（与 Prometheus `symbolFactor=32` 一致），查询时最多向前走 32 条。
- 我们额外加了一个 16K 槽的直接映射缓存（`ref -> Arc<str>`），命中率高且内存固定。

### 2.3 Series 区段

Series 按 label 集合字典序排列，每条记录：

```
len(uvarint, payload 字节数) | payload | CRC32C(4B BE)
payload:
  labels_count(uvarint)
  [name_ref(uvarint32), value_ref(uvarint32)] * labels_count
  chunks_count(uvarint)
  chunk_meta...
```

Chunk meta 编码（`index.go: Decoder.Series`）：

```
第一条：
  mint        varint64 (zig-zag)
  maxt - mint uvarint64
  ref         uvarint64
后续：
  mint - prev_maxt  uvarint64
  maxt - mint       uvarint64
  ref - prev_ref    varint64 (zig-zag)
```

关键点：

1. **记录起点按 16 字节对齐**（v2/v3）。`TOC.series` 本身可能**未对齐**，第一条记录前有 0 填充；必须从 `align_up(TOC.series, 16)` 开始遍历。这是实现时最容易踩的坑（我们第一版因此读到 0 条 series）。
2. `len` **不包含** 长度字段自身，也不含 CRC。
3. CRC 覆盖 payload（不含长度字段、不含 CRC）。
4. 区段结束边界取 `TOC.label_indices`（v2/v3 写完后立即记录），退化时取 `postings` / 文件末尾；遍历时遇到 `0x00` 即视为 padding 结束。
5. Series ID：v2/v3 为 `offset / 16`；v1 为 `offset`。Tombstones 使用同一 ID。
6. v1 无对齐、symbol 引用为绝对偏移；当前实现兼容，但测试 fixture 覆盖 v2。

### 2.4 与官方文档的差异

`docs/format/index.md` 对 chunk meta 的文字描述与源码一致；但对 `TOC.series` 的对齐语义未强调。以源码 `Writer.AddSeries`（写前 `addPadding(16)`）与 `Reader.Series`（`id*16`）为准。

---

## 3. chunks 段文件

```
magic 0x85BD40DD (4B BE) | version 1 (1B) | padding (3B) | chunk records...
```

Chunk reference（uint64）：

```
高 32 位：segment index（按文件名数字排序后的**下标，0 基**）
低 32 位：chunk 在文件内的字节偏移
```

> `Writer.writeChunks` 使用 `w.seq() = len(files)-1`，所以第一个文件 `000001` 的 ref 高 32 位是 **0**；读取端按 `sequenceFiles()` 排序后的下标索引即可。

Chunk record（`chunks.go: Reader.ChunkOrIterable`）：

```
len(uvarint) | encoding(1B) | data(len bytes) | CRC32C(4B BE)
```

关键点：

1. **`len` 只统计数据字节，不含 encoding 字节**。官方 `chunks.md` 写的是“Chunk size in bytes”，容易误解为包含 encoding；源码是 `chkEnd = encStart + 1 + chkDataLen + 4`。
2. CRC 覆盖 `encoding + data`。
3. 解码器拿到的是 `data`（已去掉 encoding）。
4. 每个 segment 最大 512MiB；读取时 mmap，`madvise(SEQUENTIAL)`。

---

## 4. Chunk 编码

`chunkenc.Encoding`：

| 值 | 名称 | 说明 |
|----|------|------|
| 0 | none | 空 chunk |
| 1 | XOR | float 样本 |
| 2 | histogram | 整数直方图 |
| 3 | floathistogram | 浮点直方图 |
| 4 | XOR2 | 联合控制位 + 可选 ST 的 float 样本 |
| 5 | histogramST | 直方图 + start timestamp |
| 6 | floathistogramST | 浮点直方图 + ST |

### 4.1 位流

所有编码共用 Prometheus 的 `bstream` 语义：

- 位按 **MSB-first** 写入/读出。
- `read_bits(n)` 返回接下来 n 位，作为 u64 的低位。
- 内部缓冲一次装入 8 字节大端；跨缓冲读取时先消费剩余位，再装下一段。
- `<varint>` 是 Go 的 zig-zag varint；`<uvarint>` 是 LEB128。
- `<varbit_int>` / `<varbit_uint>` 是 Prometheus 自定义的桶编码（`varbit.go`），前缀长度 1~8 位，值位宽 3/6/9/12/18/25/56/64。符号判断用 `bits > (1 << (sz-1))`（**严格大于**），与 Go 实现保持一致。

### 4.2 XOR（float）

```
num_samples(uint16 BE) | ts0(varint) | v0(float64 bits)
  | (ts1-ts0)(uvarint) | v1_xor(varbit_xor)
  | (dod_2)(varbit_ts) | v2_xor ...
```

- `v_xor` 控制位：`0` 值不变；`10` 复用上一条 leading/trailing；`11` + leading(5b) + sigbits(6b) + 值位。
- `sigbits == 0` 表示 64。
- dod 前缀：`0`(0)、`10`(14b)、`110`(17b)、`1110`(20b)、`1111`(64b)。
- 负 dod 通过无符号回绕后按位解释；实现使用 `wrapping_add`。

### 4.3 XOR2（float）

```
num_samples(uint16) | st_header(1B) | ts0(varint) | v0(64b) | ?st0(varint)
  | ts1_delta(uvarint) | v1(varbit_xor2) | ?st1(varbit_ts)
  | joint_sample_2 | ?st_2_dod | ...
```

样本 ≥2 的联合控制前缀：

| 前缀 | dod | 后续值编码 |
|------|-----|-----------|
| `0` | 0 | 无（值不变） |
| `10` | 0 | `varbit_xor2_nn` |
| `110` + 13b | 13 位有符号 | `varbit_xor2` |
| `1110` + 20b | 20 位有符号 | `varbit_xor2` |
| `11110` + 64b | 精确值 | `varbit_xor2` |
| `11111` | 0 | 无（stale NaN） |

- `varbit_xor2`：`0` 不变；`10` 复用窗口；`110` + leading(5)+sigbits(6)+值；`111` stale NaN。
- `varbit_xor2_nn`：`0` 复用窗口；`1` + leading+sigbits+值。
- `baselineV` 保存最后一个**非 stale** 值，`0` 前缀复制它，而不是上一个样本值。
- ST 头：bit7 = `firstSTKnown`，低 7 位 = `firstSTChangeOn`；ST 数据追加在样本之后，语义见 `st.go`。

### 4.4 Histogram（整数）

```
num_samples(uint16 BE) | flags(1B) | zero_threshold | schema(varbit_int)
  | pos_spans | neg_spans | ?custom_values | samples...
```

- `zero_threshold`：0 → 单字节 0；2^-243..2^10 的 2 的幂 → 1..254；否则 255 + float64。
- spans：`num(varbit_uint)`，每个 span 为 `length(varbit_uint), offset(varbit_int)`。
- schema ∈ [-4,8] 或 -53（自定义桶）；-53 时追加 `custom_values`（varbit_uint 编码：0 表示后跟 float64，否则 `(v-1)/1000`）。
- 样本 0：`ts(varbit_int) | count(varbit_uint) | zero_count(varbit_uint) | sum(64b) | 桶 delta(varbit_int)*`
- 样本 ≥1：`ts_dod | count_dod | zero_count_dod | sum_xor | 桶 dod*`
- 桶是**二阶差分**：`bucketDelta += dod; bucketValue += bucketDelta; absolute = cumulative(bucketValue)`。只做一阶累加会得到错误的桶值（我们在对拍中修正过）。
- `sum` 为 stale NaN 时，该样本**没有桶数据**；迭代器返回仅含 `Sum` 的直方图。迁移输出同样只保留 stale 标记，count/buckets 为 0。

### 4.5 FloatHistogram

- 结构同整数直方图，但 `count / zero_count / sum / 每个桶` 都是 64 位 float：
  - 样本 0：直接写 float64。
  - 样本 ≥1：`xorRead` 逐字段解码。
- 桶值是**绝对值**，不做差分。
- stale 判断同整数直方图。

### 4.6 Histogram ST / FloatHistogram ST

- header 仍是 3 字节，但布局不同：
  - byte0 高 2 位 = counter reset flags，低 6 位 + byte1 = **14 位样本数**；
  - byte2 = ST header（bit7 firstSTKnown，低 7 位 firstSTChangeOn）。
- 每个样本的直方图数据之后追加可选 ST：
  - `num_read == 1` 且 firstSTKnown：`st = cur_t - varint`
  - `num_read == 2` 且 change_on == 1：`st = prev_t - varbit_int`
  - `num_read > 2` 且 `num_read-1 >= change_on`：累加 `stDiff`，`st = prev_t - stDiff`
- 我们的输出在 histograms 表里保留 `st` 列。

### 4.7 counter reset 标记

Header 高 2 位的原始值写入 `reset_hint`：

| 值 | 含义 |
|----|------|
| 0 | unknown |
| 1 | not counter reset |
| 2 | counter reset |
| 3 | gauge |

没有做 Prometheus 查询期的 reset 语义推导；迁移只保留原始信息。

---

## 5. Tombstones

实际磁盘布局（`tombstones/tombstones.go`）：

```
magic 0x0130BA30 (4B BE) | flag/version (1B, 必须为 1)
  | (series_ref uvarint, mint varint, maxt varint)*
  | CRC32C (4B BE)
```

关键点：

1. **没有单独的第二字节版本号**：magic 后的第一个字节就是 `Encode()` 写出的格式标记（1）。官方 `tombstones.md` 描述的“最后 8 字节指向 stones 起始”在当前实现中并不存在。
2. CRC 覆盖 **stones 区段**（magic 与 flag 之后，CRC 之前），即 `file[5 .. len-4]`。`WriteFile` 与 `ReadTombstones` 都是这个范围。
3. 区间是**闭区间** `[mint, maxt]`（`Interval.InBounds` 使用 `<=`），与 Block 的半开区间不同。
4. 删除区间可能相邻；读取后按 `mint` 排序并合并相邻/重叠区间，减少查询开销。
5. 缺失 `tombstones` 文件视为无删除。

---

## 6. 时间与过滤语义

| 对象 | 语义 |
|------|------|
| Block `[minTime, maxTime)` | 半开 |
| Chunk meta `[min_time, max_time]` | 闭区间（索引记录的首/末样本时间） |
| Tombstone `[mint, maxt]` | 闭区间 |
| 迁移 `--min-time/--max-time` | 闭区间，按样本时间戳过滤 |
| 块级时间预筛 | `block.maxTime > min && block.minTime <= max` |

过滤顺序：series label matcher → chunk 时间预筛 → 样本时间过滤 → tombstones 过滤。被 tombstone 删除的样本不写出，计入 `samples_skipped_tombstone`。

---

## 7. 版本兼容矩阵

| 组件 | 支持 | 说明 |
|------|------|------|
| index v1 | 读取 | 无 16B 对齐；symbol 引用为绝对偏移；fixture 未覆盖 |
| index v2 | 读取 + 对拍 | Prometheus 2.x/3.x 主流格式 |
| index v3 | 读取 | series 布局与 v2 相同；仅 postings 编码变化，本工具不解析 postings |
| chunk encoding 0–6 | 读取 + 对拍 | 6 种编码均有 fixture 逐样本对拍 |
| tombstones v1 | 读取 + 对拍 | 真实 Block fixture 带删除区间 |
| meta v1 | 读取 | 未知字段忽略 |
| Head / WAL / chunks_head | 不支持 | 需先让 Prometheus flush/compact 成 Block |
| 远程读 / 对象存储 | 不支持 | 只读本地 Block 目录 |

---

## 8. 实现自检清单（对拍项）

- [x] 真实 Block：42 series、100 chunks、4951 samples（含 2 条直方图 series）逐样本一致。
- [x] 浮点 NaN / StaleNaN 按 bit pattern 比较。
- [x] 直方图 count / zero_count / sum / schema / zero_threshold / spans / 绝对桶 / ST 全部一致。
- [x] 6 种编码 fixture 解码一致。
- [x] Tombstones 区间与样本过滤数量一致（41 条）。
- [x] 损坏 CRC 返回 `Checksum` 错误（单元测试覆盖）。
- [x] 16 字节对齐、chunk `len` 不含 encoding、tombstone CRC 范围等坑位有注释与测试。
