# 目的

我要将Prometheus tsdb的数据迁出, 并存入其他存储, 比如duckdb。因为我的数据块特别多且大，用tsdb标准库Go的话，由于有gc的机制，所以我迁移数据的过程需要大量的memory资源

# 调研

有一个可以作为起点的 Rust 项目：rusty_chunkenc。

不过，我查了一下目前的 Rust 生态，需要先说明一个重要区别：

Rust 已经有 Prometheus Chunk 解码实现，但我没有找到一个成熟、完整、可以直接打开 Prometheus TSDB Block 并遍历所有 Series、Labels、Chunks、Tombstones 的 Rust 库。

对于该场景，我考虑基于 Rust 自己实现一个轻量级 Block Reader。这项工作是有现实可行性的，而且可以很好地控制内存使用。

## 1. 目前值得关注的 Rust 库

rusty_chunkenc

最值得关注

Rust 实现的 Prometheus chunkenc

版本：0.1.2

它是我目前找到的最接近你需求的项目。

已实现的功能：

- 解析 Prometheus XOR 编码的 Chunks。
- 编码和解码时间戳、Float64 样本。
- 解析 Prometheus Chunk Disk Format。
- 提供 varint、uvarint、varbit 等底层解码能力。

但存在明显限制：

- `index` 模块仍标记为 WIP。
- `folder` 模块仍标记为 WIP。
- Histogram / FloatHistogram 尚未实现。
- 不能直接作为完整的 TSDB Block Reader 使用。

来源：[Docs.rs](https://docs.rs/rusty-chunkenc/latest/rusty_chunkenc) · [GitHub](https://github.com/sintef/rusty-chunkenc)

其他相关项目

rusty-promql-parser

非数据读取器

PromQL Parser 的 Rust 移植，与 TSDB 文件解析无关。

[GitHub](https://github.com/sintef/rusty-promql-parser)

我的建议是把 `rusty_chunkenc` 当作底层 Chunk Decoder 的参考或部分依赖，而不是直接依赖它完成整个迁移。

## 2. Prometheus TSDB Block 实际上并不复杂到无法自己解析

一个标准的 Prometheus Block 目录结构如下：

```
01JXXXXXXXXXXXXXXX/
├── meta.json
├── index
├── tombstones
└── chunks/
    ├── 000001
    ├── 000002
    ├── 000003
    └── ...
```

Prometheus 官方有完整的格式文档：

- [TSDB Format Documentation](https://github.com/prometheus/prometheus/tree/main/tsdb/docs/format)
- [Index Format](https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md)
- [Chunks Format](https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/chunks.md)

整个读取过程可以分成三个部分。

### 2.1 Index Reader

`index` 文件包含：

- Symbol Table：Label Key、Label Value 的字符串字典。
- Series：每条 Time Series 的 Labels 和 Chunk Metadata。
- Label Index：Label 的索引。
- Postings：用于根据 Label 筛选 Series。
- TOC：各个索引区段的偏移信息。

其中最重要的是 Series 区段。

每条 Series 记录包含：

```
Series
├── Number of Labels
├── Label Name Reference
├── Label Value Reference
├── Number of Chunks
└── Chunk Metadata[]
    ├── min_time
    ├── max_time
    └── chunk_ref
```

Series 的标签和 Chunk 元数据已经存在于 Index 中，因此读取全部数据时，并不需要构建一套完整的 Label 查询引擎。

可以直接顺序遍历 Series，解析出：

1. Series Labels。
2. Chunk 的时间范围。
3. Chunk 的物理存储位置。

这也是我认为最值得用 Rust 自己实现的部分。

官方格式说明：https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md



### 2.2 Chunk Reader

这是实际读取样本数据的部分。

Prometheus Chunk Segment 的结构：

```
Chunk Segment
│
├── Magic Number
├── Version
├── Padding
│
├── Chunk 1
│   ├── Length (uvarint)
│   ├── Encoding
│   ├── Data
│   └── CRC32C
│
├── Chunk 2
│
└── Chunk N
```

其中 Chunk Reference 是一个 uint64：

```
┌────────────────────────┬────────────────────────┐
│ Segment Sequence (32)  │ Offset (32)            │
└────────────────────────┴────────────────────────┘
```

所以可以直接计算：

```
let segment_id = (chunk_ref >> 32) as u32;
let offset = (chunk_ref & 0xFFFF_FFFF) as u32;
```

随后：

1. 根据 `segment_id` 找到对应文件。
2. 根据 `offset` 定位 Chunk。
3. 读取 Chunk Length。
4. 读取 Encoding。
5. 校验 CRC32C。
6. 解码样本。

注意，当前 Prometheus 不只有传统 XOR 编码，还包括 XOR2、Histogram、FloatHistogram 等格式。如果需要兼容不同版本，不能只实现 XOR Decoder。
https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/chunks.md



### 2.3 Tombstones Reader

这里也不能遗漏。

Prometheus 删除 Series 时，并不会立即从原始 Chunk Segment 中删除样本，而是通过 Tombstones 记录时间范围删除信息。

如果直接读取 Chunk 文件而忽略 Tombstones，迁移出来的数据可能包含已经删除的样本。

因此，需要将 Tombstones 纳入读取流程。

## 3. 我建议的 Rust 实现架构

我的第一目标是迁移到 DuckDB, 以后也可能会迁移到其他thirdparty的存储中，我不暂时不希望做一个完整的 Prometheus TSDB 实现，而是做一个专门用于历史数据导出的 Streaming Block Reader。

## Rust TSDB Migration Engine

Prometheus TSDB Blocks

meta.json · index · chunks · tombstones


Block Manager

Block 生命周期、版本、时间范围


Mmap Manager

映射 Index 和 Chunk Segment


Index Reader

Symbol、Series、Chunk References


Chunk Decoder

XOR、XOR2、Histogram


Streaming RecordBatch Builder

固定容量缓冲区，达到阈值立即 Flush


DuckDB Writer

批量写入 · 分区 · 压缩 · Parquet



建议使用以下 Rust 组件：

| 模块             | 推荐库               | 用途                  |
| -------------- | ----------------- | ------------------- |
| Memory Mapping | `memmap2`         | 映射 Index 和 Chunk 文件 |
| Binary Parsing | `nom` 或手写 Decoder | 解析二进制格式             |
| Checksum       | `crc32c`          | CRC32C 校验           |
| Serialization  | `serde`           | 读取 meta.json        |
| Arrow          | `arrow`           | 构造 RecordBatch      |
| DuckDB         | `duckdb`          | 写入 DuckDB           |
| Error          | `thiserror`       | 错误处理                |

这里有一个关键设计：不要把整个 Block 解码成内存中的 Series 集合。

应该按照：

```
Open Block
    ↓
Mmap Index
    ↓
Mmap Chunk Segments
    ↓
Read One Series
    ↓
Read Its Chunk References
    ↓
Decode One Chunk
    ↓
Append Samples to Batch
    ↓
Batch Full?
    ├── Yes → Flush to DuckDB
    └── No  → Continue
    ↓
Next Series
    ↓
Close Block
```

比如每 64K 或 128K 条样本构造一个 Batch，然后写入 DuckDB。

这样内存主要由以下部分决定：

- 当前映射文件的驻留页面。
- 当前 Series 的 Label 和 Chunk Metadata。
- 当前 Chunk Decoder。
- 当前 RecordBatch。
- DuckDB 自身的执行和写入缓冲区。

不需要把几百 GB 的历史数据加载到内存。

## 4. 一个值得注意的问题：Go GC 可能并不是唯一的内存问题

这里有一个容易忽略的事实。

Prometheus 官方 TSDB 本身已经大量使用 Memory Mapping。

例如，Go TSDB 的 `chunks.NewDirReader()` 会使用 `OpenMmapFile()` 映射 Chunk Segment 文件，而不是把全部 Chunk 文件复制到 Go Heap 中。

相关源码：

- [chunks.go](https://github.com/prometheus/prometheus/blob/main/tsdb/chunks/chunks.go)
- [mmap.go](https://github.com/prometheus/prometheus/blob/main/tsdb/fileutil/mmap.go)

这意味着需要区分两种内存：

| 内存类型                          | 是否受 Go GC 管理 |
| ----------------------------- | ------------ |
| Go Heap 上的 Series、Labels、临时对象 | 是            |
| mmap 映射的 Chunk 文件内容           | 否            |
| mmap 文件对应的驻留页面                | 否            |
| DuckDB Buffer Pool            | 否            |

尤其需要注意：mmap 并不意味着没有物理内存消耗。随着程序读取文件，文件页面会进入进程 RSS，也可能被容器的 cgroup 内存统计计入。

因此，我建议先观察：

- `go_memstats_heap_alloc_bytes`
- `go_memstats_heap_inuse_bytes`
- `process_resident_memory_bytes`
- 容器 `memory.current`

这样可以判断你遇到的究竟是 Go Heap 增长，还是 mmap 文件页面和其他 Native Memory 的占用。

如果 Go Heap 才是主要问题，那么优化 Go 版本的迭代方式也可能解决问题。例如逐个打开 Block、逐条遍历 Series、限制 Batch 大小、及时关闭 mmap。

## 5. 我的最终想法

我要将大量 Prometheus TSDB 数据迁移到 DuckDB 的目标或其他thirdparty存储，我的方案: Rust 完整自研 Block Reader.

原因是我的目标并不是开发一个 Prometheus 的替代品，而是把 TSDB 当成一种历史数据源。实际上没有必要实现 PromQL、Postings 查询引擎、Compaction 等完整功能。

你真正需要的只有：

1. 能打开 Block。
2. 能顺序遍历全部 Series。
3. 能根据 Chunk Reference 定位物理文件。
4. 能解码不同类型的 Chunk。
5. 能过滤 Tombstones。
6. 能以有界内存持续输出 Arrow RecordBatch。

这样就能形成一个专门的 Rust 数据迁移引擎。

我还有一个架构上的思考：不需要将 Rust Block Reader 与 DuckDB Writer 紧密耦合。

让 Block Reader 输出 Arrow RecordBatch，DuckDB 只负责消费 Batch。以后你还可以把同一套 TSDB Reader 用于写 Parquet、DataFusion，或者其他分析引擎。
