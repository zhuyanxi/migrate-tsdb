# 架构设计 — tsdb-migrate

> 目标：把 Prometheus TSDB Block 中的海量历史样本以**有界内存、可中断、可恢复**的方式迁移到 Remote Write v1.0 接收端；同时提供 CLI、HTTP API 与 Web 控制台三个交付形态。
>
> 本文档只描述设计与边界，具体格式细节见 `02-format-notes.md`，接口契约见 `03-api.md`，实施顺序与验收见 `01-roadmap.md`。

---

## 1. 目标与非目标

### 1.1 目标

| # | 目标 | 可验证指标 |
|---|------|-----------|
| G1 | 顺序读取 Block 全量 Series/Chunk/Sample，不依赖 Go runtime | 与 Prometheus 官方 reader 对拍，样本逐条一致 |
| G2 | 迁移过程内存有界，不随 Block 总量增长 | 峰值 RSS 由 `workers × (batch + request buffer)` 决定；100GB 级数据不 OOM |
| G3 | 迁移速度快 | 纯解码吞吐 ≥ 100 MB/s/线程（压缩 chunk 计），多块并行近线性扩展 |
| G4 | 可暂停 / 恢复 / 停止 | 状态机 + 单元级 checkpoint；接收端与本地 checkpoint 非事务，恢复可能重放当前单元 |
| G5 | 数据正确 | CRC32C 校验、Tombstones 过滤、时间范围过滤、NaN/StaleNaN 原样保留 |
| G6 | 可运维 | HTTP API + Web 控制台，展示速率、ETA、分块明细、错误与日志；服务重启后任务可恢复 |
| G7 | 可扩展 | Sink 抽象，新增存储后端不改 Reader/Engine；Reader 不依赖迁移逻辑 |

### 1.2 非目标（明确不做）

- 不实现 PromQL、Postings 倒排查询、Compaction、规则/告警。
- 不读取 Head / WAL / `chunks_head`（只处理已落盘的 Block 目录；文档说明如何先 flush/compact）。
- Mimir 只做**块存储导出**：不访问 querier / ingester / compactor API，不读取 Mimir 的 bucket index、删除标记之外的控制面数据。
- 不做去重（同一 series 跨多个 Block 的重叠由上层按 `block` 分区与时间过滤处理；重叠块会在 inspect 中告警）。
- 不追求字节级“断点续传”：checkpoint 粒度为迁移单元（默认 5 万 series / 数百万样本），恢复会重做当前未提交单元。

---

## 2. 总体架构

```
┌────────────────────────────────────────────────────────────────────────────┐
│                                Web 控制台 (静态)                            │
│  任务列表 · 创建任务 · 启动/暂停/恢复/停止 · 总进度/分块进度 · 速率/ETA · 日志  │
└───────────────────────────────┬────────────────────────────────────────────┘
                                │ HTTP JSON / 轮询
┌───────────────────────────────▼────────────────────────────────────────────┐
│                        tsdb-migrate-server (Rust/axum)                     │
│  Job Manager（状态机、控制信号、状态持久化、日志环形缓冲）                      │
│  REST API（jobs / actions / progress / blocks / logs / fs scan）            │
│  静态资源（内嵌 index.html/app.js/styles.css）· 可选 Bearer Token 鉴权        │
└───────────────────────────────┬────────────────────────────────────────────┘
                                │ 进程内调用（同一套 engine）
┌───────────────────────────────▼────────────────────────────────────────────┐
│                         migrate-core（编排层，无 IO 格式知识）                │
│  JobConfig · Planner · Engine · Control(启动/暂停/恢复/停止) · Stats         │
│  Checkpoint（unit 级提交）· Remote Write sink · Manifest                    │
└───────────────┬───────────────────────────────────┬────────────────────────┘
                │ 顺序遍历 Series/Chunk             │ 批量发送 HTTP 请求
┌───────────────▼──────────────────┐   ┌────────────▼───────────────────────┐
│ tsdb-reader（纯格式层，无状态机）   │   │ Remote Write v1.0 receiver         │
│ Block 发现 · meta.json            │   │ Prometheus / Mimir / compatible    │
│ Index v1/v2/v3 · Symbols · Series │   └────────────────────────────────────┘
│ Chunk Segments · CRC32C           │
│ XOR/XOR2/Histogram/FloatHist/ST   │
│ Tombstones                        │
└───────────────┬──────────────────┘
                │ mmap（只读）
┌───────────────▼────────────────────────────────────────────────────────────┐
│                      Prometheus TSDB Block 目录                             │
│  <data-dir>/<ULID>/{meta.json,index,tombstones,chunks/000001...}           │
└────────────────────────────────────────────────────────────────────────────┘
                ▲
                │ 本地 Block / 已下载缓存
┌───────────────┴────────────────────────────────────────────────────────────┐
│            Grafana Mimir 对象存储（S3 / MinIO / 本地模拟）                   │
│  <prefix>/<tenant>/<ULID>/{meta.json,index,tombstones,chunks/...}          │
│  tenant 发现 · 对象清单分组 · 按块流式下载 · 本地 cache                     │
└────────────────────────────────────────────────────────────────────────────┘
```

### 2.1 为什么是三层而不是一个单体

- **格式层（tsdb-reader）**只回答“这个字节是什么”，不做批处理、不做并发、不做状态。它可以被单独用于校验工具或其他 TSDB 分析工具。
- **编排层（migrate-core）**只回答“按什么顺序读、什么时候 flush、断点在哪、写到哪里”。它通过 `Sink` 与存储解耦，通过 `SeriesSource` 与格式解耦（当前唯一实现是 BlockReader）。
- **接入层（CLI / Server / Web）**只回答“如何被调用、如何展示”。CLI 与 Server 共享同一个 `Engine`，不存在两套迁移逻辑，避免行为漂移。

---

## 3. 组件职责边界

### 3.0 数据源层（migrate-core::mimir）

| 模块 | 职责 | 明确不负责 |
|------|------|-----------|
| `MimirConfig` | bucket / prefix / region / endpoint / tenant 过滤 / cache 配置 | 不保存密钥；凭证走 AWS 环境变量 |
| tenant 发现 | 对 `<prefix>/` 做 delimiter listing，得到 tenant 目录 | 不解析 bucket index |
| 对象清单 | 流式遍历 `<prefix>/<tenant>/`，按 `ULID` 分组，累计 index/chunks 大小 | 不把全部对象键常驻内存 |
| block 下载 | 按块并发流式下载到 `<cache>/<tenant>/<ULID>`，`.ready` 标记 | 不做 range 级远程读取 |
| 生命周期 | 完成后清理缓存（可配置保留） | 不删除源对象 |

### 3.1 tsdb-reader

| 模块 | 职责 | 明确不负责 |
|------|------|-----------|
| `block` | 扫描 data-dir，识别合法 Block，读取 `meta.json`，按 `minTime` 排序，报告重叠 | 不判断业务上是否该迁 |
| `index` | 解析 TOC / Symbol Table / Series 区段；按 16 字节对齐顺序遍历；返回 labels 与 chunk meta | 不解析 postings（顺序全量导出不需要） |
| `chunks` | mmap 各 segment，校验 magic/version，按 `ChunkRef` 定位，校验 CRC32C | 不缓存 chunk、不做跨 chunk 合并 |
| `chunkenc` | XOR / XOR2 / Histogram / FloatHistogram / ST 解码，tombstone 过滤回调 | 不做降采样、不做 counter reset 修正 |
| `tombstones` | 解析并合并删除区间，提供 `is_deleted(series_ref, ts)` | 不修改块、不物理删除 |
| `symbols` | 符号表按 32 个一组的偏移索引 + 小型直接映射缓存，避免把全部字符串常驻堆 | 不构建全局 label 索引 |

**关键约束**：`BlockReader` 可以 `Send`，但一个实例只被一个 reader 线程驱动（`Symbols` 的内部缓存非线程安全）；需要并行时由编排层按 Block / 按 Series 分片，每片独立 reader 或独立遍历。

### 3.2 migrate-core

| 模块 | 职责 |
|------|------|
| `Planner` | 将 data-dir 展开为 Block 计划；应用 block 白名单、时间范围、label matcher；读取 meta.stats 或预扫描 index 得到 series/chunks 期望值 |
| `Engine` | 按 `block_parallel` 并发处理 Block；每个 Block 内部由 1 个 reader + N 个 decode worker + 1 个 coordinator 组成流水线；控制暂停/停止；聚合统计 |
| `Control` | 状态机 `Created→Running→Pausing→Paused→Running→…→Completed/Stopped/Failed`，用原子标志 + Condvar 实现协作式中断 |
| `Batch` | 定长样本缓冲（默认 65536 样本 / 32MB 上限），满则 flush；Float 与 Histogram 分列构建 |
| `Checkpoint` | 单元级提交：完成单元 U 的远端请求后，原子写入 `completed_unit` 与 `next_offset` |
| `Sink` | `write / finish_unit / end_block`；唯一实现为 Remote Write v1.0 |
| `Manifest` | 全局与分块计数、过滤结果、耗时、错误；用于本地状态恢复与任务展示 |

### 3.3 tsdb-migrate-server

- **JobManager**：内存 `HashMap<JobId, Arc<JobHandle>>` + 磁盘 `state/jobs/<id>.json`；进程重启后把 `Running/Pausing/Stopping` 恢复为 `Paused(interrupted)`。
- **JobHandle**：`Control`、`Stats`、`BlockProgress[]`、日志环形缓冲（默认 2000 行）。API 只读快照，不与引擎争锁。
- **路径安全**：所有 `data_dir` / `output_dir` 必须位于 `--allow-root`（可多次指定，默认当前工作目录）之下，canonicalize 后校验，防止任意文件读写。
- **鉴权**：设置 `TSDB_MIGRATE_TOKEN` 或 `--token` 后，除 `/api/health` 外全部要求 `Authorization: Bearer`。
- **静态资源**：`include_str!` 内嵌，单二进制部署；不需要 Node/npm 构建。

### 3.4 Web 控制台

- 纯静态（HTML + CSS + 原生 ES2020），无构建步骤、无 CDN 依赖，可在内网离线使用。
- 只通过 HTTP API 工作；Token 存 `localStorage`。
- 展示：任务状态、series/chunk 双进度条、样本速率、ETA、当前块、分块明细表、错误计数、日志尾部。
- 交互：创建任务（扫描 Block → 勾选 → 参数）、启动、暂停、恢复、停止；按钮可用性由状态机驱动。

---

## 4. 关键数据流

### 4.1 迁移主流程

```
scan blocks（本地目录 / Mimir tenant 发现）
   → plan (series_total / chunks_total / samples_expected)
   → [Mimir] 按块下载到本地 cache（流式、有界并发、.ready 标记）
   → for each block (block_parallel):
        open block (mmap index + segments, load tombstones)
        reader thread: series_iter(start_offset)
             → resolve labels (symbol cache)
             → send SeriesJob{unit, offset, labels, chunks} to bounded channel
        decode workers (N):
             → for each chunk: read segment bytes, CRC check, decode samples
             → filter: time range, tombstones
             → append to Batch (float/hist columns)
             → Batch full → encode protobuf + Snappy → HTTP POST
        coordinator:
             → unit 完成（所有 worker 的请求均返回）
             → Sink.finish_unit
             → 原子写 checkpoint（completed_unit, next_offset）
             → 更新全局/分块统计
        all units done → 写 block 完成标记与 manifest entry
   → write manifest.json
```

### 4.2 并行模型

两层并行，互不干扰：

1. **Block 级并行**：Remote Write 将 `block_parallel` 固定为 1，避免同一 tenant 的样本乱序。
2. **Block 内解码并行**（`decode_workers`，默认 4）：index 顺序遍历必须单线程（保证 offset 连续、checkpoint 语义简单），chunk 解码按 series 分发给 worker 池。

内存与背压：

- series 通道容量 `channel_capacity`（默认 2048）→ reader 不会无限领先。
- 每个 worker 的 Batch 上限 `batch_samples`（默认 65536）/ `batch_bytes`（默认 32 MiB）。
- 单元屏障（`unit_series` 默认 50000）限制在途数据与 checkpoint 粒度。
- 默认总解码线程 = `block_parallel × decode_workers`，建议 ≤ 物理核数 − 2。

### 4.3 暂停 / 恢复 / 停止的语义

| 动作 | 行为 | 最坏延迟 |
|------|------|---------|
| Pause | 停止派发新 unit；在途 unit 完成、提交 checkpoint 后进入 `Paused` | 一个 unit 的解码时间（秒级） |
| Resume | 从 `next_offset` 继续；重放尚未 checkpoint 的单元 | 立即 |
| Stop | 同 Pause，但完成后进入 `Stopped`，不再自动继续 | 一个 unit |
| 进程崩溃 | 从最后已提交 checkpoint 恢复；当前单元可能已部分或全部到达接收端 | 恢复时重放当前单元 |
| 服务重启 | 任务状态落盘；`Running` → `Paused(interrupted)`，可手动 resume | 立即 |

**重放边界**：Remote Write 与本地 checkpoint 不能组成原子事务。若请求成功后进程在 checkpoint 落盘前退出，恢复时会重发当前单元；接收端通常按相同 series 与时间戳幂等处理，但不承诺 exactly-once。

---

## 5. 内存模型（为什么不会 OOM）

| 组成 | 量级 | 说明 |
|------|------|------|
| index mmap | 虚拟地址 = 文件大小；RSS = 实际触碰页 | 顺序扫描，内核可回收；不读 postings |
| chunk segment mmap | 同上 | 逐 chunk 触碰，`madvise(SEQUENTIAL)` |
| Symbol offsets | `symbols/32 × 8B` | 1000 万符号 ≈ 2.5 MB |
| Symbol 缓存 | 固定 16K 槽 × 短字符串 | 数 MB，命中率高（series 按 label 排序） |
| 当前 Series labels + chunk metas | 单条 series，KB 级 | 不保留历史 series |
| series 通道 | `capacity × 单条 series` | 默认 2048 × ~1KB ≈ 2MB |
| 每 worker Batch | ≤ 32 MiB 或 64K 样本 | 与 worker 数线性 |
| Remote Write 请求 | 每 worker 当前 protobuf / Snappy 请求缓冲 | 与 batch 同量级 |
| Tombstones | 每块删除区间数 | 通常 KB~MB |
| Mimir 下载缓存 | 每并发块一个 block 目录（磁盘） | 默认完成后删除；`keep_cache` 可保留 |

对比 Go 方案：没有 GC 扫描大堆、没有 `labels.Labels` 对象图、没有 `SeriesSet` 全量物化。mmap 的常驻页由内核管理，可通过 `madvise(DONTNEED)` 在块结束时主动释放。

---

## 6. Remote Write 与本地状态

样本以 Remote Write v1.0 的 protobuf 格式编码，并用 Snappy block 压缩后发送。Mimir tenant ID 默认通过 `X-Scope-OrgID` 转发；本地 Prometheus 源不设置 tenant header。

`<out>/` 只保存 `manifest.json`、`.state/` checkpoint、`blocks/.../_complete.json` 完成标记，以及可选的 Mimir 下载缓存。样本数据不在本地持久化。

## 7. 正确性保障

1. **格式层**：所有 CRC（index series、TOC、chunk、tombstones）默认校验，可 `--no-verify-crc` 关闭以提速（会记录警告）。
2. **对拍**：用 Prometheus 官方 Go 实现生成真实 Block 与 6 种编码的 chunk fixture，导出 ground truth；Rust 测试逐样本比对（含时间戳、值、NaN 位模式、bucket 绝对值、ST）。
3. **Tombstones**：按 series ref + 时间区间过滤，统计 `samples_skipped_tombstone`；inspect 报告每块 tombstone 数量。
4. **计数记录**：`manifest.json` 记录每块与任务级样本计数；接收端写入结果需在目标系统侧校验。
5. **恢复**：已完成块默认跳过；`--force` 会重新发送，接收端对相同 series 与时间戳应保持幂等。
6. **可观测**：每个块记录 `chunks_total/done`、`crc_errors`、`decode_errors`、`unknown_encoding`，任何一条样本解码失败默认使该块失败（可通过 `--skip-bad-chunks` 降级为记录错误继续）。

---

## 8. 性能设计要点

- **零拷贝**：`memmap2` 映射 index 与 chunk segment，解码直接在 `&[u8]` 上进行，不复制 chunk 到堆。
- **按需 symbols**：只建立每 32 个符号的偏移表，避免数百 MB 字符串常驻。
- **批量发送**：Batch 转为 protobuf 并 Snappy 压缩；请求大小受 `batch_samples` / `batch_bytes` 控制。
- **CRC 可并行**：每个 chunk 独立校验；使用硬件 CRC32C（SSE4.2/ARMv8）。
- **顺序 IO**：series 顺序遍历与 chunk 物理布局一致（同一 series 的 chunk 连续），`madvise(SEQUENTIAL)` 提升预读。
- **避免重复解码**：单遍扫描直接构造 Remote Write time series；规划统计来自 meta 或仅扫 index 结构。
- **背压**：有界通道 + 有界 batch + 单元屏障，RSS 可预测。

---

## 9. 技术选型

| 关注点 | 选择 | 理由 |
|--------|------|------|
| 语言 | Rust 2021 | 无 GC、可预测内存、mmap 友好、并发安全 |
| mmap | `memmap2` | 成熟、跨平台、支持 `advise` |
| 对象存储 | `object_store` (aws feature) | S3/MinIO 兼容、流式 GET、分页 LIST、无需捆绑 libaws |
| CRC32C | `crc32c` | 硬件加速，Castagnoli 多项式 |
| 序列化 | `serde` / `serde_json` | meta.json、状态、manifest |
| Remote Write | `prost` + `snap` + `reqwest` | 编码 Prometheus Remote Write v1.0 请求并发送 |
| HTTP | `axum` + `tokio` | 轻量、类型安全、SSE 友好 |
| CLI | `clap` | 标准、帮助信息完善 |
| 哈希 | `xxhash-rust` (xxh3) | 稳定的 series_uid，速度快 |
| 日志 | `tracing` + `tracing-subscriber` | 结构化日志，可与 axum 集成 |

不引入 `prometheus` Go 绑定（跨语言 FFI 会重新引入 Go runtime 与 GC）。

---

## 10. 部署形态

```
# CLI（本地/一次性迁移）
tsdb-migrate migrate --data-dir /prometheus/data --out /var/lib/tsdb-migrate/job \
     --remote-write-url https://receiver.example/api/v1/write --block-parallel 1 --decode-workers 4

# Server（常驻，Web 操作）
TSDB_MIGRATE_TOKEN=xxx tsdb-migrate-server --bind 0.0.0.0:8080 --state-dir /var/lib/tsdb-migrate --allow-root /prometheus --allow-root /var/lib/tsdb-migrate
# 浏览器打开 http://<host>:8080/
```

- 单二进制，静态资源内嵌；无 Node、无 Python 运行时依赖。
- 只读挂载 Prometheus data 目录即可；输出目录需可写。
- 建议容器内存 limit ≥ `2GB + block_parallel × decode_workers × 64MB`，并留出 page cache 余量。
