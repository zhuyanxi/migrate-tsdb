# 分阶段实现方案与验收标准

> 原则：每个阶段都必须**可运行、可验证**，后一阶段不依赖未完成的承诺。阶段间允许并行，但验收门槛必须逐条通过。
>
> 文中命令均在仓库根目录执行；`FIXTURE` 指 `fixtures/gen/data`。

---

## 阶段总览

| 阶段 | 内容 | 主要交付物 | 状态 |
|------|------|-----------|------|
| P0 | 架构与格式基线 | `docs/00-architecture.md`、`docs/02-format-notes.md`、本文件 | 已完成 |
| P1 | TSDB 格式读取库 | `crates/tsdb-reader`、对拍测试、fixture | 已完成 |
| P2 | 迁移引擎 + Sink + CLI | `crates/migrate-core`、`crates/tsdb-migrate-cli`、Parquet 输出 | 已完成 |
| P3 | HTTP API 服务 | `crates/tsdb-migrate-server`、状态持久化、控制接口 | 已完成 |
| P4 | Web 控制台 | `web/*`、内嵌静态资源、端到端操作 | 已完成 |
| P5 | 性能/稳定性验收 | 基准数据、`docs/05-verification.md`、运维手册 | 完成（E1 未达标，见验证报告） |
| P6 | Mimir 对象存储数据源 | tenant 感知的 S3 接入、本地模拟、CLI/API/Web、验证 | 已完成（真实 S3 待补测） |

> 状态在实现完成后回填；若某项未达成，保持“进行中”并记录阻塞原因，不虚报。

---

## P0 — 架构与格式基线

### 交付物

- `docs/00-architecture.md`：总体架构、组件边界、并发/内存模型、输出模型。
- `docs/02-format-notes.md`：Index/Chunks/Tombstones/ChunkEnc 的字段级说明，逐条映射到 `prometheus/tsdb` 源码。
- `docs/01-roadmap.md`：本文件。

### 验收标准

1. 架构图覆盖 Reader / Engine / Sink / API / Web 五层，且每层“不负责什么”写清楚。
2. 格式文档中每个结构体都能在 Prometheus 源码中找到对应实现（给出文件与函数名）。
3. 明确版本兼容矩阵（index v1/v2/v3、encoding 0–6、tombstones v1）。

---

## P1 — TSDB 格式读取库

### 交付物

- `crates/tsdb-reader`：
  - `meta.rs`：`meta.json` 解析、Block 发现与排序、重叠检测。
  - `varint.rs`：uvarint/varint（zigzag）与边界检查。
  - `index.rs`：magic/version、TOC（末尾 52 字节）、Symbol Table（32 个一组偏移）、Series 顺序遍历（16 字节对齐、长度前缀、CRC）。
  - `chunks.rs`：segment 枚举与 mmap、magic/version 校验、`ChunkRef` 定位、CRC32C。
  - `chunkenc/`：XOR / XOR2 / Histogram / FloatHistogram / HistogramST / FloatHistogramST 解码；`bstream`、`varbit`、XOR 值解码。
  - `tombstones.rs`：magic/version、CRC、区间合并、查询。
- `fixtures/gen`：Go 生成器（真实 Block + 6 种 chunk fixture + ground truth JSON）。
- 单元/集成测试：`cargo test -p tsdb-reader`。

### 验收标准

| # | 验收项 | 命令 | 期望 |
|---|--------|------|------|
| A1 | 真实 Block 的 series 数与 ground truth 一致 | `cargo test -p tsdb-reader --test block_truth` | 42 series，标签逐条相等 |
| A2 | 浮点样本逐条一致（含 NaN/StaleNaN 位模式） | 同上 | 0 差异 |
| A3 | 直方图样本一致（count/zero_count/sum/schema/spans/绝对桶/ST） | 同上 | 0 差异 |
| A4 | 6 种 chunk 编码解码正确 | `cargo test -p tsdb-reader chunkenc` | 全部通过 |
| A5 | Tombstones 过滤正确 | 同上 | 被删区间样本数 = 0，区间外样本保留 |
| A6 | 损坏 CRC 能报错 | `cargo test -p tsdb-reader crc` | 返回 `Checksum` 错误 |
| A7 | `inspect` 能列出块信息 | `cargo run -p tsdb-migrate-cli -- inspect --data-dir $FIXTURE/block` | 输出 ULID/时间范围/series/chunks |

---

## P2 — 迁移引擎、Sink 与 CLI

### 交付物

- `crates/migrate-core`：
  - `planner`：块计划、时间/标签过滤、`meta.stats` 与 index 预扫描。
  - `engine`：block 级并行 + block 内 reader/decoder/coordinator 流水线。
  - `control`：`Created/Running/Pausing/Paused/Stopping/Stopped/Completed/Failed` 状态机。
  - `batch` + `sink/parquet`：series / samples / histograms 三张表；zstd/snappy/none。
  - `checkpoint`：unit 级提交与恢复清理。
  - `manifest` + `duckdb`：行数、耗时、错误；生成/执行 DuckDB 装载 SQL。
- `crates/tsdb-migrate-cli`：`inspect / plan / migrate / verify / duckdb-load`。
- 输出目录布局与 `manifest.json`。

### 验收标准

| # | 验收项 | 命令 | 期望 |
|---|--------|------|------|
| B1 | 迁移 fixture 块成功 | `tsdb-migrate migrate --data-dir $FIXTURE/block --out /tmp/out` | 退出码 0，生成 manifest |
| B2 | 样本行数 = ground truth − tombstone 删除数 | `tsdb-migrate verify --out /tmp/out` | 行数与 manifest 一致 |
| B3 | Parquet 可被 DuckDB 查询 | `duckdb /tmp/out.db -c "..."` 或 `load_duckdb.sql` | 查询到已知 series 与样本值 |
| B4 | 暂停/恢复不重不漏 | 迁移大块时 `SIGINT`/`resume`（CLI 用 `--resume`） | 行数与一次跑完一致 |
| B5 | 已完成块二次运行跳过 | 再次执行同一命令 | 日志显示 skipped，不新增 part |
| B6 | 时间范围过滤正确 | `--min-time/--max-time` | 导出样本均落在区间内 |
| B7 | 内存有界 | `/usr/bin/time -l` 跑大块 | 峰值 RSS 与块大小无线性关系，记录于 P5 |

CLI 交互约定：

```
tsdb-migrate migrate --data-dir <dir> --out <dir> \
  [--blocks ULID,...] [--min-time RFC3339|ms] [--max-time ...] [--match k=v] \
  [--sink parquet|ndjson] [--compression zstd|snappy|none] \
  [--block-parallel 2] [--decode-workers 4] [--batch-samples 65536] \
  [--unit-series 50000] [--channel-capacity 2048] \
  [--resume] [--force] [--no-verify-crc] [--skip-bad-chunks] \
  [--json-progress] [--limit-series N]
```

---

## P3 — HTTP API 服务

### 交付物

- `crates/tsdb-migrate-server`：
  - `JobManager`：创建/启动/暂停/恢复/停止/删除、状态落盘、重启恢复。
  - API：`/api/health`、`/api/blocks`、`/api/jobs`（CRUD）、`/api/jobs/:id/actions`、`/api/jobs/:id/blocks`、`/api/jobs/:id/logs`。
  - 路径白名单、可选 Bearer Token、CORS 配置。
  - `docs/03-api.md` 契约文档。

### 验收标准

| # | 验收项 | 命令 | 期望 |
|---|--------|------|------|
| C1 | 健康检查 | `curl /api/health` | `{"status":"ok"}` |
| C2 | 扫描 Block | `curl "/api/blocks?data_dir=..."` | 返回块列表与统计 |
| C3 | 创建并启动任务 | `POST /api/jobs` + `POST .../actions {"action":"start"}` | 状态 `running` |
| C4 | 暂停 | `{"action":"pause"}` | ≤ 5s 变 `paused`，进度停止增长 |
| C5 | 恢复 | `{"action":"resume"}` | 继续增长，最终 `completed` |
| C6 | 停止 | `{"action":"stop"}` | `stopped`，checkpoint 可恢复 |
| C7 | 重启恢复 | 重启 server 后查询任务 | 状态为 `paused` 且 `interrupted=true`，可 resume |
| C8 | 鉴权 | 带错误 token 请求 | 401 |
| C9 | 路径越权 | `data_dir=/etc` | 400 |

---

## P4 — Web 控制台

### 交付物

- `web/index.html`、`web/app.js`、`web/styles.css`（无构建、无外链）。
- 服务端内嵌静态资源（单二进制）。
- 支持：任务列表、创建任务（扫描块/选择块/参数）、启动/暂停/恢复/停止、总体进度、分块明细、速率/ETA、日志、错误提示。

### 验收标准

| # | 验收项 | 期望 |
|---|--------|------|
| D1 | 打开 `/` 正常渲染，无控制台报错 | 深色高密度终端风格 |
| D2 | 创建任务表单可扫描并展示块 | 与 `/api/blocks` 一致 |
| D3 | 四个控制按钮状态正确 | 按状态机禁用/启用 |
| D4 | 进度实时更新 | 1–2s 轮询，series/chunks/samples/速率/ETA 变化 |
| D5 | 断线/错误提示 | API 不可达时显示离线状态，不白屏 |
| D6 | Token 模式可配置 | localStorage 保存，401 提示重新输入 |

---

## P5 — 性能与稳定性验收

### 交付物

- `fixtures/gen -big` 生成的基准块（400 series × 1441 samples，含直方图）。
- `docs/05-verification.md`：对拍结果、DuckDB 查询结果、吞吐/RSS 实测、已知限制。
- `docs/04-operations.md`：部署、参数调优、故障恢复、容量规划。

### 验收标准

> 实测结果与未达标项见 `docs/05-verification.md` §9；不修改验收口径。

| # | 验收项 | 期望 |
|---|--------|------|
| E1 | 单块单 worker 解码吞吐 | ≥ 100 MB/s 压缩 chunk（实测 45 MB/s，未达标） |
| E2 | 多块并行扩展 | 4 倍资源下吞吐 ≥ 2.5 倍 |
| E3 | 内存 | 峰值 RSS ≤ `500MB + block_parallel × decode_workers × 64MB` |
| E4 | 长跑稳定 | 连续迁移 ≥ 10 分钟无泄漏、无 FD 增长 |
| E5 | 断点续传一致性 | 中断 3 次后最终行数与一次跑完完全一致 |
| E6 | 校验工具 | `verify` 能发现人为删除/截断的 part 文件 |

---

## 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| Index v3 / 未来编码变更 | 读取失败 | 版本白名单 + 明确报错；未知 encoding 计入 `unknown_encoding`，不静默丢数据 |
| 重叠 Block | 重复样本 | `inspect` 告警；支持 `--blocks` 白名单；输出保留 `block` 分区便于去重 |
| 磁盘写满 | 任务失败 | 启动前检查可用空间（预估 1.2× chunk 大小）；失败保留 checkpoint |
| 暂停延迟 | 用户感知 | unit 默认 5 万 series，可通过 `--unit-series` 调小（代价是更多小文件） |
| Parquet 小文件过多 | 查询变慢 | 按 unit 合并写入；`--unit-series` 调大；文档给出 compact 建议 |
| 直方图精度 | count 用 f64 | 记录限制；>2^53 的计数属于异常场景 |
| 路径安全 | 服务器被滥用 | canonicalize + allow-root 白名单 + 可选 token |

---

## P6 — Grafana Mimir 对象存储数据源

### 交付物

- `crates/migrate-core/src/mimir.rs`：
  - S3（`object_store`，兼容 MinIO/Ceph/LocalStack）与本地目录模拟两种后端。
  - tenant 发现（delimiter listing）、对象清单流式分组、`meta.json` 并发读取。
  - 按块流式下载到本地 cache（并发可配、`.ready` 标记、失败清理、完成后按配置清理）。
- tenant 感知的核心链路：
  - block 内部 key = `tenant/ulid`；
  - 输出路径 `blocks/tenant=<tenant>/block=<ULID>/...`（本地目录源保持原路径）；
  - checkpoint `state/blocks/<tenant>/<ULID>.json`；
  - `BlockSnapshot`/manifest 增加 tenant；DuckDB Hive 分区自动带 `tenant` 列。
- CLI：`--mimir-bucket / --mimir-prefix / --mimir-region / --mimir-endpoint / --mimir-tenant / --mimir-local-root / --mimir-cache-dir / --mimir-keep-cache`。
- API：`POST /api/mimir/tenants`；`POST /api/jobs` 支持 `mimir` 配置与 `tenants` 选择。
- Web：数据源切换、tenant 扫描/勾选、tenant 列与配置展示。

### 验收标准与结果

| # | 验收项 | 结果 |
|---|--------|------|
| F1 | 解析 Mimir 布局并识别 tenant | ✅ 本地模拟 bucket 识别 tenant-a/b |
| F2 | 多 tenant 同名 ULID 不冲突 | ✅ 输出/checkpoint 按 tenant 分开 |
| F3 | tenant 过滤 | ✅ API 创建仅 tenant-a，samples=4910 |
| F4 | 对象下载缓存与清理 | ✅ 下载/`.ready`/完成后清理 |
| F5 | 断点续传覆盖 Mimir | ✅ 2 × 28,815,342 行与一次跑完一致 |
| F6 | CLI/API/Web 入口 | ✅ 见 `docs/05-verification.md` §10 |
| F7 | 真实 AWS S3 | ⏳ 代码路径已接入，当前环境无凭证/桶，未做真实桶压测 |
| F8 | DuckDB tenant 分区 | ✅ `select tenant,count(*)` 正确 |
