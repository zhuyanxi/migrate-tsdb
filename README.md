# tsdb-migrate

把 Prometheus TSDB Block / Grafana Mimir 对象存储中的海量历史样本，以**有界内存、可暂停/恢复、不重不漏**的方式迁移到 Parquet / DuckDB。Rust 实现，不依赖 Go runtime，不构建 Prometheus 查询引擎。

```
Prometheus Blocks ─┐
                   ├─▶ tsdb-reader ──▶ migrate-core ──▶ Parquet (Hive 分区) ──▶ DuckDB
Mimir S3 (tenant) ─┘        │                 │
  tenant 发现          index/chunks/     pipeline / checkpoint / sink
  按块下载 cache        tombstones
                              │
                   CLI  ◀─────┴─────▶ HTTP API + Web 控制台
```

## 特性

- **零拷贝读取**：mmap index 与 chunk segment；只建立 `symbols/32` 的偏移表，不把全部 label 字符串放内存。
- **全编码支持**：XOR、XOR2、Histogram、FloatHistogram、HistogramST、FloatHistogramST。
- **Tombstones 感知**：按 series ref + 闭区间过滤，不导出已删除样本。
- **Grafana Mimir 数据源**：自动发现 `<bucket>/<prefix>/<tenant>/<ULID>`，支持 AWS S3 / MinIO / 本地目录模拟；tenant 隔离输出与 checkpoint。
- **有界内存流水线**：Block 级并行 × 块内解码线程，背压 + 批量 + 单元屏障；内存与数据总量无关。
- **断点续传**：单元级 checkpoint + 确定性 part 文件名，恢复时清理未提交尾部，part 粒度不重不漏。
- **三种交付形态**：本地 CLI、可部署 HTTP API、内嵌 Web 控制台（启动/暂停/恢复/停止 + 详细进度）。
- **DuckDB metric 宽表**：每个 metric 一张表（表名 = metric name），label 作为列，含 `timestamp`；Prometheus 单实例，Mimir 每 tenant 独立实例。

## 快速开始

### CLI

```bash
cargo build --release

# 查看块
./target/release/tsdb-migrate inspect --data-dir /prometheus/data

# 迁移到 Parquet
./target/release/tsdb-migrate migrate \
  --data-dir /prometheus/data \
  --out /mnt/export \
  --block-parallel 2 --decode-workers 4

# 校验行数
./target/release/tsdb-migrate verify --out /mnt/export

# DuckDB 装载（Prometheus：单 DB）
./target/release/tsdb-migrate duckdb-load --out /mnt/export --db /mnt/metrics.duckdb

# DuckDB 装载（Mimir：每 tenant 一个 DB）
./target/release/tsdb-migrate duckdb-load --out /mnt/export --db /mnt/duckdb/

# 只生成 SQL
./target/release/tsdb-migrate duckdb-load --out /mnt/export --no-exec
```

中断后再次执行同一条 `migrate` 命令即可续跑（默认 `resume=true`）；已完成块会跳过。

### Mimir（S3）

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1

# 发现 tenant
./target/release/tsdb-migrate inspect --mimir-bucket bucketA --mimir-prefix mimir

# 迁指定 tenant（省略 --mimir-tenant 表示全部）
./target/release/tsdb-migrate migrate \
  --mimir-bucket bucketA --mimir-prefix mimir --mimir-tenant tenant-a \
  --out /mnt/export --block-parallel 2 --decode-workers 4

# MinIO / 本地镜像
./target/release/tsdb-migrate migrate --mimir-bucket mimir-blocks \
  --mimir-endpoint http://127.0.0.1:9000 --mimir-allow-http --mimir-path-style \
  --out /mnt/export
./target/release/tsdb-migrate migrate --mimir-local-root /mnt/mimir-mirror --out /mnt/export
```

### Server + Web

```bash
TSDB_MIGRATE_TOKEN=change-me ./target/release/tsdb-migrate-server \
  --bind 0.0.0.0:8080 \
  --state-dir /var/lib/tsdb-migrate \
  --allow-root /prometheus \
  --allow-root /mnt/export
```

打开 `http://<host>:8080/`：新建任务、扫描 Block、启动/暂停/恢复/停止、查看总体与分块进度、吞吐、ETA、日志。

## 输出

```
<out>/
├── manifest.json
├── load_duckdb.sql                    # Prometheus
├── load_duckdb_<tenant>.sql           # Mimir，每 tenant 一份
├── migration.duckdb                   # Prometheus 单实例
├── duckdb/<tenant>.duckdb             # Mimir 每 tenant 实例
├── .duckdb_stage/                     # metric 分区中间产物
├── .state/blocks/<ULID>.json          # 本地源 checkpoint
├── .state/blocks/<tenant>/<ULID>.json # Mimir 源 checkpoint
├── .cache/                            # Mimir 下载缓存（完成后默认清理）
└── blocks/
    ├── block=<ULID>/                  # 本地源
    │   ├── _complete.json
    │   └── {series,samples,histograms}/part-*.parquet
    └── tenant=<tenant>/block=<ULID>/  # Mimir 源
        ├── _complete.json
        └── {series,samples,histograms}/part-*.parquet
```

Parquet 中间层仍为 `series/samples/histograms` 三张表；DuckDB 最终结构为：

- Prometheus：单个 `migration.duckdb`。
- Mimir：`duckdb/<tenant>.duckdb`，每个 tenant 独立。
- 每个 metric 一张表：`timestamp` + `value`（或直方图列）+ 该 metric 的 label 列。

```sql
SELECT * FROM "http_requests_total"
WHERE "timestamp" >= TIMESTAMP '2026-11-01 00:00:00';
```

完整 DDL、label 冲突策略与限制见 `docs/07-duckdb-schema.md`。

## 文档

| 文档 | 内容 |
|------|------|
| [docs/00-architecture.md](docs/00-architecture.md) | 架构、组件边界、并发/内存模型、输出模型 |
| [docs/01-roadmap.md](docs/01-roadmap.md) | 分阶段方案、交付物、验收标准与状态 |
| [docs/02-format-notes.md](docs/02-format-notes.md) | Index/Chunks/Tombstones/ChunkEnc 字段级说明与踩坑 |
| [docs/03-api.md](docs/03-api.md) | HTTP API 契约、状态机、错误码 |
| [docs/04-operations.md](docs/04-operations.md) | 部署、调参、DuckDB、Mimir 接入、故障恢复、监控 |
| [docs/05-verification.md](docs/05-verification.md) | 对拍、端到端、断点续传、性能实测与已知限制 |
| [docs/06-mimir-source.md](docs/06-mimir-source.md) | Mimir 接入方式、对象定位规则、各层改动范围与兼容性 |
| [docs/07-duckdb-schema.md](docs/07-duckdb-schema.md) | DuckDB metric 宽表、按 tenant 分库、DDL 与装载流程 |

## 仓库结构

```
crates/
  tsdb-reader/           # 纯格式层：meta/index/chunks/tombstones/chunkenc
  migrate-core/          # 规划、流水线、控制、checkpoint、sink、manifest、DuckDB
  tsdb-migrate-cli/      # tsdb-migrate 二进制
  tsdb-migrate-server/   # tsdb-migrate-server 二进制（内嵌 web/）
web/                     # 单页控制台（无构建、无外链）
fixtures/gen/            # Go 生成器：真实 Block + 6 种编码 fixture + ground truth
docs/                    # 架构/路线图/格式/API/运维/验证
scripts/                 # dev-fixtures.sh / e2e.sh / bench.sh
prometheus/              # 本项目依赖的 Prometheus 源码快照（只读参考）
```

## 测试

```bash
cargo test --workspace        # 33 个测试：格式对拍、CRC、状态机、配置、路径安全、DuckDB schema、Mimir 本地源
scripts/e2e.sh                # 本地 fixture → Parquet → verify → DuckDB
scripts/mimir-local-e2e.sh    # Mimir 布局双 tenant（同名 ULID）→ verify → DuckDB
scripts/bench.sh gen 20000    # 生成 2000 万级样本基准块
scripts/bench.sh              # release 吞吐/RSS
```

`cargo test` 依赖 `fixtures/gen/data/block`（已随仓库保留）。重新生成：

```bash
scripts/dev-fixtures.sh        # 需要 Go 1.26+
```

## 性能（M 系列 10 核，release）

- 单块 20002 series / 2880 万样本：0.93–1.04s，峰值 RSS 72–110 MiB。
- 4 块 1.15 亿样本，`block_parallel=4`：1.66s，302 MiB，69.4M samples/s。
- 内存 ≈ 基础 + 并发度 × 每 worker 缓冲；与数据总量无关。

## 已知限制

- 只读已落盘 Block；不读取 Head/WAL/`chunks_head`。
- Mimir 支持 S3 / MinIO / 本地目录模拟；不做远程 range 读取，按块下载到本地 cache。
- 不解析 postings；标签过滤在 series 遍历时进行。
- 不做跨块去重；输出保留 `block` 分区，重叠块会在 `inspect` 中告警。
- 单线程 chunk 吞吐约 45 MB/s，未达 100 MB/s 目标（多块并发约 113 MB/s）；详见验证报告。
- `unit_series` 过小会导致大量 fsync，生产建议 5 万级。

## License

Apache-2.0（与仓库内 Prometheus 源码许可一致）。
