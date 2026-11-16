# tsdb-migrate

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/zhuyanxi/migrate-tsdb/pulls)

把 Prometheus TSDB Block 与 Grafana Mimir 对象存储里的海量历史样本，以**有界内存、可暂停恢复**的方式迁移到 Prometheus Remote Write v1.0 接收端。纯 Rust 实现，不需要 Go runtime，也不构建 Prometheus 查询引擎。

```
Prometheus Blocks ─┐
                   ├─▶ tsdb-reader ──▶ migrate-core ──▶ Prometheus Remote Write v1.0
Mimir S3 (tenant) ─┘        │                 │
  tenant 发现          index/chunks/     pipeline / checkpoint / sink
  按块下载 cache        tombstones
                              │
                   CLI  ◀─────┴─────▶ HTTP API + Web 控制台
```

## 目录

- [项目简介](#项目简介)
- [安装与快速开始](#安装与快速开始)
- [使用示例](#使用示例)
- [配置说明](#配置说明)
- [开发与测试](#开发与测试)
- [性能](#性能)
- [已知限制](#已知限制)
- [仓库结构](#仓库结构)
- [贡献指南](#贡献指南)
- [许可证](#许可证)
- [致谢与联系](#致谢与联系)

## 项目简介

Prometheus 的 Block 目录不适合直接批量读：index 是自定义二进制、chunk 有 6 种编码、还需要处理 tombstones。常见做法是用 Prometheus 自己的查询接口慢慢拉，或者写脚本逐块解析——前者慢且受限于查询引擎，后者很容易在内存和断点续传上翻车。

本项目把「读格式」和「搬数据」彻底分开：`tsdb-reader` 负责零拷贝解析，`migrate-core` 负责流水线与状态管理，上层同时提供 CLI、HTTP API 和 Web 控制台三种入口。

### 核心功能

| 能力 | 说明 |
|------|------|
| 零拷贝读取 | mmap index 与 chunk segment；只建立 `symbols/32` 偏移表，不把全部 label 字符串读进内存 |
| 全编码支持 | XOR、XOR2、Histogram、FloatHistogram、HistogramST、FloatHistogramST |
| Tombstones 感知 | 按 series ref + 闭区间过滤，不导出已删除样本 |
| 两种数据源 | 本地 Prometheus 目录；Grafana Mimir 对象存储（AWS S3 / MinIO / 本地目录模拟），自动识别 tenant 并通过 tenant header 路由 |
| 有界内存 | Block 级并行 × 块内解码线程，背压 + 批量 + 单元屏障；内存占用与数据总量无关 |
| 断点续传 | 单元级 checkpoint；接收端写入与本地 checkpoint 非事务，恢复时可能重发当前单元 |
| 唯一输出 | Prometheus Remote Write v1.0（Snappy + protobuf），样本数据不落本地文件 |
| 三种形态 | 本地 CLI、可部署 HTTP API、内嵌 Web 控制台（启动/暂停/恢复/停止 + 实时进度） |

## 安装与快速开始

### 环境要求

- **Rust stable**（开发环境为 1.99；`tsdb-reader` crate 声明的 MSRV 为 1.77）
- 可选：Go 1.26+（仅在重新生成测试 fixture 时需要）

### 从源码构建

```bash
git clone https://github.com/zhuyanxi/migrate-tsdb.git
cd migrate-tsdb
cargo build --release
```

产物：

- `target/release/tsdb-migrate` — CLI
- `target/release/tsdb-migrate-server` — HTTP API + 内嵌 Web 控制台

### 5 分钟上手

```bash
# 1. 看一眼有哪些块
./target/release/tsdb-migrate inspect --data-dir /prometheus/data

# 2. 推送到 Remote Write（中断后重跑同一条命令即可恢复）
export TSDB_MIGRATE_REMOTE_WRITE_BEARER_TOKEN=replace-me
./target/release/tsdb-migrate migrate \
  --data-dir /prometheus/data \
  --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write \
  --block-parallel 1 --decode-workers 4
```

`--out` 是本地任务状态目录，只存 checkpoint、manifest 和 block 完成标记，不存样本数据。

## 使用示例

### 1. 推送到 Remote Write 接收端

```bash
export TSDB_MIGRATE_REMOTE_WRITE_BEARER_TOKEN=replace-me
./target/release/tsdb-migrate migrate \
  --data-dir /prometheus/data --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write
```

### 2. 迁移 Grafana Mimir 对象存储

对象布局为 `<bucket>/<prefix>/<tenant>/<ULID>/{meta.json,index,tombstones,chunks/}`。

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1

# 发现 tenant
./target/release/tsdb-migrate inspect --mimir-bucket bucketA --mimir-prefix mimir

# 迁指定 tenant（省略 --mimir-tenant 表示全部）
./target/release/tsdb-migrate migrate \
  --mimir-bucket bucketA --mimir-prefix mimir --mimir-tenant tenant-a \
  --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write
```

MinIO / 本地镜像：

```bash
./target/release/tsdb-migrate migrate --mimir-bucket mimir-blocks \
  --mimir-endpoint http://127.0.0.1:9000 --mimir-allow-http --mimir-path-style \
  --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write

./target/release/tsdb-migrate migrate \
  --mimir-local-root /mnt/mimir-mirror --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write
```

本地源不发送 tenant header；Mimir 默认使用 `X-Scope-OrgID`，可用 `--remote-write-tenant-header` 修改。Remote Write v1.0 不支持 native histogram，遇到会直接失败而不是静默丢数。协议细节见 [docs/08-remote-write-sink.md](docs/08-remote-write-sink.md)。

### 3. 启动 HTTP API + Web 控制台

```bash
TSDB_MIGRATE_TOKEN=change-me ./target/release/tsdb-migrate-server \
  --bind 0.0.0.0:8080 \
  --state-dir /var/lib/tsdb-migrate \
  --allow-root /prometheus \
  --allow-root /mnt/export
```

打开 `http://<host>:8080/`：新建任务、扫描 Block、选择 tenant、启动/暂停/恢复/停止，查看总体与分块进度、吞吐、ETA 和日志。API 契约见 [docs/03-api.md](docs/03-api.md)。

## 配置说明

### CLI 子命令

| 命令 | 用途 |
|------|------|
| `tsdb-migrate inspect` | 列出数据目录或 Mimir bucket 中的块与 tenant |
| `tsdb-migrate plan` | 只做规划，不写数据 |
| `tsdb-migrate migrate` | 执行迁移 |

### 迁移关键参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `--data-dir` | — | Prometheus 数据目录（与 Mimir 参数二选一） |
| `--out` | 必填 | 本地任务状态目录，不保存样本数据 |
| `--remote-write-url` | 必填 | Prometheus Remote Write v1.0 接收端 URL |
| `--remote-write-tenant-header` | `X-Scope-OrgID` | Mimir tenant header；留空表示不发送 |
| `--block-parallel` | 2 | 同时处理的块数；Remote Write 会被强制为 1 以保证顺序 |
| `--decode-workers` | 4 | 单块内解码线程数 |
| `--batch-samples` | 65536 | 批次样本上限（≥1024） |
| `--batch-bytes-mb` | 32 | 批次内存上限 |
| `--unit-series` | 50000 | checkpoint 粒度（每单元 series 数），过小会导致大量 fsync |
| `--channel-capacity` | 2048 | series 通道容量，控制背压 |
| `--match` | — | 精确 label 匹配，如 `job=api`，可重复 |
| `--min-time` / `--max-time` | — | 毫秒时间戳范围过滤 |
| `--no-resume` | — | 忽略 checkpoint 重新开始 |
| `--force` | — | 已完成块也重新迁移 |
| `--fail-fast` | — | 单块失败即中止（默认继续后续块） |
| `--no-verify-crc` | — | 跳过 CRC32C 校验，换取吞吐 |
| `--skip-bad-chunks` | — | 解码失败的 chunk 跳过而不使整块失败 |

### Mimir 参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `--mimir-bucket` | — | S3 bucket |
| `--mimir-prefix` | 空 | 对应 Mimir `-blocks-storage.storage-prefix` |
| `--mimir-tenant` | 全部 | 可重复或逗号分隔 |
| `--mimir-region` | `us-east-1` | |
| `--mimir-endpoint` | — | 自定义 S3 endpoint（MinIO / Ceph / LocalStack） |
| `--mimir-local-root` | — | 用本地目录模拟 bucket 布局 |
| `--mimir-cache-dir` | `<out>/.cache` | 按块下载缓存目录 |
| `--mimir-keep-cache` | — | 迁移完成后保留缓存 |

### 环境变量

| 变量 | 作用 |
|------|------|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | S3 凭证（走标准 AWS 链） |
| `TSDB_MIGRATE_TOKEN` | API 服务的 Bearer token，等价于 `--token` |
| `TSDB_MIGRATE_REMOTE_WRITE_BEARER_TOKEN` | Remote Write 鉴权，不写入任务状态文件 |

### 输出目录

```
<out>/
├── manifest.json                      # 任务级汇总
├── blocks/block=<ULID>/_complete.json # 本地源完成标记
├── blocks/tenant=<tenant>/block=<ULID>/_complete.json # Mimir 源完成标记
├── .state/blocks/<ULID>.json          # checkpoint
└── .cache/                            # 可选的 Mimir 下载缓存
```

### 调参建议

- 内存 ≈ 基础开销 + `block_parallel` × 每 worker 缓冲，与数据总量无关；压内存先降 `--block-parallel`，再降 `--batch-bytes-mb`。
- 网络与接收端吞吐是常见瓶颈：`--batch-samples` 和 `--batch-bytes-mb` 控制请求大小，`--unit-series` 控制 checkpoint 与重放粒度。
- 只想快速抽样验证时，用 `--match` 或 `--min-time/--max-time` 缩小范围。

## 开发与测试

```bash
cargo build --workspace
cargo test --workspace

scripts/e2e.sh              # fixture → Remote Write 本地测试接收端
scripts/mimir-local-e2e.sh  # Mimir tenant 布局 inspect/plan smoke test
scripts/bench.sh gen 20000  # 生成基准块
scripts/bench.sh            # release 吞吐 / RSS
```

测试数据由 `fixtures/gen` 用**仓库内 Prometheus 源码**生成真实 Block 与 6 种编码的 ground truth，`tsdb-reader` 的解码结果与之逐字节对拍。重新生成需要 Go 1.26+：

```bash
scripts/dev-fixtures.sh     # 重新生成 fixtures/gen/data
scripts/sync-fixtures.sh    # 同步一份到 crates/tsdb-reader/tests/data
```

## 性能

迁移吞吐取决于 Remote Write 接收端、网络、批次大小和解码并行度。设置 `TSDB_MIGRATE_BENCH_REMOTE_WRITE_URL` 后，可用 `scripts/bench.sh` 对测试接收端测量端到端吞吐与 RSS。

## 已知限制

- 只读已落盘 Block，不读取 Head / WAL / `chunks_head`。
- Mimir 不做远程 range 读取，按块下载到本地 cache 后处理。
- 不解析 postings，label 过滤在 series 遍历时进行。
- 不做跨块去重；同一 tenant 时间范围重叠的 block 会被拒绝，以保证 Remote Write 样本顺序。
- 为保证时间顺序，Remote Write 会将 block 并行度设为 1；块内解码仍可并行。
- Remote Write v1.0 无 native histogram；且 HTTP 写入与本地 checkpoint 不是一个事务，异常退出后当前批可能重发（接收端对相同 series+时间戳幂等）。
- `migrate-core::mimir::s3_download_tests` 有 2 个用例当前失败（断点续传的请求次数断言），与迁移主链路无关，待修。

## 仓库结构

```
crates/
  tsdb-reader/           # 格式层：meta/index/chunks/tombstones/chunkenc（可独立发布）
  migrate-core/          # 规划、流水线、控制、checkpoint、Remote Write、manifest
  tsdb-migrate-cli/      # tsdb-migrate 二进制
  tsdb-migrate-server/   # tsdb-migrate-server 二进制（内嵌 web/）
web/                     # 单页控制台（无构建、无外链）
fixtures/gen/            # Go 生成器：真实 Block + 6 种编码 fixture + ground truth
docs/                    # 架构/路线图/格式/API/运维/验证
scripts/                 # dev-fixtures / sync-fixtures / e2e / mimir-local-e2e / bench
prometheus/              # 依赖的 Prometheus 源码快照（只读参考）
```

## 贡献指南

欢迎 Issue 与 Pull Request。

1. **先开 Issue 对齐方向**：涉及接口、输出格式或 checkpoint 语义的改动，先描述场景与方案，避免大改返工。
2. **Fork + 分支**：从 `main` 切出 `feat/...` 或 `fix/...` 分支。
3. **保持提交粒度**：一条提交做一件事，信息用 `<type>(<scope>): <summary>` 形式（如 `fix(tsdb-reader): declare an MSRV the dependency tree actually supports`）。
4. **提交前跑门禁**：

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets
   cargo test --workspace
   cargo test -p tsdb-reader --no-default-features    # feature 组合不能坏
   ./scripts/e2e.sh && ./scripts/mimir-local-e2e.sh
   ```
5. **改到格式层要补对拍**：`tsdb-reader` 的任何解析改动，必须让 `fixtures` 测试仍然逐字节通过；改了 fixture 就同步 `tests/data`。
6. **改到行为要补文档**：`docs/` 下的架构、API、验证文档与 README 一起更新。

代码风格：`rustfmt` 默认配置，拒绝新增 clippy 告警；公开 API 必须有文档注释（`tsdb-reader` 已开启 `#![deny(missing_docs)]`）。

## 许可证

本项目以 **MIT** 协议开源，全文见 [LICENSE](LICENSE)。

```
MIT License

Copyright (c) 2026 zhuyanxi
```

`crates/tsdb-reader/src/chunkenc/` 下的 chunk 解码器是 Prometheus TSDB 编解码器的 Rust 移植，这部分仍适用 Apache License 2.0，归属与文件清单见 [NOTICE](NOTICE)，许可证全文见 [LICENSE-APACHE](LICENSE-APACHE)。

## 致谢与联系

- [Prometheus](https://github.com/prometheus/prometheus)：TSDB 格式文档与 chunk 编解码的参考实现（Apache-2.0）。
- [Grafana Mimir](https://github.com/grafana/mimir)：对象存储布局与 tenant 机制的参考。
- [Prometheus Remote Write](https://prometheus.io/docs/specs/prw/remote_write_spec/)：兼容的远端写入协议。

问题与建议请提 [GitHub Issues](https://github.com/zhuyanxi/migrate-tsdb/issues)；安全问题请勿公开提交，直接联系维护者 [@zhuyanxi](https://github.com/zhuyanxi)。
