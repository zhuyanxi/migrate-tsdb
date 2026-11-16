# 部署与运维手册

## 1. 构建与产物

```bash
cargo build --release
```

产物：

| 文件 | 用途 |
|------|------|
| `target/release/tsdb-migrate` | 本地/一次性迁移 CLI |
| `target/release/tsdb-migrate-server` | HTTP API + 内嵌 Web 控制台（单二进制） |

依赖：Rust 1.85+；无 C/C++ 运行时依赖。

交叉编译示例：

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

---

## 2. CLI

### 2.1 常用命令

```bash
# 查看块
tsdb-migrate inspect --data-dir /prometheus/data

# 只做计划，不写数据
tsdb-migrate plan --data-dir /prometheus/data --out /mnt/export

# 推送到 Remote Write
tsdb-migrate migrate --data-dir /prometheus/data --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write \
  --block-parallel 1 --decode-workers 4 --unit-series 50000

# 只迁某几个块 / 时间范围 / 标签
tsdb-migrate migrate ... --blocks 01H...,01H...
tsdb-migrate migrate ... --min-time 2026-01-01 --max-time 2026-02-01
tsdb-migrate migrate ... --match job=api --match env=prod

# 中断后恢复（默认 resume=true，重复执行即可）
tsdb-migrate migrate --data-dir /prometheus/data --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write

# 强制重迁某个已完成块
tsdb-migrate migrate ... --force

```

### 2.2 退出码

| 码 | 含义 |
|----|------|
| 0 | 成功（Completed） |
| 1 | 参数/格式/IO 错误，任务未启动 |
| 2 | 任务结束但状态为 Failed 或 Stopped |

### 2.3 机器可读进度

`--json-progress` 时 stdout 每秒一行 JSON，最后一行 `event=finished`：

```json
{"state":"running","elapsed_ms":12000,"series_done":1500,"series_total":50000,"chunks_done":18000,"chunks_total":600000,"samples":2150000,"samples_skipped_tombstone":41,"samples_skipped_range":0,"bytes_read":1200000,"blocks_completed":0,"blocks_total":10,"samples_per_sec":182000.0,"eta_seconds":42}
{"event":"finished","state":"completed","failed_blocks":0,"skipped_blocks":0,"samples":28815462,"floats":28815342,"histograms":120,"samples_skipped_tombstone":41,"samples_skipped_range":0,"bytes_read":40000000,"bytes_written":0}
```

日志始终写 stderr，不污染 stdout。

---

## 3. Server 部署

### 3.1 启动

```bash
export TSDB_MIGRATE_TOKEN='change-me'
tsdb-migrate-server \
  --bind 0.0.0.0:8080 \
  --state-dir /var/lib/tsdb-migrate \
  --allow-root /prometheus \
  --allow-root /var/lib/tsdb-migrate/jobs
```

- `--allow-root` 可重复，默认当前工作目录；API 中所有路径必须落在其下。
- `--state-dir` 保存 `jobs/*.json` 与任务元数据。
- 未设置 token 时 API 完全开放，仅建议内网/本机使用。
- Web 控制台在 `http://<host>:8080/`，静态资源已内嵌，无需额外部署。

### 3.2 systemd

```ini
[Unit]
Description=tsdb-migrate control plane
After=network.target

[Service]
User=prometheus
Group=prometheus
Environment=TSDB_MIGRATE_TOKEN=change-me
ExecStart=/usr/local/bin/tsdb-migrate-server \
  --bind 0.0.0.0:8080 \
  --state-dir /var/lib/tsdb-migrate \
  --allow-root /prometheus \
  --allow-root /var/lib/tsdb-migrate/jobs
Restart=on-failure
RestartSec=3
# 迁移是 IO/CPU 密集型，避免被 OOM killer 误杀
MemoryMax=8G

[Install]
WantedBy=multi-user.target
```

### 3.3 Docker

```dockerfile
FROM debian:bookworm-slim
COPY target/release/tsdb-migrate-server /usr/local/bin/
RUN useradd -r -u 1000 migrate && mkdir -p /state && chown migrate /state
USER migrate
ENTRYPOINT ["/usr/local/bin/tsdb-migrate-server"]
CMD ["--bind","0.0.0.0:8080","--state-dir","/state","--allow-root","/prometheus","--allow-root","/jobs"]
```

```bash
docker run -d --name tsdb-migrate \
  -p 8080:8080 \
  -e TSDB_MIGRATE_TOKEN=change-me \
  -v /prometheus:/prometheus:ro \
  -v /var/lib/tsdb-migrate/jobs:/jobs \
  -v tsdb-migrate-state:/state \
  --memory 8g \
  tsdb-migrate-server
```

建议把 Prometheus 数据目录**只读**挂载；本地任务状态目录单独持久化挂载，样本由 Remote Write 发送到接收端。

### 3.4 反向代理

Nginx：

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 300s;
}
```

如果跨域访问 API，启动加 `--cors-any`（或在外层网关处理 CORS）。

---

## 4. 资源规划

### 4.1 参数

| 参数 | 默认 | 作用 | 调大 | 调小 |
|------|------|------|------|------|
| `block_parallel` | 2（实际固定为 1） | 同时处理的 Block 数 | Remote Write 为保证顺序固定为 1 | 内存↓ |
| `decode_workers` | 4 | 每个 Block 的解码线程 | 单块吞吐↑（受核数限制） | CPU/内存↓ |
| `batch_samples` | 65536 | 每个 HTTP 请求的样本上限 | 请求数↓，内存↑ | 内存↓，请求数↑ |
| `batch_bytes` | 32MiB | 每批字节上限 | 同上 | 同上 |
| `unit_series` | 50000 | checkpoint / 重放粒度 | checkpoint 次数↓，恢复时可能重放更多样本 | checkpoint 次数↑ |
| `channel_capacity` | 2048 | series 通道容量 | 流水线更顺，内存↑ | 内存↓ |

总解码线程 = `block_parallel × decode_workers`，建议 ≤ 物理核数 − 2。

### 4.2 内存估算

```
RSS ≈ 基础 80MiB
    + block_parallel × (index/symbol 常驻 + 2 × decode_workers × batch 缓冲)
    + 文件页缓存（可回收）
```

实测（M 系列 10 核，2 万 series / 2880 万样本 / 47MiB 块）：

| 配置 | 峰值 RSS | 耗时 | 吞吐 |
|------|---------|------|------|
| `bp=1, dw=1` | 72 MiB | 1.04s | 27.7M samples/s |
| `bp=1, dw=2` | 85 MiB | 1.03s | 28.0M samples/s |
| `bp=1, dw=4` | 110 MiB | 0.93s | 31.0M samples/s |
| `bp=1, dw=8` | 144 MiB | 1.56s | 18.5M samples/s |
| 4 块 `bp=4, dw=2` | 302 MiB | 1.66s | 69.4M samples/s |

建议容器内存 ≥ `1GiB + block_parallel × decode_workers × 64MiB`，并给 page cache 留余量。

### 4.3 网络与本地磁盘

- 样本数据直接发送到 Remote Write 接收端，不落本地文件；本地目录仅保存 manifest、checkpoint、完成标记和可选的 Mimir 下载缓存。
- `batch_samples` / `batch_bytes` 控制请求大小；批次越大，通常请求数越少但单次内存占用越高。
- Mimir 下载缓存可显著增加本地磁盘占用；配置 `mimir_keep_cache` 时会在迁移后保留。

---

## 5. Remote Write 与本地状态

### 5.1 本地目录

```
<out>/
├── manifest.json                      # 任务级汇总
├── .cache/                            # 可选的 Mimir 下载缓存
├── .state/blocks/<ULID>.json          # 本地源 checkpoint
├── .state/blocks/<tenant>/<ULID>.json # Mimir 源 checkpoint
└── blocks/
    ├── block=<ULID>/_complete.json
    └── tenant=<tenant>/block=<ULID>/_complete.json
```

样本数据不保存在 `<out>`；删除本地任务目录不会撤回已写入接收端的数据。

### 5.2 交付与恢复

```bash
export TSDB_MIGRATE_REMOTE_WRITE_BEARER_TOKEN=replace-me
tsdb-migrate migrate --data-dir /prometheus/data \
  --out /var/lib/tsdb-migrate/job \
  --remote-write-url https://receiver.example/api/v1/write \
  --remote-write-tenant-header X-Scope-OrgID
```

- Mimir tenant 默认通过 `X-Scope-OrgID` 转发；多 tenant 任务必须保留 tenant header。
- Remote Write v1.0 不支持 native histogram，遇到时 block 会失败，可能已有其他 worker 的 float 请求送达。
- 接收端请求与本地 checkpoint 非事务；进程在请求成功后、checkpoint 写入前退出时，恢复会重放当前单元。

## 6. 故障恢复

### 6.1 暂停 / 停止 / 崩溃

- 暂停/停止在**单元边界**生效：当前单元的所有 worker 请求返回后才写 checkpoint。
- checkpoint 记录 `completed_unit` 与 `next_offset`；恢复时从 `next_offset` 继续遍历 index。
- 接收端写入与 checkpoint 非事务：如果请求已成功、checkpoint 尚未落盘时进程退出，恢复会重发当前单元；接收端通常按相同 series/时间戳幂等处理，但不保证 exactly-once。
- 进程被 `kill -9`、机器断电、容器重启都适用；服务端重启会把 running 任务恢复为 `paused(interrupted)`。

### 6.2 已完成块

- `blocks/[tenant=<tenant>/]block=<ULID>/_complete.json` 是完成标记；默认跳过。
- `--force` / API 新建任务并指定同一输出目录（需先删除旧任务记录）会重新发送该块。
- 本地目录不保存样本数据；删除任务记录不会撤回已写入接收端的数据。

### 6.3 CRC / 解码错误

默认行为：该 Block 失败，其余继续（`continue_on_error=true`）。

- `--skip-bad-chunks`：记录错误并跳过坏 chunk（可能包含该 chunk 已解码的前缀样本，属于 best-effort）。
- `--no-verify-crc`：跳过 CRC 校验以提速；不建议用于长期归档。
- 错误计数在 `blocks/:id` 的 `crc_errors` / `decode_errors` / `unknown_encodings`。

### 6.4 常见问题

| 现象 | 原因 | 处理 |
|------|------|------|
| `no Prometheus TSDB blocks found` | 目录是 WAL/head，或路径错 | 确认存在 `<ULID>/meta.json`；先让 Prometheus 完成 compact |
| `unknown index file version 4` | 未来格式 | 升级工具；先不要迁移 |
| `chunk encoding 7` | 新编码 | 升级工具；`unknown_encodings` 计数 |
| `checksum mismatch: chunk` | 磁盘损坏/拷贝不完整 | 校验源盘；备份后重试 |
| `outside the allowed roots` | 路径不在 `--allow-root` | 增加 allow-root 或改路径 |
| 暂停很久不生效 | 当前 unit 很大 | 调小 `--unit-series` |
| 任务恢复后进度从 0 开始 | 引擎未 hydrate 或 checkpoint 缺失 | 查看 `.state/blocks/<ULID>.json` |

---

## 7. Grafana Mimir 对象存储数据源

### 7.1 布局与 tenant 规则

```
s3://<bucket>/<storage-prefix>/<tenant>/<ULID>/{meta.json,index,tombstones,chunks/...}
```

- tenant 是 bucket/prefix 下的第一级目录；同一 ULID 可以出现在多个 tenant。
- 本地完成标记位于 `blocks/tenant=<tenant>/block=<ULID>/_complete.json`；样本数据通过 Remote Write 发送。
- checkpoint 为 `.state/blocks/<tenant>/<ULID>.json`，不同 tenant 互不覆盖。
- `bucket-index.json`、`markers/`、`debug/` 等控制面对象会被忽略；`deletion-mark.json` 标记的块跳过。

### 7.2 CLI

```bash
# 发现 tenant
tsdb-migrate inspect --mimir-bucket bucketA --mimir-prefix mimir --mimir-region us-east-1

# 迁指定 tenant（可重复 / 逗号分隔）
tsdb-migrate migrate \
  --mimir-bucket bucketA --mimir-prefix mimir --mimir-region us-east-1 \
  --mimir-tenant tenant-a --mimir-tenant tenant-b \
  --out /var/lib/tsdb-migrate/mimir-job \
  --remote-write-url https://receiver.example/api/v1/write \
  --remote-write-tenant-header X-Scope-OrgID --block-parallel 1 --decode-workers 4

# 迁全部 tenant（省略 --mimir-tenant）
tsdb-migrate migrate \
  --mimir-bucket bucketA --mimir-prefix mimir \
  --out /var/lib/tsdb-migrate/mimir-job \
  --remote-write-url https://receiver.example/api/v1/write --block-parallel 1

# MinIO / S3 兼容
tsdb-migrate migrate \
  --mimir-bucket mimir-blocks --mimir-endpoint http://127.0.0.1:9000 \
  --mimir-allow-http --mimir-path-style \
  --out /var/lib/tsdb-migrate/mimir-job \
  --remote-write-url https://receiver.example/api/v1/write

# 本地目录模拟 bucket（离线测试 / NFS 镜像）
tsdb-migrate migrate --mimir-local-root /mnt/mimir-mirror \
  --out /var/lib/tsdb-migrate/mimir-job \
  --remote-write-url https://receiver.example/api/v1/write
```

凭证：

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...      # 可选
export AWS_REGION=us-east-1
```

凭证不会写入任务状态；服务端/CLI 进程需要能读取上述环境变量。

最小 IAM 权限（bucket `bucketA`，prefix `mimir/`）：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::bucketA",
      "Condition": {"StringLike": {"s3:prefix": ["mimir/*"]}}
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::bucketA/mimir/*"
    }
  ]
}
```

### 7.3 缓存与资源

- 每个并发 block 会下载一个完整 block 到 `<out>/.cache/<tenant>/<ULID>/`，完成后默认删除。
- 磁盘预留 ≥ `block_parallel × 最大块大小`；典型 2h block 几十 MB~数 GB。
- `--mimir-keep-cache` 保留缓存，便于重跑/调试；会持续占用磁盘。
- 下载并发由 `--mimir-downloads`（默认 8）控制，受 `block_parallel` 限制。
- 对象存储延迟高时，可适当提高 `max_concurrent_downloads`；内存不随下载并发线性增长（流式写盘）。
- 下载按 `--mimir-part-mb`（默认 256 MiB）分片 + 断点续传；失败后缓存目录保留，重跑自动补齐。
- 单次 GET 默认**不设总超时**（`--mimir-timeout-secs 0`），只保留 `--mimir-read-timeout-secs`（默认 120s）
  的停滞检测。若必须限制单请求时长（例如限制总带宽的代理），注意它覆盖整个响应体，
  大对象会被腰斩成 `request or response body error`。
- 连续零进展 `--mimir-download-retries`（默认 20）次后才放弃该对象。

### 7.4 API / Web

- `POST /api/mimir/tenants` 发现 tenant（只 LIST，不下载）。
- Web 新建任务选择「Grafana Mimir 对象存储（S3）」→ 填 bucket/prefix/region/endpoint → 扫描 tenant → 勾选 → 创建。
- 任务详情按块展示 tenant；配置面板显示 bucket/prefix 与实际 tenant 过滤。

### 7.5 Remote Write tenant 路由

- Mimir tenant 默认通过 `X-Scope-OrgID` 转发；可用 `--remote-write-tenant-header` 修改 header 名称。
- 多 tenant 任务留空 tenant header 会被拒绝，避免将不同 tenant 的数据合并。
- 验证迁移结果应在 Remote Write 接收端按 tenant 检查样本与时间范围。

### 7.6 一致性

对象存储 LIST 是最终一致的；迁移期间 Mimir 可能刚写完块但对象尚不可读。下载失败会使该块失败，重新执行同一任务（resume）会从 checkpoint 继续；未完成的缓存目录不会被视为有效块。

## 8. 监控

- `GET /api/health`：服务存活、任务计数。
- `GET /api/jobs/:id`：进度、速率、ETA、错误。
- `GET /api/jobs/:id/blocks`：单块进度与错误。
- `GET /api/jobs/:id/logs`：最近 2000 行日志。
- `manifest.json`：任务结束后的权威统计。

建议告警：

| 指标 | 阈值 |
|------|------|
| `state == failed` | 立即 |
| `totals.crc_errors + decode_errors > 0` | 立即 |
| `rate.samples_per_sec == 0` 且状态 running 超过 5 分钟 | 检查是否卡住 |
| Mimir 缓存目录可用空间 < 10% | 扩容/清理缓存 |

---

## 9. Prometheus 侧准备

1. 确认目标数据已经落成 Block（`data/<ULID>/`），而不是只在 head/WAL。
2. 建议先完成 compaction，减少重叠块与碎片。
3. 迁移期间 Prometheus 可以继续运行；本工具只读打开文件（mmap），不会修改块。
4. 如果 Prometheus 正在删除/压缩块，可能出现文件消失或部分读取失败；建议对数据目录做快照，或在低峰期迁移。
5. 保留原始块直到 Remote Write 接收端的数据校验通过。
