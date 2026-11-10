# HTTP API 契约

Base URL：`http://<host>:<port>`，默认 `http://127.0.0.1:8080`。

所有请求/响应均为 `application/json; charset=utf-8`（静态资源除外）。

---

## 1. 鉴权

服务端启动时设置 `--token` 或环境变量 `TSDB_MIGRATE_TOKEN` 后：

- `/api/health` 与静态资源（`/`、`/app.js`、`/styles.css`）无需鉴权；
- 其余 `/api/*` 需要：

```
Authorization: Bearer <token>
```

失败返回：

```json
HTTP/1.1 401 Unauthorized
{"error":"missing or invalid bearer token"}
```

未设置 token 时，所有接口开放（仅建议在受信网络使用）。

---

## 2. 路径安全

`data_dir` / `output_dir` 都会做 canonicalize，并必须位于启动参数 `--allow-root`（可重复，默认当前工作目录）之下。越界返回 `403`：

```json
{"error":"data_dir /private/etc is outside the allowed roots","status":403}
```

- `data_dir` 必须存在且是目录；
- `output_dir` 可以不存在，会向上找到已存在的祖先目录做校验后拼接剩余路径。

---

## 3. 状态机

```
Created ──start──▶ Running ──pause──▶ Pausing ──(所有 block 到达检查点)──▶ Paused
                     │  ▲                  │                                  │
                     │  └──────resume──────┴──────────────────────────────────┘
                     │
                     ├──stop──▶ Stopping ──(当前单元提交)──▶ Stopped ──resume──▶ Running
                     │
                     └──全部 block 完成──▶ Completed
                     └──有 block 失败────▶ Failed
```

- `Pausing` / `Stopping` 是过渡态：引擎在**当前迁移单元**完成并写 checkpoint 后才真正停下，最坏延迟约等于一个单元的解码时间。
- `Stopped` 可以再次 `resume`（服务端会重新拉起引擎线程，从 checkpoint 继续）。
- `Completed` / `Failed` 不能 resume；需要新建任务（或 CLI 使用 `--force`）。

---

## 4. 接口

### 4.1 健康检查

```
GET /api/health
```

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_secs": 128,
  "jobs": {"total": 3, "running": 1, "paused": 0, "completed": 2, "failed": 0}
}
```

### 4.2 扫描 Block

```
GET /api/blocks?data_dir=<path>&scan=<bool>&limit=<n>
```

- `data_dir`：必需。Prometheus data 目录或单个 Block 目录。
- `scan=true`：当 `meta.json` 没有 stats 时打开 index 做结构扫描；即使有 stats 也强制扫描以获得精确值。
- `limit`：默认 5000，最大 20000（防止误扫超大目录）。

```json
{
  "blocks": [
    {
      "ulid": "01HXXXXXXXXXXXXXXXXXXXXXXX",
      "path": "/prometheus/data/01H...",
      "min_time": 1700000000000,
      "max_time": 1700022485001,
      "min_time_iso": "2023-11-14T22:13:20.000Z",
      "max_time_iso": "2023-11-15T04:28:05.001Z",
      "series": 42,
      "chunks": 100,
      "samples": 4951,
      "chunks_bytes": 13432,
      "index_bytes": 5525,
      "tombstones_bytes": 22,
      "warnings": []
    }
  ],
  "total": 1,
  "scanned": true
}
```

#### Mimir 数据源示例

`data_dir` 与 `mimir` 二选一：

```json
{
  "name": "mimir-prod",
  "output_dir": "/mnt/duckdb/mimir",
  "mimir": {
    "backend": "s3",
    "bucket": "bucketA",
    "prefix": "mimir",
    "region": "us-east-1",
    "endpoint": "",
    "tenants": ["tenant-a", "tenant-b"],
    "cache_dir": "",
    "keep_cache": false,
    "max_concurrent_downloads": 8,
    "allow_http": false,
    "force_path_style": false
  },
  "filter": {"min_time": null, "max_time": null, "matchers": []},
  "sink": {"kind": "parquet", "compression": "zstd"},
  "options": {"block_parallel": 2, "decode_workers": 4, "unit_series": 50000}
}
```

- `backend`: `s3` | `local`。`local` 时 `bucket` 是服务端可见的目录路径（用于测试/离线镜像），同样必须位于 `--allow-root` 下。
- `tenants` 为空表示迁移该 bucket/prefix 下发现的全部 tenant。
- S3 凭证只从服务端环境变量读取（`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_REGION`），不会出现在 API 请求或 job 状态中。
- `cache_dir` 为空时使用 `<output_dir>/.cache`；建议与输出同盘，至少预留 `block_parallel × 单块大小` 空间。

### 4.3 Mimir Tenant 发现

```
POST /api/mimir/tenants
Content-Type: application/json
```

请求体就是 `mimir` 配置（`tenants` 会被忽略，始终发现全部）：

```json
{
  "backend": "s3",
  "bucket": "bucketA",
  "prefix": "mimir",
  "region": "us-east-1",
  "endpoint": "",
  "allow_http": false,
  "force_path_style": false
}
```

```json
{
  "elapsed_ms": 842,
  "tenants": [
    {"id": "tenant-a", "blocks": 128, "objects": 14208, "bytes": 7340032000},
    {"id": "tenant-b", "blocks": 64, "objects": 7104, "bytes": 3670016000}
  ]
}
```

- 该接口只做对象 LIST，不下载块；用于 UI 展示/选择 tenant。
- `local` 后端同样可用（bucket 为目录）。
- 大 bucket 的 LIST 可能耗时数秒到数十秒，并可能产生 S3 请求费用。

### 4.4 任务列表

```
GET /api/jobs
```

返回 `JobSnapshot[]`，按开始时间倒序。字段见 §5.1。

### 4.5 创建任务

```
POST /api/jobs
Content-Type: application/json
```

```json
{
  "name": "prod-2026-11",
  "data_dir": "/prometheus/data",
  "output_dir": "/mnt/duckdb/export",
  "blocks": ["01H...", "01H..."],
  "filter": {
    "min_time": 1700000000000,
    "max_time": 1700022485001,
    "matchers": [{"name": "job", "value": "api"}]
  },
  "sink": {
    "kind": "parquet",
    "compression": "zstd",
    "row_group_size": 65536,
    "duckdb_path": "",
    "duckdb_exec": true
  },
  "options": {
    "block_parallel": 2,
    "decode_workers": 4,
    "batch_samples": 65536,
    "batch_bytes": 33554432,
    "unit_series": 50000,
    "channel_capacity": 2048,
    "verify_crc": true,
    "mmap_advise": true,
    "skip_bad_chunks": false,
    "resume": true,
    "force": false,
    "continue_on_error": true
  }
}
```

- `blocks` 为空表示全部块。
- `filter.matchers` 是精确匹配，AND 关系；暂不支持正则。
- `sink.kind`：`parquet` | `ndjson` | `duckdb`。`duckdb` = 先写 Parquet，完成后调用 `duckdb` CLI 装载。
- 创建时会同步执行 planning（扫描 `meta.json` / 必要时 index），返回时 `engine_ready=true`。
- 同一个 `output_dir` 不允许被多个任务同时使用（避免 part/checkpoint 冲突）。

响应：`JobDetail`（§5.2），HTTP 200。

错误：

| 场景 | 状态码 | `error` 示例 |
|------|--------|-------------|
| 参数缺失/非法 | 400 | `data_dir is required` |
| 路径越界 | 403 | `output_dir ... outside the allowed roots` |
| 目录不存在 / 无 Block | 400 | `no Prometheus TSDB blocks found under ...` |
| 输出目录被占用 | 409 | `output_dir already used by another job` |

### 4.6 任务详情

```
GET /api/jobs/{id}
```

返回 `JobDetail`。若任务是从磁盘恢复且尚未构建引擎，会在此请求内完成 planning + 进度回填（可能耗时数百毫秒到数秒）。

### 4.7 任务动作

```
POST /api/jobs/{id}/actions
Content-Type: application/json

{"action": "start" | "pause" | "resume" | "stop"}
```

| action | 允许状态 | 行为 |
|--------|---------|------|
| `start` | `created` / `stopped` | 拉起引擎线程，从 checkpoint 继续 |
| `pause` | `running` | 置 `pausing`；所有 block 在下一个单元提交后停 |
| `resume` | `paused` / `pausing` / `stopped` | 取消暂停或重新拉起引擎 |
| `stop` | `running` / `pausing` / `paused` | 置 `stopping`；提交当前单元后退出 |

非法状态返回 `409`：

```json
{"error":"cannot pause from state completed","status":409}
```

### 4.8 分块进度

```
GET /api/jobs/{id}/blocks?status=<s>&offset=<n>&limit=<n>&sort=<min_time|progress|status>
```

```json
{
  "total": 24,
  "offset": 0,
  "limit": 100,
  "blocks": [
    {
      "key": "tenant-a/01H...",
      "ulid": "01H...",
      "tenant": "tenant-a",
      "path": "/prometheus/data/01H...",
      "min_time": 1700000000000,
      "max_time": 1700022485001,
      "status": "running",
      "error": null,
      "series_total": 5002,
      "series_done": 1500,
      "chunks_total": 60060,
      "chunks_done": 18000,
      "samples_expected": 7203965,
      "floats": 2150000,
      "histograms": 0,
      "samples_skipped_tombstone": 41,
      "samples_skipped_range": 0,
      "series_skipped_filter": 0,
      "units_total": 101,
      "units_done": 30,
      "current_unit": 30,
      "bytes_read": 1200000,
      "bytes_written": 800000,
      "crc_errors": 0,
      "decode_errors": 0,
      "unknown_encodings": 0,
      "started_at": "2026-11-09T14:31:41.000Z",
      "finished_at": null,
      "duration_ms": 1200
    }
  ]
}
```

`status`：`pending` | `running` | `paused` | `completed` | `failed` | `skipped`。

`tenant` 对本地目录源为 `null`；Mimir 源为对应 tenant。`key` 是内部唯一键（`tenant/ulid` 或 `ulid`）。

### 4.9 日志

```
GET /api/jobs/{id}/logs?after=<seq>&limit=<n>
```

```json
{
  "last_seq": 128,
  "entries": [
    {"seq": 127, "ts": "2026-11-09T14:31:41.123Z", "level": "info", "msg": "block ...: 开始（series=5002 chunks=60060 ...）"}
  ]
}
```

- 内存环形缓冲，容量 2000 行；服务重启后清空。
- `after` 用于增量拉取；返回 `seq > after` 的前 `limit` 条。
- `level`：`debug` | `info` | `warn` | `error`。

### 4.10 删除任务记录

```
DELETE /api/jobs/{id}
```

- 仅允许非活动任务（引擎线程已退出，且状态不在 running/pausing/stopping）。
- 只删除 `state/jobs/<id>.json` 与内存记录，**保留输出数据**。
- 响应：`{"deleted":"<id>","output_kept":true}`。

---

## 5. 数据结构

### 5.1 JobSnapshot

```json
{
  "job_id": "ea97e8c1fe5d",
  "name": "api-bench",
  "state": "running",
  "error": null,
  "data_dir": "/prometheus/data",
  "output_dir": "/mnt/duckdb/export",
  "started_at": "2026-11-09T14:31:40.238Z",
  "elapsed_ms": 12034,
  "totals": {
    "series_total": 5002,
    "series_done": 1500,
    "chunks_total": 60060,
    "chunks_done": 18000,
    "samples_expected": 7203965,
    "floats": 2150000,
    "histograms": 0,
    "samples": 2150000,
    "samples_skipped_tombstone": 41,
    "samples_skipped_range": 0,
    "series_skipped_filter": 0,
    "units_total": 101,
    "units_done": 30,
    "bytes_read": 1200000,
    "bytes_written": 800000,
    "crc_errors": 0,
    "decode_errors": 0,
    "unknown_encodings": 0
  },
  "blocks_total": 1,
  "blocks_completed": 0,
  "blocks_failed": 0,
  "blocks_skipped": 0,
  "blocks_running": 1,
  "blocks_pending": 0,
  "rate": {
    "samples_per_sec": 182000.0,
    "bytes_per_sec": 3355443.0,
    "chunks_per_sec": 1500.0,
    "series_per_sec": 120.0,
    "window_secs": 27.0
  },
  "eta_seconds": 42,
  "options": { "...": "同创建请求" },
  "sink_kind": "parquet"
}
```

`elapsed_ms` 在任务进入 `completed` / `failed` / `stopped` 后冻结（不再随时间增长）。

### 5.2 JobDetail

```json
{
  "job": { "...": "JobSnapshot" },
  "config": { "...": "JobConfig（含 data_dir 或 mimir / output_dir / filter / sink / options）" },
  "interrupted": false,
  "engine_alive": true,
  "engine_ready": true,
  "logs_last_seq": 128
}
```

- `interrupted=true`：服务重启时任务处于 running/pausing/stopping，被恢复为 paused，需要手动 resume。
- `engine_ready=false`：planning 尚未完成（通常只在刚恢复的极短时间内出现）。

---

## 6. 错误格式

所有非 2xx 响应：

```json
{"error": "human readable message", "status": 400}
```

| 状态码 | 含义 |
|--------|------|
| 400 | 参数/路径/planning 失败 |
| 401 | 缺少或错误的 Bearer token |
| 403 | 路径越界 |
| 404 | 任务不存在 / 静态资源不存在 |
| 409 | 状态冲突 / 输出目录被占用 |
| 500 | 持久化失败等内部错误 |

---

## 7. curl 速查

```bash
# 健康
curl -s localhost:8080/api/health

# 扫描本地块
curl -s "localhost:8080/api/blocks?data_dir=/prometheus/data&scan=true"

# 发现 Mimir tenant
curl -s -X POST localhost:8080/api/mimir/tenants -H 'content-type: application/json' \
  -d '{"backend":"s3","bucket":"bucketA","prefix":"mimir","region":"us-east-1"}' 

# 创建
curl -s -X POST localhost:8080/api/jobs -H 'content-type: application/json' -d @job.json

# 启动 / 暂停 / 恢复 / 停止
curl -s -X POST localhost:8080/api/jobs/<id>/actions -H 'content-type: application/json' -d '{"action":"start"}'
curl -s -X POST localhost:8080/api/jobs/<id>/actions -H 'content-type: application/json' -d '{"action":"pause"}'
curl -s -X POST localhost:8080/api/jobs/<id>/actions -H 'content-type: application/json' -d '{"action":"resume"}'
curl -s -X POST localhost:8080/api/jobs/<id>/actions -H 'content-type: application/json' -d '{"action":"stop"}'

# 进度 / 分块 / 日志
curl -s localhost:8080/api/jobs/<id>
curl -s "localhost:8080/api/jobs/<id>/blocks?status=completed&limit=50"
curl -s "localhost:8080/api/jobs/<id>/logs?after=0&limit=100"

# 鉴权版本
curl -s -H "Authorization: Bearer $TSDB_MIGRATE_TOKEN" localhost:8080/api/jobs
```
