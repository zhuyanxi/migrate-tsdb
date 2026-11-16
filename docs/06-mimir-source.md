# Grafana Mimir 数据源接入说明

> 本文专门说明 Mimir 对象存储数据源：如何接入、数据如何定位，以及 tenant 如何通过 Remote Write 路由。
>
> 通用格式见 `02-format-notes.md` §9；部署/凭证见 `04-operations.md` §7；Remote Write 协议见 `08-remote-write-sink.md`。

---

## 1. 接入方式

### 1.1 数据源类型

| 类型 | 配置 | 用途 |
|------|------|------|
| `s3`（默认） | `bucket` + `prefix` + `region` + 可选 `endpoint` | AWS S3 / MinIO / Ceph / LocalStack |
| `local` | `bucket` = 本地目录 | 离线镜像、NFS 挂载、CI 测试；布局与 S3 完全相同 |

S3 凭证**只从进程环境变量读取**，不进入 API/job 状态文件：

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN       # 可选
AWS_REGION              # 可被 mimir.region 覆盖
```

MinIO / S3 兼容端点额外设置：

```json
{
  "backend": "s3",
  "bucket": "mimir-blocks",
  "prefix": "mimir",
  "endpoint": "http://127.0.0.1:9000",
  "allow_http": true,
  "force_path_style": true
}
```

### 1.2 配置结构

```rust
pub struct MimirConfig {
    pub backend: MimirBackend,   // s3 | local
    pub bucket: String,
    pub prefix: String,          // Mimir storage-prefix
    pub region: String,          // default us-east-1
    pub endpoint: String,        // 空 = AWS 默认
    pub tenants: Vec<String>,    // 空 = 全部
    pub cache_dir: String,       // 空 = <output>/.cache
    pub keep_cache: bool,
    pub max_concurrent_downloads: usize, // 1..=64, default 8
    pub download_part_mb: u64,           // 1..=4096, default 256
    pub download_timeout_secs: u64,      // 0 = unlimited, default 0
    pub download_read_timeout_secs: u64, // 0 = unlimited, default 120
    pub download_retries: usize,         // 1..=1000, default 20
    pub allow_http: bool,
    pub force_path_style: bool,
}
```

`JobConfig` 中：

```json
{
  "data_dir": null,
  "mimir": { "...": "MimirConfig" },
  "output_dir": "/mnt/export"
}
```

`data_dir` 与 `mimir` **二选一**；同时提供返回 400。

---

## 2. 数据定位规则

### 2.1 对象布局

```
s3://<bucket>/<prefix>/<tenant>/<ULID>/
  meta.json
  index
  tombstones
  chunks/000001
  chunks/000002
```

- `prefix` 可空；规范化后保证单个尾斜杠。
- `tenant` 是第一级目录，必须匹配 `[A-Za-z0-9._-]{1,128}`。
- `ULID` 是 26 位大写 Crockford Base32。
- 必须存在 `meta.json`、`index`、至少一个 `chunks/*`；否则不是可迁移块。
- 忽略 `bucket-index.json`、`markers/`、`debug/`、非 ULID 目录。
- 出现 `deletion-mark.json` 的块视为待删除，计划阶段跳过并告警。
- 同一 ULID 允许出现在多个 tenant；内部 key 为 `tenant/ULID`。

### 2.2 发现流程

```
list_with_delimiter(<prefix>/)           → tenant 列表
  for each tenant:
    list(<prefix>/<tenant>/) 流式遍历     → 按 ULID 聚合对象大小/计数
    并发 GET meta.json（16）              → minTime/maxTime/stats
    应用 tenant 过滤 / block 白名单 / 时间范围
  → Plan { blocks: [{ tenant, ulid, cache_path, meta }] }
```

- tenant 发现只做一次 delimiter LIST；不会读取 bucket index。
- 对象清单**流式分组**，内存只保留每个 block 的摘要，不保留全部对象 key。
- `meta.json` 读取并发 16；失败只跳过该块并记录 warning。

### 2.3 下载与缓存

```
<output>/.cache/<tenant>/<ULID>/
  meta.json, index, tombstones, chunks/...    # 完成后才出现
  index.tsdb-part, chunks/000001.tsdb-part     # 下载中的对象（可续传）
  .tsdb-migrate-ready                         # 所有对象写完后写入
```

- 每个 block 独立下载；并发由 `max_concurrent_downloads` 控制，且受 `block_parallel` 限制。
- 暂停/停止在单元边界生效；如果正在下载一个块，会等该块下载完成后再进入暂停/停止。
- **对象按 `download_part_mb`（默认 256 MiB）分片，用 Range GET 拉取**，先写到
  `<name>.tsdb-part`，完成后 rename 成正式文件名：文件要么完整、要么不出现。
- **任何中断都从已落盘的字节续传**：body 流断开、请求停滞、响应体短于 Content-Range、
  进程被杀，都只是让下一次尝试从当前偏移继续，不会重传整个对象。
- `download_retries` 统计的是**连续零进展**的失败次数；只要有 1 个字节进展就清零，
  所以慢链路一路续传不会被判失败。
- 每个对象已完整（大小匹配）时直接跳过，因此失败后重跑只补齐缺的部分。
- **下载失败不删除缓存目录**，错误信息会带 `partial download kept in <dir>; rerun to resume`。
- 块迁移完成后默认删除缓存；`keep_cache=true` 保留。
- 暂停/停止/失败时保留缓存，便于 resume 复用。

#### 为什么必须关掉默认的 30s 请求超时

object_store 的 `ClientOptions::timeout` 默认 **30 秒**，且覆盖**整个响应体**
（reqwest 的 `TotalTimeoutBody`）。单次 GET 的 body 超过 30 秒就报：

```
Generic S3 error: HTTP error: request or response body error
```

object_store 对 body 流的内部重试还受 `RetryConfig::retry_timeout`（默认 180 秒，从首次
请求起算）限制，超预算后错误直接上抛。于是 20 GiB 的 `index` 在默认参数下只有在
「约 150 秒内推完 20 GiB」时才能成功，否则必然失败；512 MiB 的 chunk 不受影响。

本实现因此：

- `download_timeout_secs = 0`（默认）→ `ClientOptions::with_timeout_disabled()`；
  需要硬上限时设为秒数。
- 用 `download_read_timeout_secs`（默认 120）代替总超时，只掐「连续 N 秒没有数据」的停滞连接。
- `RetryConfig { max_retries, retry_timeout: 3600s }`，把恢复责任交给上面的分片循环。

---

## 3. 各层改动范围

| 层 | 文件 | 改动 |
|----|------|------|
| 配置 | `migrate-core/src/config.rs` | 新增 `MimirConfig` / `MimirBackend`；`JobConfig.data_dir` 可选、新增 `mimir`；校验 tenant/prefix/cache |
| 数据源 | `migrate-core/src/mimir.rs` | S3/本地后端、tenant 发现、流式清单、meta 并发读取、按块下载/清理 |
| 规划 | `migrate-core/src/engine.rs` | 有 `mimir` 时构造 `MimirSource` 并调用 `source.plan()`；本地走原 `planner::plan()` |
| Block 身份 | `tsdb-reader/src/meta.rs` | `BlockInfo` 增加 `tenant`/`remote`/`key()`；本地源填空 |
| 统计 | `migrate-core/src/stats.rs` | `BlockStats`/`BlockSnapshot` 增加 key/tenant；`Totals.tenants` |
| 本地标记 | `migrate-core/src/sink/mod.rs` | Mimir 路径 `blocks/tenant=<t>/block=<ulid>/_complete.json`；本地保持 `blocks/block=<ulid>/_complete.json` |
| Checkpoint | `migrate-core/src/checkpoint.rs` | `tenant` 字段；路径 `state/blocks/<tenant>/<ulid>.json`；本地路径不变 |
| 执行 | `migrate-core/src/engine.rs` | block 开始前 `materialize`；完成后 `cleanup`；失败/暂停保留缓存 |
| CLI | `tsdb-migrate-cli/src/main.rs` | `--mimir-*` 参数；inspect/plan/migrate 支持 Mimir，Remote Write 按 tenant header 路由 |
| API | `tsdb-migrate-server/src/api.rs` | `/api/mimir/tenants`；create 支持 `mimir`/可选 `data_dir`；路径校验 |
| Web | `web/*` | 数据源切换、tenant 扫描/勾选、tenant 列、配置展示 |

**未改动**：`tsdb-reader` 的 index/chunks/chunkenc/tombstones 解析逻辑完全复用。Mimir block 下载后就是一个标准 Block 目录。

---

## 4. 与本地目录源的兼容性

| 维度 | 本地目录源 | Mimir 源 |
|------|-----------|----------|
| 配置 | `data_dir` | `mimir` |
| Block 定位 | `data_dir/<ULID>` | `<prefix>/<tenant>/<ULID>` |
| 内部 key | `<ULID>` | `<tenant>/<ULID>` |
| 本地完成标记 | `blocks/block=<ULID>/_complete.json` | `blocks/tenant=<t>/block=<ULID>/_complete.json` |
| checkpoint | `.state/blocks/<ULID>.json` | `.state/blocks/<tenant>/<ULID>.json` |
| Remote Write tenant | 不设置 tenant header | 默认发送 `X-Scope-OrgID` |
| 缓存 | 无 | `<out>/.cache`，完成后清理 |
| CLI | `--data-dir` | `--mimir-bucket` 或 `--mimir-local-root` |
| API | `data_dir` | `mimir` |
| 已有任务/状态 | 不变 | 新增字段有 serde default，可读旧 manifest/checkpoint |

本地源与 Mimir 源都只在本地保存 checkpoint、manifest 和完成标记；样本数据统一写入 Remote Write 接收端。

---

## 4.1 Remote Write tenant 路由

- Mimir tenant 默认通过 `X-Scope-OrgID` header 分别写入接收端的 tenant。
- 单 tenant 任务可以将 `remote_write_tenant_header` 留空；多 tenant 任务留空会被拒绝，避免 tenant 数据合并。
- Remote Write v1.0 不支持 native histogram，遇到时 block 会失败而不静默丢弃。

## 5. 并发、内存与 IO

- 对象下载与块解码是**串行阶段**：一个块先完整下载，再进入现有 pipeline；Remote Write 将 `block_parallel` 固定为 1 以保证 tenant 内样本时间顺序。
- 下载缓存占用的是**磁盘**，不是堆内存；单块下载的并发流有界，内存 ≈ 并发流 buffer。
- 对象存储延迟高时，提高 `max_concurrent_downloads` 可以缩短单块下载时间；处理块的并行度由 Remote Write 顺序约束固定为 1。
- Mimir 的 LIST 顺序不作为迁移顺序依赖；引擎按 tenant 和时间排序，并拒绝同 tenant 时间范围重叠的块。

---

## 6. 失败与恢复

| 场景 | 行为 |
|------|------|
| tenant 列表为空 | 任务创建失败，提示检查 bucket/prefix/凭证 |
| 单块 meta.json 缺失/损坏 | 跳过该块，warning；其余继续 |
| 下载对象缺失/403 | 该块失败，删除缓存目录；可 resume 重试 |
| 迁移中断 | 已完成单元 checkpoint 保留；缓存保留；resume 复用 |
| 块完成 | 写 `_complete.json`；删除缓存（默认） |
| 源对象删除（deletion-mark） | 计划阶段跳过；不导出 |
| S3 限流 | 当前失败该块；按指数退避重试未实现（见已知限制） |

---

## 7. 已知限制

1. 不做**远程 range 读取**：必须整块下载；对超大块需要相应磁盘。
2. 不解析 Mimir **bucket index**：超大 bucket 首次 LIST 较慢，并产生 LIST 请求费用。
3. 没有 S3 客户端级重试/限流退避；依赖 `object_store` 默认重试，失败由任务重跑。
4. tenant 字符集限制 `[A-Za-z0-9._-]`；非常规 tenant ID 会被忽略。
5. `meta.json` 无 stats 的老块：进度总量为 0，只能看 samples/日志；建议升级 Mimir 或预扫描。
6. 真实 AWS S3 端到端压测待补；本地模拟覆盖了布局、tenant 隔离、下载、续传。
