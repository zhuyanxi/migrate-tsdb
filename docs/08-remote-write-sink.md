# Prometheus Remote Write v1.0 Sink

> 目标：把 TSDB Block 直接推到任意兼容 [Remote Write 1.0](https://prometheus.io/docs/specs/prw/remote_write_spec/) 的接收端（Prometheus、Mimir、Thanos、Cortex、VictoriaMetrics、GreptimeDB…），不落 Parquet。
>
> 实现：`crates/migrate-core/src/sink/remote_write.rs`。

---

## 1. 数据流

```
index / chunks / tombstones
        │  解码（不变）
        ▼
Batch { series[], floats[] }
        │  build_write_request
        ▼
WriteRequest { timeseries[] }  ──prost──▶  protobuf
        │  snap::raw::Encoder（block 格式）
        ▼
HTTP POST  ──▶  <remote_write_url>
```

- Parquet / DuckDB 链路完全不受影响；`sink.kind=remote_write` 时引擎会跳过 DuckDB 收尾。
- 一个 Batch 一次请求；请求体大小由 `options.batch_samples` / `options.batch_bytes` 控制。
- 客户端为**进程级共享**（`OnceLock<Client>`）：sink 是「每 block × 每 worker 一个」创建的，独占 client 会导致每个 block 重建连接池与线程池。

---

## 2. 协议对照

| 规范条款 | 实现 | 覆盖测试 |
|----------|------|----------|
| 编码为 `HTTP POST`，路径任意 | `client.post(url)` | `writes_a_spec_compliant_request` |
| `Content-Encoding: snappy` | 固定头 | 同上 |
| `Content-Type: application/x-protobuf` | 固定头 | 同上 |
| `X-Prometheus-Remote-Write-Version: 0.1.0` | 固定头 | 同上 |
| `User-Agent: <name & version>` | `tsdb-migrate/<crate version>` | 同上 |
| Snappy **block** 格式（禁止 framed） | `snap::raw::Encoder` | 同上（校验魔数 + 可解码） |
| Protobuf 3，`WriteRequest/TimeSeries/Label/Sample`，tag 1/2 | `prost` 结构体 | 独立 schema 解码（`tests/support/mod.rs`） |
| 时间戳为 int64 毫秒、值为 float64 | `i64` / `f64` | `handles_a_large_payload` |
| label 名 `[a-zA-Z_][a-zA-Z0-9_]*` | `is_valid_label_name` | `rejects_invalid_label_names` |
| metric 名 `[a-zA-Z_:][a-zA-Z0-9_:]*` | `is_valid_metric_name`（只校验 `__name__`） | `rejects_invalid_metric_names` |
| label 名按字典序排列 | 发送前 `sort_by(name)` | `labels must be sorted` |
| 不得重复 label 名 | 排序后相邻比较 | `rejects_empty_label_values_and_duplicate_names` |
| 不得有空 label 名/值 | 显式拒绝 | 同上 |
| 同一 series 的样本按时间升序 | 请求内 `sort_by_key(ts)`；跨请求靠 block/unit 顺序 | `samples must be time ordered`、`sends_every_batch_in_order` |
| 2xx = 成功 | `is_success()` | `accepts_the_whole_2xx_family` |
| 5xx 必须重试 + 退避 | 指数退避 200ms→10s，5 次 | `retries_on_5xx_then_succeeds`、`gives_up_after_max_attempts_on_persistent_5xx` |
| 4xx（除 429）不得重试 | 立即失败 | `does_not_retry_on_client_errors` |
| 429 可重试，尊重 `Retry-After` | 支持秒数与 IMF-fixdate，下限 100ms、上限 30s | `retries_429_and_honours_retry_after`、`honours_a_future_retry_after_date`、`a_zero_retry_after_still_waits_before_retrying` |
| 响应体 MUST ignore（保留给未来） | 成功路径不读 body | `ignores_the_response_body` |
| 客户端不得允许配置保留头 | `validate_config` 拒绝协议头与 hop-by-hop 头 | `rejects_protocol_and_hop_by_hop_headers_as_tenant_header` |
| native histogram 不在 1.0 内 | 明确报错，不静默丢弃 | `rejects_native_histograms`、`reports_native_histograms_as_a_block_failure` |

---

## 3. 顺序保证

接收端拒绝 out-of-order 样本，所以「同一 series 的样本必须按时间递增到达」是硬约束。

| 粒度 | 保证方式 |
|------|----------|
| 单个请求内 | `samples.sort_by_key(timestamp)` |
| 单个 unit 内 | series 一次性进入同一 worker 的 batch；`EndUnit` 时整批 flush |
| 跨 unit | unit 之间 series 不重叠（`unit_series` 按 series 切分），因此同一 series 只属于一个 unit、一个 worker |
| 跨 worker | 不同 worker 只持有不同 series，可并发发送 |
| 跨 block | `block_parallel` 强制为 1（并打印 Warn），block 串行；`order_remote_write_blocks` 按 tenant + `min_time` 排序，并拒绝同一 tenant 内时间范围重叠的 block |
| 跨 block 的 sink 生命周期 | `run_block_pipeline` 的 `thread::scope` 会 join 所有 worker，block N 的请求全部返回后才开始 N+1 |

> 重叠检测：按 `min_time` 排序后，只要相邻两块不相交，整个序列就不相交（链式性质），因此无需记录全局最大 `max_time`。

**代价**：真实 Prometheus `data/` 目录常包含不同 compaction level 的重叠 block，此时 remote-write sink 会直接拒绝启动并提示用 `blocks` / `filter.min_time|max_time` 缩小范围——这是刻意选择：宁可拒绝，也不产生乱序写入。

---

## 4. 重试与错误语义

| 情况 | 行为 | 结果 |
|------|------|------|
| 2xx（含 204） | 成功 | batch 释放，写 checkpoint |
| 3xx | 不跟随重定向，立即失败 | block 失败 |
| 400/401/413 等 4xx | 立即失败，附带响应体前 2 KB | block 失败 |
| 429 | 按 `Retry-After`（否则指数退避）重试，上限 5 次 | 成功或失败 |
| 5xx | 指数退避重试，上限 5 次 | 成功或失败 |
| 连接失败 / 响应头被截断 | 指数退避重试，上限 5 次 | 成功或失败 |
| native histogram | 编码前即失败，不发送 | block 失败 |

重试不保证 exactly-once：请求已到接收端但响应丢失时，同一批会重发。Remote Write 接收端对「相同 series + 相同时间戳」是幂等的，因此实践上安全，但不能承诺「接收端恰好收到一次」。

---

## 5. 配置

| 位置 | 字段 | 说明 |
|------|------|------|
| CLI | `--sink remote-write` | 选择 sink |
| CLI | `--remote-write-url` | 完整 URL |
| CLI | `--remote-write-tenant-header` | 默认 `X-Scope-OrgID`；留空表示不发送 |
| API/配置 | `sink.kind = "remote_write"` | |
| API/配置 | `sink.remote_write_url` | |
| API/配置 | `sink.remote_write_tenant_header` | 默认 `X-Scope-OrgID` |
| 环境变量 | `TSDB_MIGRATE_REMOTE_WRITE_BEARER_TOKEN` | 仅服务端环境读取，不写入任务状态文件 |

Tenant 规则：

- 本地目录源无 tenant，不发送 tenant 头。
- Mimir 单 tenant 且未配置 header：放行并 Warn（数据会落到接收端默认 tenant）。
- Mimir **多 tenant** 且未配置 header：直接拒绝——否则多个 tenant 的数据会混成一路，无法再分开。

---

## 6. 限制

| 限制 | 说明 |
|------|------|
| 无 native histogram | v1.0 协议无该字段；遇直方图样本直接失败，避免静默丢数 |
| 不支持 metadata / exemplars | v1.0 未纳入规范 |
| 重叠 block 被拒绝 | 见 §3 |
| 本地无产物 | `blocks/` 下只写 `_complete.json`，`verify` 对 remote-write 任务没有意义 |
| 无持久化缓冲 | 进程退出后未发送的数据依赖 checkpoint 重放（unit 粒度） |
| 超时固定 30s | 大 batch 走慢链路时可能超时，可下调 `batch_bytes` 规避 |

---

## 7. 审查与修复记录

按严重程度列出本轮审查 + 测试发现的问题。

### 高

| # | 问题 | 影响 | 修复 |
|---|------|------|------|
| 1 | Mimir 多 tenant 且 tenant header 为空时静默混写 | 多个 tenant 的数据写入接收端同一命名空间，事后无法拆分 | 新增 `check_remote_write_tenants`：>1 tenant 且 header 为空 → 配置错误；单 tenant → Warn。测试 `rejects_multi_tenant_remote_write_without_a_tenant_header`、`warns_when_a_single_tenant_has_no_header` |

### 中

| # | 问题 | 影响 | 修复 |
|---|------|------|------|
| 2 | 每个 batch 都 `body.to_vec()` 复制整包 | 32 MB batch 时每请求多一份峰值内存，N 个 worker 叠加 | 改用 `bytes::Bytes`，重试零拷贝。测试 `handles_a_large_payload`（20k 样本） |
| 3 | 每个 block × 每个 worker 新建 `reqwest::Client` | 每个 block 重建连接池 + 后台线程，跨 block 无法复用连接 | 进程级 `OnceLock<Client>` 共享 |
| 4 | 429 且 `Retry-After: 0` 时退化为热循环 | 5 次重试在毫秒级打完，压低接收端 | `MIN_RETRY_AFTER = 100ms` 下限。测试 `a_zero_retry_after_still_waits_before_retrying` |

### 低

| # | 问题 | 影响 | 修复 |
|---|------|------|------|
| 5 | `Retry-After` 只解析整数秒 | 返回 HTTP-date 的接收端被忽略，退化为 200ms 退避，可能连续撞 429 | 用 `chrono` 解析 IMF-fixdate；过去时间按「立即重试」处理（仍受 100ms 下限）。测试 `honours_a_future_retry_after_date`、`treats_a_past_retry_after_date_as_retry_now` |
| 6 | 保留头黑名单不全 | 用户可把 tenant header 配成 `Connection`/`Transfer-Encoding`/自定义 `X-Prometheus-Remote-Write-*`，破坏请求 | 增加 hop-by-hop 头与 `x-prometheus-remote-write-*` 前缀拦截。测试 `rejects_protocol_and_hop_by_hop_headers_as_tenant_header` |
| 7 | 重叠报错信息不可操作 | 用户不知道该选哪个 block | 错误信息带上两个 ULID，并提示用 `blocks` / `filter.min_time|max_time`。测试 `overlap_error_names_both_blocks` |

### 确认无需修改

| 项 | 结论 |
|----|------|
| 响应体被截断时算成功 | 规范要求客户端忽略响应体，2xx 状态已表明接收成功 |
| `Retry-After` 上限 30s 截断 | 服务端要求更久时提前重试只会再拿一次 429，不丢数据 |
| 传递性重叠检测 | 按 `min_time` 排序 + 相邻比较等价于全序不重叠（测试 `rejects_transitively_overlapping_blocks` 固定该性质） |
| 空 label 值 / 非法名字抛错而非跳过 | 规范禁止发送，且静默跳过会造成难以察觉的数据丢失 |

---

## 8. 测试

```bash
# sink 层（mock 接收端，28 项）
cargo test -p migrate-core --test remote_write_sink

# 引擎层（真实 Block → 流水线 → HTTP，4 项）
cargo test -p migrate-core --test remote_write_e2e

# block 排序 / 重叠 / tenant 规则（9 项）
cargo test -p migrate-core --lib engine::tests
```

覆盖矩阵：

| 类别 | 用例 |
|------|------|
| 正常写入 | 协议头齐全、label 排序、样本排序、2xx 族、多 batch 顺序、20k 样本单请求、UTF-8 label、空 batch 跳过、单连接复用 |
| 边界 | 请求体为 snappy block（校验魔数）、负时间戳、无 `__name__`、超长 label 值、past/future `Retry-After`、`Retry-After: 0` |
| 异常 | 5xx 重试成功/耗尽、4xx 不重试、429 重试、连接被拒、响应头截断、307 不跟随、native histogram、非法 label/metric 名、空值、重复 label、孤儿样本、非法 URL/凭据/保留头 |
| 引擎 | 真实 fixture 全链路（121 样本、时间严格递增）、histogram 导致 block 失败、重叠 block 启动前拒绝、`sink_kind` 快照、`block_parallel` 强制为 1 |
