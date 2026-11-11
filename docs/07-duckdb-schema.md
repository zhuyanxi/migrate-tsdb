# DuckDB 表结构进化：metric 宽表 + 按 tenant 分库

> 目标：最终 DuckDB 中**每个 metric 一张表**，表名为 metric name；每条 series 的每个 label 成为一列（列名 = label name）；每张表包含 `timestamp` 与数据值列。Prometheus 源使用单个 DuckDB 实例；Mimir 源为每个 tenant 生成一个独立 DuckDB 实例。
>
> **Parquet 导出、reader、chunk 解码、批量、checkpoint、暂停/恢复逻辑均未改动。** 改动集中在“… → DuckDB”的最终装载阶段。

---

## 1. 最终数据库布局

### 1.1 Prometheus / 本地目录源

```
<out>/
├── blocks/…                          # 原 Parquet 输出，不变
├── load_duckdb.sql
└── migration.duckdb                  # 单个 DuckDB 实例
```

### 1.2 Mimir 源

```
<out>/
├── blocks/tenant=<tenant>/…           # 原 Parquet 输出，不变
├── load_duckdb_<tenant>.sql
└── duckdb/
    ├── tenant-a.duckdb                # 每个 tenant 独立实例
    └── tenant-b.duckdb
```

- `sink.duckdb_path` 为空时使用上述默认路径。
- Prometheus：`sink.duckdb_path` 可指定 `.duckdb` 文件。
- Mimir：`sink.duckdb_path` 应为目录；仅单 tenant 时允许指定 `.duckdb` 文件。
- 数据库在每次装载前重建（删除旧文件），避免残留旧 metric 表。

---

## 2. metric 表结构

### 2.1 浮点样本 metric

以 `http_requests_total` 为例，series 的 label 集合为 `{__name__, job, instance, method}`：

```sql
CREATE TABLE "http_requests_total" (
    "timestamp" TIMESTAMP,
    "value"     DOUBLE,
    "instance"  VARCHAR,
    "job"       VARCHAR,
    "method"    VARCHAR
);
```

一行对应一个数据点：

| timestamp | value | instance | job | method |
|-----------|-------|----------|-----|--------|
| 2026-11-11 10:00:00 | 12.5 | host-1 | api | GET |
| 2026-11-11 10:00:00 | 3 | host-2 | api | POST |

- `timestamp`：由 Parquet 的毫秒时间戳 `ts` 通过 `epoch_ms(ts)` 转为 DuckDB `TIMESTAMP`。
- `value`：float 样本值（DOUBLE）；NaN / StaleNaN 保留原值。
- label 列：该 metric 所有 series 的 label 并集；某个 series 缺失的 label 为 `NULL`。
- `__name__` 不单独建列：它就是表名。
- 同一个 metric 的多条 series 通过 label 列区分，不再有 `series_uid` 列。

### 2.2 原生直方图 metric

原生直方图没有单一 `value`，每个数据点是一组计数/桶。表结构为：

```sql
CREATE TABLE "http_request_duration_seconds" (
    "timestamp"       TIMESTAMP,
    "count"           DOUBLE,
    "zero_count"      DOUBLE,
    "sum"             DOUBLE,
    "schema"          INTEGER,
    "zero_threshold"  DOUBLE,
    "pos_spans"       LIST<STRUCT("offset" INTEGER, "length" UINTEGER)>,
    "neg_spans"       LIST<STRUCT("offset" INTEGER, "length" UINTEGER)>,
    "pos_buckets"     LIST<DOUBLE>,
    "neg_buckets"     LIST<DOUBLE>,
    "custom_values"   LIST<DOUBLE>,
    "reset_hint"      UTINYINT,
    "st"              BIGINT,
    -- label 列…
    "job"             VARCHAR
);
```

- 直方图 metric 没有 `value` 列；标量 metric 没有直方图列。
- 若某 metric 同时有 float 与 histogram 样本（罕见），表会同时包含 `value` 和直方图列，各自为 `NULL`。

### 2.3 label 列命名与冲突

- 列名默认就是 label name，带双引号（如 `"instance"`）。
- `__name__` 不建列。
- 与保留列冲突的 label 会重命名为 `label_<name>`，并写入 SQL 注释/日志：
  - 保留列：`timestamp`、`value`、`count`、`zero_count`、`sum`、`schema`、`zero_threshold`、`pos_spans`、`neg_spans`、`pos_buckets`、`neg_buckets`、`custom_values`、`reset_hint`、`st`。
- 该策略避免 DuckDB 重复列名导致整个装载失败；绝大多数 Prometheus label 不会命中。
- metric 表名支持任意 Prometheus metric 名（包括 `:`），统一用双引号转义。

### 2.4 时间与值语义

- `timestamp` 为 UTC 语义的 `TIMESTAMP`（无时区）；原始毫秒精度保留。
- `value` 为 `DOUBLE`；StaleNaN/NaN 写入 DuckDB 后仍是 NaN。
- 直方图桶为**绝对计数**（迁移阶段已从 delta 还原）。
- 标签缺失为 `NULL`，空字符串保持空字符串。

---

## 3. 装载流程

```
Parquet (series / samples / histograms)
        │
        ▼ 一次扫描
CREATE TEMP TABLE _tsdb_metric_long
  metric_key(md5(metric)), ts, value, hist 列, labels MAP
        │
        ▼ COPY ... PARTITION_BY(metric_key)
.duckdb_stage/<tenant|local>/metric_key=<md5>/*.parquet
        │
        ▼ 每个 metric 一条 CREATE TABLE AS
SELECT epoch_ms(ts) AS timestamp,
       value,
       labels['job'] AS job, ...
FROM read_parquet('.../metric_key=<md5>/*.parquet')
        │
        ▼
DuckDB database:
  <out>/migration.duckdb
  或 <out>/duckdb/<tenant>.duckdb
```

设计原因：

- 直接为每个 metric 写 `JOIN samples×series WHERE metric=…` 会导致 **N 次全量扫描**；20k metric 时不可行。
- 先按 metric 做一次分区 Parquet，再按分区建表，只扫描一次原始样本，后续每个 metric 只读自己的小分区。
- 分区键使用 `md5(metric)`，避免 metric 名中的 `:`、`/` 等字符进入路径；Rust 端用同一 MD5 生成表查询路径。
- staging 目录（`<out>/.duckdb_stage/…`）是中间产物，成功装载后清空；保留空目录便于手动重跑 SQL。

---

## 4. 各层修改范围

| 层 | 文件 | 修改 |
|----|------|------|
| Parquet 导出 | `migrate-core/src/sink/parquet.rs` | **无改动** |
| Reader / 解码 | `crates/tsdb-reader/**` | **无改动** |
| 迁移流水线 | `migrate-core/src/engine.rs`（pipeline） | **无改动** |
| DuckDB 表结构 | `migrate-core/src/duckdb.rs` | 重写：扫描 Parquet 的 metric/label schema；生成 metric 宽表 SQL；按 tenant 生成 DB；stage 分区装载 |
| Engine 收尾 | `migrate-core/src/engine.rs` | 完成后调用 `duckdb::plan`；Parquet sink 也生成 SQL；DuckDB sink 按 plan 执行 |
| 配置 | `migrate-core/src/config.rs` | `sink.duckdb_path` 语义：Prometheus 文件 / Mimir 目录 |
| CLI | `tsdb-migrate-cli` | `duckdb-load` 支持 `--no-exec`、`--print-sql`、多 tenant DB 目录 |
| API/Web | 无 | 无需修改；仍通过 `sink.kind=duckdb` / 默认路径工作 |

> `manifest.json`、checkpoint、Parquet 目录结构均保持不变；`verify` 继续校验 Parquet 行数。

---

## 5. 兼容性与限制

### 5.1 兼容

- 原有 `series/samples/histograms` Parquet 表仍存在（供自定义分析），只是不再作为 DuckDB 最终表。
- 旧的 `load_duckdb.sql`（normalized 表）会被新的 metric 宽表 SQL 覆盖；旧 DuckDB 文件在重新装载时删除/重建。
- 本地目录源的输出目录、checkpoint 路径不变。

### 5.2 限制

| 限制 | 说明 |
|------|------|
| 直方图表无 `value` | 原生直方图用 count/sum/buckets 列表达 |
| label 冲突重命名 | 冲突 label 变为 `label_<name>`，见 §2.3 |
| 装载磁盘峰值 | staging 分区 Parquet + 最终 DB ≈ 2× DuckDB 数据量 |
| 每次装载重建 DB | 避免旧 metric 表残留；DB 内不要存放其他业务表 |
| 大量 metric 的 DDL 数量 | 20k metric ≈ 20k 条 `CREATE TABLE`，SQL 文件数 MB；DuckDB 建表本身很快 |
| staging 手动重跑 | 成功装载后 staging 清空；重跑 `duckdb-load` 会重新生成 |

---

## 6. 使用示例

```bash
# Prometheus：生成单个 DB
tsdb-migrate migrate --data-dir /prometheus/data --out /mnt/export \
  --sink duckdb

# Mimir：每个 tenant 一个 DB
tsdb-migrate migrate --mimir-bucket bucketA --mimir-prefix mimir \
  --out /mnt/export --sink duckdb

# 只生成 SQL，不执行（服务器未安装 duckdb CLI 时）
tsdb-migrate duckdb-load --out /mnt/export --no-exec

# 查看生成的 SQL
tsdb-migrate duckdb-load --out /mnt/export --no-exec --print-sql
```

查询：

```sql
-- 每个 metric 直接查表
SELECT * FROM "http_requests_total" WHERE "timestamp" >= TIMESTAMP '2026-11-11 10:00:00';

-- 某 label 的序列
SELECT "timestamp", "value" FROM "http_requests_total"
WHERE "job" = 'api' AND "instance" = 'host-1' ORDER BY 1;

-- 直方图
SELECT "timestamp", unnest("pos_buckets") AS bucket_count
FROM "http_request_duration_seconds" LIMIT 10;
```

---

## 7. 验证记录

用 `fixtures/gen` 生成的真实 Prometheus block（42 series / 100 chunks / 4910 samples，含 float 与 native histogram）验证：

| 场景 | 命令要点 | 结果 |
|------|----------|------|
| Prometheus 单实例 | `migrate --data-dir … --sink duckdb` | 生成 `<out>/migration.duckdb`，**42 张 metric 表** |
| 表结构（float） | `DESCRIBE fixture_metric_00` | `timestamp TIMESTAMP`、`value DOUBLE`、`instance/job/shard VARCHAR` |
| 行数（float） | `SELECT count(*)` | 121 行；`timestamp` 范围 `2023-11-14 22:13:20` → `22:43:20` |
| 表结构（直方图） | `DESCRIBE fixture_hist_00` | `count/zero_count/sum/schema/zero_threshold/pos_spans/neg_spans/pos_buckets/neg_buckets/custom_values/reset_hint/st` + `job`，无 `value` |
| 行数（直方图） | `SELECT count(*)` | 60 行 |
| Mimir 双 tenant | `migrate --mimir-local-root … --sink duckdb` | 生成 `duckdb/tenant-a.duckdb`、`duckdb/tenant-b.duckdb`，各 **42 张表**、`fixture_metric_00` 各 121 行，schema 完全一致 |
| 未安装 duckdb CLI | 从 `PATH` 中移除 `duckdb` | 任务仍 `Completed`，SQL 正常生成，日志给出一条 `Warn`：`DuckDB 装载跳过 […]：duckdb CLI not found in PATH` |
| 路径规则 | 见 `duckdb.rs` 单测 | Prometheus → 单库；Mimir 多 tenant 指定 `.duckdb` 文件 → 明确报错；Mimir 单 tenant 允许指定文件 |

`scripts/e2e.sh` 与 `scripts/mimir-local-e2e.sh` 会在检测到 Python `duckdb` 时，直接执行生成的 SQL 并断言表数量、行数与列名，作为常规回归的一部分。

> 本地验证时若未安装 `duckdb` CLI，可用 Python 绑定包装一个同名脚本（读取 `argv[1]` 作为库路径、从 stdin 读 SQL、`duckdb.connect(path).execute(sql)`）放入 `PATH`，即可完整走通 `--sink duckdb` 的真实装载链路。
