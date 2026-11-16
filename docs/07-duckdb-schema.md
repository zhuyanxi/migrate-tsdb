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
        ▼ 一次扫描（流式，不落地中间表）
COPY (
  SELECT metric_key(md5(metric)), ts, value, hist 列, labels MAP ...
)[
        ▼ PARTITION_BY(metric_key)
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
- staging 目录（`<out>/.duckdb_stage/…`）是中间产物，**只有全部 metric 建表成功后才会清空**；保留空目录便于手动重跑 SQL。
- staging 直接由 `COPY (子查询)` 流式写出，**不再先 `CREATE TEMP TABLE` 物化长表**：旧写法会把整张长表先放进临时库（超出 `memory_limit` 就全量溢写到 `temp_directory`），再 COPY 一遍，等于多占一份数据。实测（2M 样本，内存压到 256–768 MiB）：旧写法峰值溢写 **≈230 MB**，新写法 **0 MB**。
- staging 的 Parquet 压缩沿用 `sink.compression`（默认 `zstd`）：实测同一份数据 zstd 为 snappy 的 **21%**（32.5 MB → 6.8 MB），这是超大导出时最省磁盘的一个开关（代价是 CPU）。

### 3.1 两阶段执行（staging / 建表）

生成的 SQL 用一行注释 `-- tsdb-migrate:phase-boundary` 分成两半，`execute()` 把它们作为**两个独立的 duckdb 进程**执行：

| 阶段 | 内容 | 失败时的处理 |
|------|------|--------------|
| staging | 一条 `COPY (子查询) … PARTITION_BY (metric_key)`，**流式写盘，不落地中间表** | 直接失败；提示「磁盘空间/上面的 DuckDB 报错才是真正原因」 |
| 校验 | 检查每个 `metric_key=*/data_*.parquet` ≥ 8 字节 | 有截断文件则清空 staging **重试一次**，仍失败才报错 |
| 建表 | 每个 metric 一条 `CREATE TABLE … read_parquet(…)` | 失败即中止（`-bail`），不会级联刷屏 |

为什么必须分阶段：DuckDB 会**先创建**每个 `data_N.parquet`，第一个 row group 落盘前文件是 **0 字节**。如果 COPY 中途被打断（磁盘满、OOM、进程被杀），或者两个装载同时写同一个目录，建表阶段就会读到这些空文件，每个都报：

```
Invalid Input Error: File '…/metric_key=<md5>/data_0.parquet' too small to be a Parquet file
```

而 duckdb CLI 默认 **遇到错误继续执行**，于是刷出多条同样的报错，最后留下一个「少了几张表」的数据库。现在的行为：

- 校验能发现截断文件并自动重跑一次 staging；
- `duckdb <db> -bail` 让 CLI 在第一条错误处停止；
- 数据库先写到 `. <name>.duckdb.tmp`，成功才 rename 成正式文件名，失败时删除临时库 —— **失败不会再留下半成品 DB**；
- 每个 staging 目录有一把 `<out>/.duckdb_stage/.<tenant>.load.lock`，同一目录的第二个装载会被拒绝（12 小时以上的陈旧锁自动忽略）；
- staging 失败时保留现场（目录 + SQL），便于排查后重跑。

> 手动重跑生成的 SQL 时同样建议加 `-bail`：`duckdb <db> -bail < load_duckdb_<tenant>.sql`。

### 3.2 资源上限（内存 24 GiB / 磁盘 200 GiB）

装载阶段的资源上限写在生成的 SQL 头部，引擎执行与手工执行**完全一致**：

```sql
-- Resource caps: memory_limit / max_temp_directory_size / temp_directory.
SET memory_limit='24GiB';                 -- 内存硬上限，超了 DuckDB 溢写而不是 OOM
SET max_temp_directory_size='200GiB';     -- 溢写目录上限
SET temp_directory='<out>/.duckdb_stage/.spill';
SET threads=8;                            -- 可选，0 = DuckDB 默认（= CPU 核数）
```

| 维度 | 由谁保证 | 说明 |
|------|----------|------|
| 内存 ≤ `sink.duckdb_memory_limit_mb`（默认 24 GiB） | **DuckDB 自身** | `memory_limit`，超限时溢写到 `temp_directory`；不会无限吃内存 |
| 溢写 ≤ `sink.duckdb_disk_limit_mb` | **DuckDB 自身** | `max_temp_directory_size` |
| 装载产物总量 ≤ `sink.duckdb_disk_limit_mb`（默认 200 GiB） | **本程序**（每 500 ms 轮询） | 统计 staging + 溢写 + 数据库（含 `.wal`）；超限即 kill 进程并**不发布**数据库 |

口径与注意事项：

- 磁盘预算只覆盖**装载自己写的东西**：`<stage>`、`temp_directory`、`<tenant>.duckdb.tmp`（及其 `.wal`）。已有导出 `blocks/` 和旧数据库不计入。
- 轮询间隔 500 ms，因此极端情况下会短暂超出预算几十 MB 到几百 MB；超限后立即终止且**不会发布**数据库。
- `GiB` 是 2³⁰ 字节；DuckDB 的 `GB` 是 10⁹ 字节，所以默认值写 `24GiB` / `200GiB`。
- 阶段切换时会**重新应用**这些设置（建表阶段是另一个 duckdb 进程）。
- 建表阶段按 `sink.duckdb_metric_batch` 分批（默认 0 = 一次跑完）：**每批提交后立即删除该批的 staging 分区**，所以 staging 随建表推进而收缩，峰值从「staging + 完整数据库」降到「max(staging, 数据库)」。
- `sink.duckdb_drop_export = true`：staging 校验通过后**删除 `blocks/<tenant>`**，再开始建表。省掉一份完整导出，但数据此后只在 DuckDB 里，重跑需要重新迁移。
- `sink.duckdb_chunk_by_block = true`（`--duckdb-chunk-by-block`）：**按 block 分批 staging**。每个 block 单独 `COPY ... WHERE block = ...`（hive 分区列，DuckDB 只扫该 block 的 parquet），随后把出现的 metric 建表/追加，再删掉这块的 staging（配 `duckdb_drop_export` 时连这块的导出一并删除），然后处理下一个 block。首次出现的 metric 用 `CREATE OR REPLACE TABLE`，之后用 `INSERT INTO`。

  峰值由此变成 **max(导出, 数据库) + 单块 staging**，而不是「整份 staging + 完整数据库」。代价：每个 block 一次 duckdb 进程（进程启动开销可忽略），以及同一 metric 跨 block 时是多次 `INSERT`（DuckDB 追加写，很快）。
- `sink.duckdb_window_minutes = 60`（`--duckdb-window-minutes 60`）：**每小时一个 DuckDB 实例**，按时间戳顺序依次装载。

  - 时间范围取自 samples Parquet 的 **footer 统计**（只读元数据，不扫数据），按 window 边界对齐（60 分钟 = 整点 UTC）。
  - 每个窗口独立：`COPY (… WHERE s.ts >= start AND s.ts < end)` → 按该窗口实际产生的 `metric_key=*` 分区生成建表语句 → 删除 staging → 发布数据库。峰值 ≈ 单窗口 staging + 单窗口数据库。
  - 数据库文件名 = **窗口标签 + 原文件名**，例如 `duckdb/2026-11-11T10-t1.duckdb`（Prometheus 源是 `2026-11-11T10-migration.duckdb`）；SQL 文件同理 `2026-11-11T10-load_duckdb_t1.sql`。
  - 标签用 **UTC** 的 `YYYY-MM-DDTHH`（若只用 `YYYY-MM-DD`，同一天 24 个窗口会重名）。要按本地时区分桶，先把数据时区对齐或用 `filter` 拆分运行。
  - 窗口内没有样本的 metric 不会建表（窗口 SQL 的建表语句在 staging 之后按分区动态生成），因此每个小时库里只包含该小时实际出现过的 metric。
- 容量预估（实测 2M 样本 / 25 万 series / 6 label，导出 13.65 MB）：

| 项 | 相对导出 | 说明 |
|----|----------|------|
| staging（`sink.compression=snappy`） | ≈ 2.4× | 每行都带 labels |
| staging（`zstd`，默认） | ≈ 0.5× | **推荐**，超大导出必选 |
| 最终数据库（每 metric 宽表） | ≈ 3.3× | label 展开成列后 DuckDB 的压缩比有限 |
| **装载峰值**（默认：导出 + staging + 数据库） | ≈ 4.8×（zstd）/ 6.7×（snappy） | 旧写法还要再加一份长表溢写 |
| 峰值（`duckdb_metric_batch>0` + `duckdb_drop_export`） | ≈ max(1.5×, 数据库) | 只剩数据库本身 |
| 峰值（再开 `duckdb_chunk_by_block`） | ≈ 数据库 + 单块 staging | **最小**：staging 不再需要整份 |

- 导出 `blocks/` 只在 staging 阶段被读，之后不再使用；装载成功后如需腾空间可手工删除（重跑就需重新迁移）。
- **两个独立的磁盘闸门**（都只统计「装载自己写的东西」，不含既有 `blocks/`）：

  | 配置 | 统计范围 | 默认 |
  |------|----------|------|
  | `sink.duckdb_stage_limit_mb`（`--duckdb-stage-limit-mb`） | `.duckdb_stage` 里的 staging + DuckDB 溢写文件 | **200 GiB** |
  | `sink.duckdb_disk_limit_mb`（`--duckdb-disk-limit-mb`） | 上一项 **加上** 正在写入的数据库（`<db>.tmp` + `.wal`） | 0 = 不限制 |

- 超限即 kill 当前 duckdb 进程并**不发布**数据库，错误信息里会写明是哪一个闸门抬的。
- 注意：**最终数据库通常比 staging 大得多**（见上表 3.3× vs 0.5×），所以「只限制 staging」并不能保证整机磁盘不被写满；数据库大小要自己按 `est_db` 预留。
- 建表阶段是单进程、串行 DDL；失败后整个装载要从 staging 重来（数据库一次性发布），超大导出建议先小批量验证。

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
| 装载磁盘峰值 | staging 分区 Parquet + 最终 DB ≈ 2× DuckDB 数据量；磁盘不够时 staging 会失败（错误信息会提示） |
| 每次装载重建 DB | 避免旧 metric 表残留；DB 内不要存放其他业务表 |
| 大量 metric 的 DDL 数量 | 20k metric ≈ 20k 条 `CREATE TABLE`，SQL 文件数 MB；DuckDB 建表本身很快 |
| staging 手动重跑 | 成功装载后 staging 清空；重跑 `duckdb-load` 会重新生成 |
| 并发装载 | 同一 `<out>` 同时只允许一个装载（load lock）；并发时会直接报错而不是产生空 parquet |
| 失败后的 DB | 装载失败不会发布 DB（先写 `.tmp` 再 rename）；旧 DB 保留不动 |

### 5.3 故障排查：`too small to be a Parquet file`

日志里出现：

```
Invalid Input Error: File '<out>/.duckdb_stage/<tenant>/metric_key=<md5>/data_0.parquet' too small to be a Parquet file
```

说明建表阶段读到的 staging 文件 **不足 8 字节**（DuckDB 连 footer magic 都读不到），不是你的 metric 数据有问题。常见成因：

1. 上一次 staging 的 `COPY` 被打断（磁盘写满、OOM、进程被杀、机器重启），空文件留在目录里；
2. 两个装载进程同时操作同一个 `<out>`（引擎收尾自动装载 + 手动 `duckdb-load`，或 `force` 重跑）；
3. 手动执行 SQL 时没有先清空 staging 目录。

现在（本版本起）这三类都会被拦在发布之前：截断文件会被校验发现并自动重跑一次 staging；并发装载会被 lock 拒绝；`-bail` 使第一条错误就是最后一条。若仍看到该报错，请往上看**第一条**错误（通常是 `IO Error` / `Out of Memory`），并检查：

```bash
df -h <out>                                     # staging 需要再存一份导出
find <out>/.duckdb_stage -type f -size -8c      # 0 字节的 staging 文件
ls -l <out>/.duckdb_stage/.*.load.lock          # 残留锁
```

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
