//! DuckDB integration.
//!
//! The migration itself still writes Parquet; this module is the final DuckDB
//! shaping step. It scans the exported `series`/`histograms` Parquet files to
//! learn the label set of every metric, then emits one DuckDB table per metric:
//!
//! ```sql
//! CREATE TABLE "http_requests_total" AS
//! SELECT epoch_ms(s.ts) AS "timestamp",
//!        s.value        AS "value",
//!        x.labels['job'] AS "job"
//! FROM read_parquet('.../samples/*.parquet', hive_partitioning=true) s
//! JOIN read_parquet('.../series/*.parquet', hive_partitioning=true) x
//!   ON s.series_uid=x.series_uid AND s.block=x.block
//! WHERE x.labels['__name__'] = 'http_requests_total';
//! ```
//!
//! A Prometheus export gets one DuckDB file; a Mimir export gets one file per
//! tenant (`<out>/duckdb/<tenant>.duckdb`).
//!
//! The Parquet layout, reader, pipeline and checkpoint logic are untouched.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow_array::{Array, MapArray, StringArray, UInt64Array};
use md5::{Digest, Md5};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics;

use crate::config::is_valid_tenant;
use crate::error::{Error, Result};

pub const LOAD_SQL_FILE: &str = "load_duckdb.sql";
/// Boundary between the staging half and the metric-table half of the generated
/// script. Written as a SQL comment so the file stays runnable by hand.
pub const PHASE_MARKER: &str = "-- tsdb-migrate:phase-boundary";
/// Smallest possible Parquet file: `PAR1` magic plus the same magic in the
/// footer. DuckDB rejects anything shorter with "too small to be a Parquet
/// file", which is exactly what a staging file looks like before its first row
/// group is flushed.
const PARQUET_MIN_BYTES: u64 = 8;
/// A lock left by a killed process is ignored after this long.
const STALE_LOCK: std::time::Duration = std::time::Duration::from_secs(12 * 60 * 60);
const RESERVED_COLUMNS: &[&str] = &[
    "timestamp",
    "value",
    "count",
    "zero_count",
    "sum",
    "schema",
    "zero_threshold",
    "pos_spans",
    "neg_spans",
    "pos_buckets",
    "neg_buckets",
    "custom_values",
    "reset_hint",
    "st",
];
const HIST_COLUMNS: &[&str] = &[
    "count",
    "zero_count",
    "sum",
    "schema",
    "zero_threshold",
    "pos_spans",
    "neg_spans",
    "pos_buckets",
    "neg_buckets",
    "custom_values",
    "reset_hint",
    "st",
];

#[derive(Debug, Clone)]
pub struct DuckDbPlan {
    /// `None` for a Prometheus/local export, `Some(tenant)` for Mimir.
    pub tenant: Option<String>,
    pub sql_path: PathBuf,
    pub db_path: PathBuf,
    /// Intermediate metric-partitioned Parquet used to build the wide tables.
    pub stage_dir: PathBuf,
    pub metrics: usize,
    pub warnings: Vec<String>,
    /// Parquet export this plan reads; deleted after staging when
    /// `drop_export` is set.
    pub export_dir: PathBuf,
    /// Delete `export_dir` once staging has been validated, so only one copy
    /// of the data is on disk during the table phase.
    pub drop_export: bool,
    /// Metrics per DuckDB batch; `0` builds all tables in one process.
    /// Each batch deletes its staging partitions as soon as it is loaded.
    pub metric_batch: usize,
    /// Row count of the samples export (0 when unknown); used for a rough
    /// database-size estimate before the load starts.
    pub samples: u64,
    /// Set when the export is split into per-hour databases.
    pub window: Option<Window>,
    /// Metric schemas of the whole export; the windowed load derives its table
    /// statements from the staging partitions actually produced.
    #[doc(hidden)]
    pub schemas: BTreeMap<String, MetricSchema>,
    /// Resource caps applied to every DuckDB process of this load.
    pub limits: LoadLimits,
}

/// Bytes per loaded sample used by the size estimate. Measured on a 2M sample
/// export with six labels: 44.8 MB of wide tables, i.e. ~22 B/row after DuckDB
/// compression. Timestamps and values dominate, so it is label-count insensitive
/// to first order; treat it as an order-of-magnitude hint, not a promise.
const ESTIMATED_BYTES_PER_SAMPLE: u64 = 22;

impl DuckDbPlan {
    /// Rough size of the resulting database, for capacity planning.
    pub fn estimated_db_bytes(&self) -> u64 {
        self.samples.saturating_mul(ESTIMATED_BYTES_PER_SAMPLE)
    }
}

/// Resource caps for the DuckDB load step.
///
/// The memory cap is enforced by DuckDB itself (`SET memory_limit`): once it is
/// reached DuckDB spills to `temp_dir` instead of allocating further. The disk
/// budget is enforced by us: it covers what the load itself writes (staging
/// Parquet, spill files and the database) and a phase is killed as soon as the
/// total exceeds it.
#[derive(Debug, Clone)]
pub struct LoadLimits {
    pub memory_bytes: u64,
    /// Cap for the temporary data (staging + spill); 0 = unlimited.
    pub stage_bytes: u64,
    /// Cap for the whole load including the database being written;
    /// 0 = unlimited.
    pub disk_bytes: u64,
    /// `0` keeps DuckDB's own default (one thread per core).
    pub threads: usize,
    pub temp_dir: PathBuf,
    /// Compression of the intermediate per-metric Parquet. This is the single
    /// biggest disk consumer of the load, so it follows `sink.compression`.
    pub stage_compression: crate::config::Compression,
}

impl LoadLimits {
    pub fn from_sink(sink: &crate::config::SinkConfig, out_dir: &Path) -> Self {
        let temp_dir = if sink.duckdb_temp_dir.trim().is_empty() {
            out_dir.join(".duckdb_stage").join(".spill")
        } else {
            let p = PathBuf::from(sink.duckdb_temp_dir.trim());
            if p.is_absolute() {
                p
            } else {
                out_dir.join(p)
            }
        };
        Self {
            memory_bytes: sink.duckdb_memory_limit_mb * 1024 * 1024,
            stage_bytes: sink.duckdb_stage_limit_mb * 1024 * 1024,
            disk_bytes: sink.duckdb_disk_limit_mb * 1024 * 1024,
            threads: sink.duckdb_threads,
            temp_dir,
            stage_compression: sink.compression,
        }
    }

    /// DuckDB statements that install the caps; prepended to every phase so
    /// manual runs of the generated file behave the same as the built-in one.
    pub fn settings_sql(&self) -> String {
        let mut sql = String::new();
        sql.push_str(&format!(
            "SET memory_limit='{}';\n",
            duckdb_size(self.memory_bytes)
        ));
        // Spill lives in the temporary area, so it is bounded by that budget.
        let spill_cap = if self.stage_bytes > 0 {
            self.stage_bytes
        } else {
            self.disk_bytes
        };
        if spill_cap > 0 {
            sql.push_str(&format!(
                "SET max_temp_directory_size='{}';\n",
                duckdb_size(spill_cap)
            ));
        }
        sql.push_str(&format!(
            "SET temp_directory='{}';\n",
            sql_escape(&sql_path_str(&self.temp_dir))
        ));
        if self.threads > 0 {
            sql.push_str(&format!("SET threads={};\n", self.threads));
        }
        sql
    }
}

/// DuckDB accepts binary size suffixes; `GiB` is unambiguous (its `GB` is 1e9).
fn duckdb_size(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes.is_multiple_of(GIB) {
        format!("{}GiB", bytes / GIB)
    } else if bytes.is_multiple_of(MIB) {
        format!("{}MiB", bytes / MIB)
    } else {
        format!("{bytes}B")
    }
}

/// DuckDB 'COPY' compression keyword for a configured compression.
fn duckdb_compression(compression: crate::config::Compression) -> &'static str {
    match compression {
        crate::config::Compression::Zstd => "zstd",
        crate::config::Compression::Snappy => "snappy",
        crate::config::Compression::None => "uncompressed",
    }
}

fn sql_path_str(path: &Path) -> String {
    path.to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/")
}

#[derive(Debug, Default, Clone)]
pub struct MetricSchema {
    labels: BTreeSet<String>,
    has_float: bool,
    has_hist: bool,
}

struct LabelColumn {
    key: String,
    column: String,
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Builds the final DuckDB plans (one per tenant/database) and writes SQL files.
/// `configured_db` is empty for defaults, a file for a Prometheus export, or a
/// directory for a multi-tenant Mimir export. Uses the default resource caps;
/// prefer [`plan_with_sink`] so configured limits are honoured.
pub fn plan(out_dir: &Path, configured_db: &str) -> Result<Vec<DuckDbPlan>> {
    let sink = crate::config::SinkConfig {
        duckdb_path: configured_db.to_string(),
        ..Default::default()
    };
    plan_with_sink(out_dir, &sink)
}

/// Like [`plan`] but takes the whole sink config, so `sink.duckdb_path` and
/// the memory/disk/thread caps all come from the job.
pub fn plan_with_sink(out_dir: &Path, sink: &crate::config::SinkConfig) -> Result<Vec<DuckDbPlan>> {
    let configured_db = sink.duckdb_path.as_str();
    let limits = LoadLimits::from_sink(sink, out_dir);
    let tenants = output_tenants(out_dir);
    let mut plans = Vec::new();
    let window_minutes = sink.duckdb_window_minutes;
    let targets: Vec<Option<String>> = if tenants.is_empty() {
        vec![None]
    } else {
        tenants.into_iter().map(Some).collect()
    };
    for tenant in targets {
        let tenant_ref = tenant.as_deref();
        let base_db = resolve_db_paths(out_dir, configured_db, tenant_ref)?;
        let stage = stage_dir(out_dir, tenant_ref);
        let scan = scan_export(out_dir, tenant_ref)?;
        // One database per time window when asked for (and the export has a
        // time range to split on).
        let windows = if window_minutes == 0 {
            Vec::new()
        } else {
            export_time_range(out_dir, tenant_ref)
                .map(|(min, max)| windows_for(min, max, window_minutes))
                .unwrap_or_default()
        };
        if windows.is_empty() {
            let (sql_path, metrics, warnings, samples) = build_sql(
                out_dir,
                tenant_ref,
                &stage,
                &limits,
                sink.duckdb_chunk_by_block,
                None,
                &scan,
            )?;
            plans.push(DuckDbPlan {
                export_dir: export_dir(out_dir, tenant_ref),
                drop_export: sink.duckdb_drop_export,
                metric_batch: sink.duckdb_metric_batch,
                samples,
                tenant: tenant.clone(),
                sql_path,
                db_path: base_db,
                stage_dir: stage,
                metrics,
                warnings,
                window: None,
                schemas: scan.metrics.clone(),
                limits: limits.clone(),
            });
        } else {
            for window in windows {
                let (sql_path, metrics, warnings, samples) = build_sql(
                    out_dir,
                    tenant_ref,
                    &stage,
                    &limits,
                    sink.duckdb_chunk_by_block,
                    Some(window),
                    &scan,
                )?;
                plans.push(DuckDbPlan {
                    export_dir: export_dir(out_dir, tenant_ref),
                    drop_export: sink.duckdb_drop_export,
                    metric_batch: sink.duckdb_metric_batch,
                    samples,
                    tenant: tenant.clone(),
                    sql_path,
                    db_path: window_db_path(&base_db, &window),
                    stage_dir: stage.clone(),
                    metrics,
                    warnings,
                    window: Some(window),
                    schemas: scan.metrics.clone(),
                    limits: limits.clone(),
                });
            }
        }
    }
    // Timestamp order, tenant by tenant.
    plans.sort_by(|a, b| {
        (a.tenant.clone(), a.window.map(|w| w.start_ms))
            .cmp(&(b.tenant.clone(), b.window.map(|w| w.start_ms)))
    });
    Ok(plans)
}

/// `<YYYY-MM-DDTHH>-<original name>`, next to the original database.
fn window_db_path(base: &Path, window: &Window) -> PathBuf {
    let name = base
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "migration.duckdb".to_string());
    base.with_file_name(format!("{}-{name}", window.label()))
}

/// Convenience used after a completed run when no custom DB path is needed.
pub fn write_sqls(out_dir: &Path) -> Result<Vec<DuckDbPlan>> {
    plan(out_dir, "")
}

fn output_tenants(out_dir: &Path) -> Vec<String> {
    let dir = out_dir.join("blocks");
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for e in rd.flatten() {
        if !e.path().is_dir() {
            continue;
        }
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(t) = name.strip_prefix("tenant=") {
            if is_valid_tenant(t) {
                out.push(t.to_string());
            }
        }
    }
    out.sort();
    out
}

fn resolve_db_paths(out_dir: &Path, configured: &str, tenant: Option<&str>) -> Result<PathBuf> {
    let configured = configured.trim();
    match tenant {
        None => {
            if configured.is_empty() {
                Ok(out_dir.join("migration.duckdb"))
            } else {
                let p = PathBuf::from(configured);
                if p.extension().map(|e| e == "duckdb").unwrap_or(false) {
                    Ok(p)
                } else {
                    Ok(p.join("migration.duckdb"))
                }
            }
        }
        Some(t) => {
            let dir = if configured.is_empty() {
                out_dir.join("duckdb")
            } else {
                let p = PathBuf::from(configured);
                if p.extension().map(|e| e == "duckdb").unwrap_or(false) {
                    // A single file only makes sense for a single tenant.
                    if output_tenants(out_dir).len() > 1 {
                        return Err(Error::sink(
                            "sink.duckdb_path must be a directory when migrating multiple Mimir tenants",
                        ));
                    }
                    return Ok(p);
                } else {
                    p
                }
            };
            Ok(dir.join(format!("{t}.duckdb")))
        }
    }
}

fn sql_path(out_dir: &Path, tenant: Option<&str>, window: Option<Window>) -> PathBuf {
    let base = match tenant {
        None => LOAD_SQL_FILE.to_string(),
        Some(t) => format!("load_duckdb_{t}.sql"),
    };
    match window {
        Some(w) => out_dir.join(format!("{}-{base}", w.label())),
        None => out_dir.join(base),
    }
}

fn export_dir(out_dir: &Path, tenant: Option<&str>) -> PathBuf {
    match tenant {
        Some(t) => out_dir.join("blocks").join(format!("tenant={t}")),
        None => out_dir.join("blocks"),
    }
}

fn stage_dir(out_dir: &Path, tenant: Option<&str>) -> PathBuf {
    out_dir
        .join(".duckdb_stage")
        .join(tenant.unwrap_or("local"))
}

fn metric_key(metric: &str) -> String {
    let digest = Md5::digest(metric.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn build_sql(
    out_dir: &Path,
    tenant: Option<&str>,
    stage: &Path,
    limits: &LoadLimits,
    chunk_by_block: bool,
    window: Option<Window>,
    scan: &ExportScan,
) -> Result<(PathBuf, usize, Vec<String>, u64)> {
    let metrics = &scan.metrics;
    // Row count from the Parquet footers only (no data is read): used for the
    // database-size estimate before the load starts.
    let samples_rows = dataset_rows(&export_dir(out_dir, tenant));
    // DuckDB's COPY ... PARTITION_BY does not create the final parent
    // directory, so prepare an empty staging directory up front.
    if stage.exists() {
        std::fs::remove_dir_all(stage)?;
    }
    std::fs::create_dir_all(stage)?;
    let stage = stage.canonicalize().unwrap_or_else(|_| stage.to_path_buf());
    let root = out_dir
        .canonicalize()
        .unwrap_or_else(|_| out_dir.to_path_buf());
    let root = sql_escape(&root.display().to_string());
    let stage_s = sql_escape(&stage.display().to_string());
    let mut sql = String::new();
    sql.push_str("-- Generated by tsdb-migrate. Requires DuckDB >= 0.10.\n");
    // Resource caps travel with the file so a manual run behaves like the
    // built-in one.
    sql.push_str("-- Resource caps: memory_limit / max_temp_directory_size / temp_directory.\n");
    sql.push_str(&limits.settings_sql());
    sql.push('\n');
    match tenant {
        Some(t) => sql.push_str(&format!(
            "-- Source: Mimir tenant {t} - one table per metric, label columns + timestamp.\n\n"
        )),
        None => sql.push_str(
            "-- Source: Prometheus/local - one table per metric, label columns + timestamp.\n\n",
        ),
    }
    let mut warnings = Vec::new();
    if metrics.is_empty() {
        sql.push_str("-- No series were found; no metric tables created.\n");
    } else {
        let any_float = metrics.values().any(|m| m.has_float);
        let any_hist = metrics.values().any(|m| m.has_hist);
        let compression = duckdb_compression(limits.stage_compression);
        let copy_into_stage = |body: &str| {
            format!("COPY (\n{body}\n) TO '{stage_s}' (FORMAT PARQUET, COMPRESSION {compression}, PARTITION_BY (metric_key), OVERWRITE_OR_IGNORE);\n\n")
        };
        let metric_statement = |metric: &str, mode: MetricWrite, warnings: &mut Vec<String>| {
            let schema = &metrics[metric];
            let (labels, mut w) = label_columns(&schema.labels);
            warnings.append(&mut w);
            let mut out = format!("-- tsdb-metric {}\n", metric_key(metric));
            out.push_str(&metric_sql(
                metric,
                schema,
                &labels,
                &format!("{stage_s}/metric_key={}/*.parquet", metric_key(metric)),
                mode,
            ));
            out.push('\n');
            out
        };

        // A hourly (windowed) database only contains the metrics that have
        // samples inside the window, which is not known before staging runs;
        // `execute` therefore derives the table statements from the staged
        // partitions. The file still records the window and the staging step.
        if window.is_some() {
            sql.push_str(&copy_into_stage(&stage_select(
                &root, tenant, any_float, any_hist, None, window,
            )));
            sql.push_str(PHASE_MARKER);
            sql.push_str("\n\n");
            sql.push_str(
                "-- Windowed load: one CREATE TABLE per metric partition that this\n-- window actually produced; the loader generates them after staging.\n",
            );
        // Chunking only makes sense with more than one block.
        } else if chunk_by_block && scan.blocks.len() > 1 {
            // Stage one block, extend the metric tables, drop that staging and
            // (optionally) that block of the export. Peak disk becomes "database
            // plus one block of staging" instead of "whole staging plus database".
            let mut created: BTreeSet<String> = BTreeSet::new();
            for (block, block_metrics) in &scan.blocks {
                sql.push_str(&format!("-- tsdb-block {block}\n"));
                sql.push_str(&copy_into_stage(&stage_select(
                    &root,
                    tenant,
                    any_float,
                    any_hist,
                    Some(block),
                    None,
                )));
                sql.push_str(&format!("-- tsdb-load {block}\n"));
                for metric in block_metrics {
                    if !metrics.contains_key(metric) {
                        continue;
                    }
                    let mode = if created.insert(metric.clone()) {
                        MetricWrite::Create
                    } else {
                        MetricWrite::Append
                    };
                    let statement = metric_statement(metric, mode, &mut warnings);
                    sql.push_str(&statement);
                }
            }
        } else {
            sql.push_str(&copy_into_stage(&stage_select(
                &root, tenant, any_float, any_hist, None, None,
            )));
            // Everything above stages the export; everything below builds the
            // metric tables. `execute` runs the two halves as separate DuckDB
            // processes and validates the staging output in between.
            sql.push_str(PHASE_MARKER);
            sql.push_str("\n\n");
            for metric in metrics.keys() {
                let statement = metric_statement(metric, MetricWrite::Create, &mut warnings);
                sql.push_str(&statement);
            }
        }
    }
    if !warnings.is_empty() {
        let mut header = String::new();
        for w in &warnings {
            header.push_str(&format!("-- WARNING: {w}\n"));
        }
        header.push('\n');
        let pos = sql.find('\n').map(|i| i + 1).unwrap_or(0);
        sql.insert_str(pos, &header);
    }
    let path = sql_path(out_dir, tenant, window);
    std::fs::write(&path, sql)?;
    Ok((path, metrics.len(), warnings, samples_rows))
}

/// The staging SELECT: the joined long-format rows, optionally restricted to one
/// block (a hive-partition column, so DuckDB prunes to that block's files).
fn stage_select(
    root: &str,
    tenant: Option<&str>,
    any_float: bool,
    any_hist: bool,
    block: Option<&str>,
    window: Option<Window>,
) -> String {
    let glob = |dataset: &str| match tenant {
        Some(t) => format!("{root}/blocks/tenant={t}/**/{dataset}/*.parquet"),
        None => format!("{root}/blocks/**/{dataset}/*.parquet"),
    };
    let filter = |alias: &str| {
        let mut clauses = Vec::new();
        if let Some(b) = block {
            clauses.push(format!("{alias}.block = '{}'", sql_escape(b)));
        }
        if let Some(w) = window {
            clauses.push(format!(
                "{alias}.ts >= {} AND {alias}.ts < {}",
                w.start_ms, w.end_ms
            ));
        }
        if clauses.is_empty() {
            String::new()
        } else {
            format!("\nWHERE {}", clauses.join(" AND "))
        }
    };
    let mut parts = Vec::new();
    if any_float {
        let mut cols = vec![
            "md5(x.labels['__name__']) AS metric_key".to_string(),
            "s.ts AS ts".to_string(),
            "s.value AS value".to_string(),
        ];
        if any_hist {
            for c in HIST_COLUMNS {
                cols.push(format!("NULL AS {}", quote_ident(c)));
            }
        }
        cols.push("x.labels AS labels".to_string());
        parts.push(format!(
            "SELECT\n    {}\nFROM read_parquet('{}', hive_partitioning = true) s\nJOIN read_parquet('{}', hive_partitioning = true) x\n  ON s.series_uid = x.series_uid AND s.block = x.block{}",
            cols.join(",\n    "),
            sql_escape(&glob("samples")),
            sql_escape(&glob("series")),
            filter("s")
        ));
    }
    if any_hist {
        let mut cols = vec![
            "md5(x.labels['__name__']) AS metric_key".to_string(),
            "h.ts AS ts".to_string(),
        ];
        if any_float {
            cols.push("NULL AS value".to_string());
        }
        for c in HIST_COLUMNS {
            cols.push(format!("h.{} AS {}", c, quote_ident(c)));
        }
        cols.push("x.labels AS labels".to_string());
        parts.push(format!(
            "SELECT\n    {}\nFROM read_parquet('{}', hive_partitioning = true) h\nJOIN read_parquet('{}', hive_partitioning = true) x\n  ON h.series_uid = x.series_uid AND h.block = x.block{}",
            cols.join(",\n    "),
            sql_escape(&glob("histograms")),
            sql_escape(&glob("series")),
            filter("h")
        ));
    }
    parts.join("\nUNION ALL\n")
}

/// How a metric table is written: first time it appears, then appended to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricWrite {
    Create,
    Append,
}

fn metric_sql(
    metric: &str,
    schema: &MetricSchema,
    labels: &[LabelColumn],
    stage_glob: &str,
    mode: MetricWrite,
) -> String {
    let table = quote_ident(metric);
    let mut cols = vec!["epoch_ms(ts) AS \"timestamp\"".to_string()];
    if schema.has_float {
        cols.push("value AS \"value\"".to_string());
    }
    if schema.has_hist {
        for c in HIST_COLUMNS {
            cols.push(format!("{} AS {}", c, quote_ident(c)));
        }
    }
    for l in labels {
        cols.push(format!(
            "labels['{}'] AS {}",
            sql_escape(&l.key),
            quote_ident(&l.column)
        ));
    }
    let head = match mode {
        MetricWrite::Create => format!("CREATE OR REPLACE TABLE {table} AS"),
        MetricWrite::Append => format!("INSERT INTO {table}"),
    };
    format!(
        "{head}\nSELECT\n    {}\nFROM read_parquet('{}');\n",
        cols.join(",\n    "),
        sql_escape(stage_glob)
    )
}

fn label_columns(labels: &BTreeSet<String>) -> (Vec<LabelColumn>, Vec<String>) {
    let mut used: BTreeSet<String> = RESERVED_COLUMNS.iter().map(|s| s.to_string()).collect();
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    for key in labels {
        let mut column = key.clone();
        if used.contains(&column) {
            column = format!("label_{key}");
            warnings.push(format!(
                "label {key:?} conflicts with a reserved DuckDB column; exported as {column:?}"
            ));
        }
        while used.contains(&column) {
            column.push('_');
        }
        used.insert(column.clone());
        out.push(LabelColumn {
            key: key.clone(),
            column,
        });
    }
    (out, warnings)
}

/// One time window of the export, half-open `[start_ms, end_ms)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Window {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl Window {
    /// `YYYY-MM-DDTHH` in UTC, used as the database-name prefix.
    pub fn label(&self) -> String {
        chrono::DateTime::from_timestamp_millis(self.start_ms)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp_millis(0).expect("epoch"))
            .format("%Y-%m-%dT%H")
            .to_string()
    }
}

impl std::fmt::Display for Window {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// Splits `[min_ms, max_ms]` into windows of `minutes`, aligned to the window
/// boundary (so 60 gives whole UTC hours).
fn windows_for(min_ms: i64, max_ms: i64, minutes: u64) -> Vec<Window> {
    let step = (minutes.max(1) as i64).saturating_mul(60_000);
    if step <= 0 || max_ms < min_ms {
        return Vec::new();
    }
    let mut start = min_ms.div_euclid(step) * step;
    let mut out = Vec::new();
    while start <= max_ms {
        out.push(Window {
            start_ms: start,
            end_ms: start + step,
        });
        start += step;
    }
    out
}

/// Time range of the samples export, from the Parquet footer statistics only
/// (no data is read).
fn export_time_range(out_dir: &Path, tenant: Option<&str>) -> Option<(i64, i64)> {
    let files = dataset_files(&export_dir(out_dir, tenant), "samples").ok()?;
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    for path in files {
        let file = std::fs::File::open(&path).ok()?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
        let meta = builder.metadata();
        let descr = meta.file_metadata().schema_descr();
        let ts = (0..descr.num_columns()).find(|i| descr.column(*i).name() == "ts")?;
        for rg in 0..meta.num_row_groups() {
            match meta.row_group(rg).column(ts).statistics()? {
                Statistics::Int64(stats) => {
                    lo = lo.min(*stats.min_opt()?);
                    hi = hi.max(*stats.max_opt()?);
                }
                _ => return None,
            }
        }
    }
    (lo <= hi).then_some((lo, hi))
}

/// What the export contains: every metric with its label set, plus which
/// metrics appear in which block (used to stage/load block by block).
#[derive(Debug, Default)]
struct ExportScan {
    metrics: BTreeMap<String, MetricSchema>,
    blocks: BTreeMap<String, BTreeSet<String>>,
}

/// `block=<ulid>` directory of a parquet path, if any.
fn block_of(path: &Path) -> Option<String> {
    path.components().find_map(|c| {
        let s = c.as_os_str().to_str()?;
        s.strip_prefix("block=").map(|b| b.to_string())
    })
}

/// One pass over the series Parquet of an export: metric -> label set, and
/// block -> metrics present there.
fn scan_export(out_dir: &Path, tenant: Option<&str>) -> Result<ExportScan> {
    let root = export_dir(out_dir, tenant);
    let hist_files = dataset_files(&root, "histograms")?;
    let series_files = dataset_files(&root, "series")?;
    let mut scan = ExportScan::default();
    if series_files.is_empty() {
        return Ok(scan);
    }
    let hist_uids = read_uids(&hist_files)?;
    for path in series_files {
        let block = block_of(&path);
        let file = std::fs::File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::sink(format!("open {}: {e}", path.display())))?;
        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
        for batch in reader {
            let batch = batch.map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
            let Some(map) = batch
                .column_by_name("labels")
                .and_then(|c| c.as_any().downcast_ref::<MapArray>())
            else {
                continue;
            };
            let Some(uids) = batch
                .column_by_name("series_uid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            else {
                continue;
            };
            let keys = map
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::sink("labels map has non-string keys"))?;
            let values = map
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::sink("labels map has non-string values"))?;
            let offsets = map.value_offsets();
            for row in 0..map.len() {
                if map.is_null(row) || uids.is_null(row) {
                    continue;
                }
                let uid = uids.value(row);
                let mut metric: Option<String> = None;
                let mut labels = Vec::new();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                for idx in start..end {
                    let k = keys.value(idx);
                    let v = values.value(idx);
                    if k == "__name__" {
                        metric = Some(v.to_string());
                    } else {
                        labels.push(k.to_string());
                    }
                }
                let Some(metric) = metric else { continue };
                let entry = scan.metrics.entry(metric.clone()).or_default();
                entry.labels.extend(labels);
                if hist_uids.contains(&uid) {
                    entry.has_hist = true;
                } else {
                    entry.has_float = true;
                }
                if let Some(block) = &block {
                    scan.blocks.entry(block.clone()).or_default().insert(metric);
                }
            }
        }
    }
    Ok(scan)
}

/// Total rows of every Parquet file under `dir` (footer metadata only).
fn parquet_rows(files: &[PathBuf]) -> u64 {
    let mut total = 0u64;
    for path in files {
        let Ok(file) = std::fs::File::open(path) else {
            continue;
        };
        let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) else {
            continue;
        };
        total += builder.metadata().file_metadata().num_rows().max(0) as u64;
    }
    total
}

/// Rows of the whole samples dataset of an export.
fn dataset_rows(root: &Path) -> u64 {
    let mut files = Vec::new();
    if root.is_dir() {
        let _ = collect_dataset(root, "samples", &mut files);
    }
    parquet_rows(&files)
}

fn read_uids(files: &[PathBuf]) -> Result<HashSet<u64>> {
    let mut out = HashSet::new();
    for path in files {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::sink(format!("open {}: {e}", path.display())))?;
        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
        for batch in reader {
            let batch = batch.map_err(|e| Error::sink(format!("read {}: {e}", path.display())))?;
            let Some(uid) = batch
                .column_by_name("series_uid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            else {
                continue;
            };
            for i in 0..uid.len() {
                if !uid.is_null(i) {
                    out.insert(uid.value(i));
                }
            }
        }
    }
    Ok(out)
}

fn dataset_files(root: &Path, dataset: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if root.is_dir() {
        collect_dataset(root, dataset, &mut out)?;
    }
    out.sort();
    Ok(out)
}

fn collect_dataset(dir: &Path, dataset: &str, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            continue;
        }
        let path = entry.path();
        if ft.is_dir() {
            if entry.file_name().to_str() == Some(dataset) {
                for f in std::fs::read_dir(&path)? {
                    let f = f?;
                    if f.file_type()?.is_file()
                        && f.path()
                            .extension()
                            .map(|x| x == "parquet")
                            .unwrap_or(false)
                    {
                        out.push(f.path());
                    }
                }
            } else {
                collect_dataset(&path, dataset, out)?;
            }
        }
    }
    Ok(())
}

fn duckdb_available() -> bool {
    // `--version` must never wait on stdin: a wrapper script that reads stdin
    // would otherwise block the whole finalizer. Only the plain flag is passed
    // here so wrapper scripts that inspect argv[1] keep seeing "--version".
    std::process::Command::new("duckdb")
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Rebuilds `db_path` from `plan.sql_path` using the DuckDB CLI.
///
/// The generated script has two halves and they are executed as separate
/// DuckDB processes:
///
/// 1. **staging** - `COPY ... PARTITION_BY (metric_key)` writes
///    `<stage>/metric_key=<md5>/data_N.parquet`;
/// 2. **loading** - one `CREATE TABLE` per metric reading that stage.
///
/// The split exists because DuckDB creates every `data_N.parquet` up front and
/// leaves it at **0 bytes** until its first row group is flushed. If the COPY
/// dies (out of disk, out of memory, kill) or two loads share the directory, the
/// metric-table phase reads those header-less files and each one fails with
/// `File ".../data_0.parquet" too small to be a Parquet file` - and the CLI,
/// whose errors do not stop a script, keeps going and leaves a database that is
/// silently missing tables. Running the phases separately lets us validate (and
/// once redo) staging, stop at the first error with `-bail`, and publish the
/// database atomically, so a failure never leaves a half-loaded `db_path`.
///
/// Both phases run under `plan.limits`: memory is capped by DuckDB
/// (`SET memory_limit`), and the disk budget is enforced here by polling what
/// the load writes (staging + spill + database) and killing the phase when it
/// goes over, so an over-budget load never publishes a database.
pub fn execute(plan: &DuckDbPlan) -> Result<()> {
    if !duckdb_available() {
        return Err(Error::sink(format!(
            "duckdb CLI not found (or too old for -bail); {} was generated, run it manually",
            plan.sql_path.display()
        )));
    }
    let _lock = LoadLock::acquire(&plan.stage_dir)?;
    let sql = std::fs::read_to_string(&plan.sql_path)?;
    let chunks = block_chunks(&sql);
    let (stage_sql, table_sql) = split_phases(&sql);
    // The table phase is a second process, so it needs the caps again; the
    // staging phase already has them from the generated file header.
    let table_sql = if table_sql.trim().is_empty() {
        String::new()
    } else {
        format!("{}{}", plan.limits.settings_sql(), table_sql)
    };
    std::fs::create_dir_all(&plan.limits.temp_dir)?;
    if let Some(parent) = plan.db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Build next to the target and rename on success: readers never observe a
    // partially loaded database.
    let tmp_db = tmp_db_path(&plan.db_path);
    let _ = std::fs::remove_file(&tmp_db);
    let wal = PathBuf::from(format!("{}.wal", tmp_db.display()));
    // Two independent caps: the temporary data (staging + spill) and, optionally,
    // the load as a whole including the database being written.
    let stage_budget = DiskBudget {
        limit: plan.limits.stage_bytes,
        dirs: vec![plan.stage_dir.as_path(), plan.limits.temp_dir.as_path()],
        files: Vec::new(),
        label: "temporary data (.duckdb_stage + spill)",
        config_field: "sink.duckdb_stage_limit_mb",
    };
    let total_budget = DiskBudget {
        limit: plan.limits.disk_bytes,
        dirs: vec![plan.stage_dir.as_path(), plan.limits.temp_dir.as_path()],
        files: vec![tmp_db.as_path(), wal.as_path()],
        label: "total disk",
        config_field: "sink.duckdb_disk_limit_mb",
    };
    let budgets = [&stage_budget, &total_budget];

    let outcome = match &chunks {
        Some(chunks) => load_block_chunks(&tmp_db, chunks, plan, &budgets),
        None if plan.window.is_some() => load_window(&tmp_db, &stage_sql, plan, &budgets),
        None => load_phases(&tmp_db, &stage_sql, &table_sql, plan, &budgets),
    }
    .and_then(|()| check_budgets(&budgets));
    if let Err(e) = outcome {
        // Leave nothing half-built behind; the staging dir and the SQL file stay
        // for inspection and for `duckdb-load` to retry.
        let _ = std::fs::remove_file(&tmp_db);
        return Err(e);
    }
    if plan.db_path.exists() {
        std::fs::remove_file(&plan.db_path)?;
    }
    std::fs::rename(&tmp_db, &plan.db_path)
        .map_err(|e| Error::sink(format!("publish {}: {e}", plan.db_path.display())))?;
    // The metric-partitioned staging Parquet is only an intermediate artifact.
    // Keep the empty directory so the generated SQL can be re-run manually.
    let _ = reset_dir(&plan.stage_dir);
    Ok(())
}

/// Loads one time window: stage the window, then create a table for every
/// metric partition the window actually produced.
fn load_window(
    tmp_db: &Path,
    stage_sql: &str,
    plan: &DuckDbPlan,
    budgets: &[&DiskBudget],
) -> Result<()> {
    let stage = plan.stage_dir.as_path();
    reset_dir(stage)?;
    if let Err(e) = run_duckdb_phase(tmp_db, stage_sql, budgets) {
        return Err(staging_failure(e, stage));
    }
    if let Err(first) = validate_stage(stage) {
        reset_dir(stage)?;
        run_duckdb_phase(tmp_db, stage_sql, budgets).map_err(|e| staging_failure(e, stage))?;
        validate_stage(stage).map_err(|second| {
            Error::sink(format!(
                "{second} (window {}: still broken after retrying staging; first attempt: {first})",
                plan.window.map(|w| w.to_string()).unwrap_or_default()
            ))
        })?;
    }
    let statements = window_metric_statements(plan)?;
    if statements.is_empty() {
        return Ok(());
    }
    let preamble = plan.limits.settings_sql();
    let per_batch = if plan.metric_batch == 0 {
        statements.len()
    } else {
        plan.metric_batch.max(1)
    };
    for batch in statements.chunks(per_batch) {
        let mut sql = preamble.clone();
        let mut hashes = Vec::with_capacity(batch.len());
        for (hash, statement) in batch {
            sql.push_str(&format!("-- tsdb-metric {hash}\n{statement}\n"));
            hashes.push(hash.clone());
        }
        run_duckdb_phase(tmp_db, &sql, budgets)?;
        for hash in &hashes {
            let _ = std::fs::remove_dir_all(stage.join(format!("metric_key={hash}")));
        }
        check_budgets(budgets)?;
    }
    Ok(())
}

/// Builds `CREATE TABLE` statements from the metric partitions in the staging
/// directory: a window only contains the metrics it actually has samples for.
fn window_metric_statements(plan: &DuckDbPlan) -> Result<Vec<(String, String)>> {
    let mut by_hash: BTreeMap<String, &MetricSchema> = BTreeMap::new();
    for (metric, schema) in &plan.schemas {
        by_hash.insert(metric_key(metric), schema);
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&plan.stage_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(hash) = name.strip_prefix("metric_key=") else {
            continue;
        };
        // A partition without any Parquet file would make read_parquet fail.
        let has_files = std::fs::read_dir(entry.path())?.flatten().any(|f| {
            f.path()
                .extension()
                .map(|x| x == "parquet")
                .unwrap_or(false)
        });
        if !has_files {
            continue;
        }
        let Some(schema) = by_hash.get(hash) else {
            continue;
        };
        // Reverse lookup of the metric name from the hash.
        let Some(metric) = plan.schemas.keys().find(|m| metric_key(m) == hash) else {
            continue;
        };
        let (labels, _) = label_columns(&schema.labels);
        let statement = metric_sql(
            metric,
            schema,
            &labels,
            &format!(
                "{}/metric_key={}/*.parquet",
                sql_path_str(&plan.stage_dir),
                hash
            ),
            MetricWrite::Create,
        );
        out.push((hash.to_string(), statement));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// One block's slice of a chunked load: its staging COPY and the table
/// statements for the metrics that appear in that block.
struct BlockChunk {
    block: String,
    stage_sql: String,
    load_sql: String,
}

/// Splits a chunked script (`-- tsdb-block` / `-- tsdb-load` markers) into
/// per-block slices. Returns `None` for the single-shot layout, whose
/// stages are split by [`PHASE_MARKER`] instead.
fn block_chunks(sql: &str) -> Option<Vec<BlockChunk>> {
    let mut preamble = String::new();
    let mut chunks: Vec<BlockChunk> = Vec::new();
    let mut in_stage = false;
    for line in sql.lines() {
        if let Some(block) = line.strip_prefix("-- tsdb-block ") {
            chunks.push(BlockChunk {
                block: block.trim().to_string(),
                stage_sql: String::new(),
                load_sql: String::new(),
            });
            in_stage = true;
            continue;
        }
        if line.starts_with("-- tsdb-load ") {
            in_stage = false;
            continue;
        }
        match chunks.last_mut() {
            None => {
                preamble.push_str(line);
                preamble.push('\n');
            }
            Some(chunk) => {
                let target = if in_stage {
                    &mut chunk.stage_sql
                } else {
                    &mut chunk.load_sql
                };
                target.push_str(line);
                target.push('\n');
            }
        }
    }
    if chunks.is_empty() {
        return None;
    }
    for chunk in &mut chunks {
        chunk.stage_sql = format!("{preamble}{}", chunk.stage_sql);
        chunk.load_sql = format!("{preamble}{}", chunk.load_sql);
    }
    Some(chunks)
}

/// Block-by-block load: stage one block, build its metric tables, then drop
/// that staging (and optionally that block of the export).
fn load_block_chunks(
    tmp_db: &Path,
    chunks: &[BlockChunk],
    plan: &DuckDbPlan,
    budgets: &[&DiskBudget],
) -> Result<()> {
    let stage = plan.stage_dir.as_path();
    for chunk in chunks {
        reset_dir(stage)?;
        if let Err(e) = run_duckdb_phase(tmp_db, &chunk.stage_sql, budgets) {
            return Err(staging_failure(e, stage));
        }
        if let Err(first) = validate_stage(stage) {
            reset_dir(stage)?;
            run_duckdb_phase(tmp_db, &chunk.stage_sql, budgets)
                .map_err(|e| staging_failure(e, stage))?;
            validate_stage(stage).map_err(|second| {
                Error::sink(format!(
                    "{second} (block {}: still broken after retrying staging; first attempt: {first})",
                    chunk.block
                ))
            })?;
        }
        for batch in metric_batches(&chunk.load_sql, plan.metric_batch) {
            run_duckdb_phase(tmp_db, &batch.sql, budgets)?;
        }
        check_budgets(budgets)?;
        // This block's staging has been loaded into the database.
        reset_dir(stage)?;
        if plan.drop_export {
            let _ = std::fs::remove_dir_all(plan.export_dir.join(format!("block={}", chunk.block)));
        }
    }
    Ok(())
}

fn load_phases(
    tmp_db: &Path,
    stage_sql: &str,
    table_sql: &str,
    plan: &DuckDbPlan,
    budgets: &[&DiskBudget],
) -> Result<()> {
    let stage = plan.stage_dir.as_path();
    if table_sql.trim().is_empty() {
        // Nothing was partitioned (no metrics detected): the script is a single
        // phase, run it as generated.
        return run_duckdb_phase(tmp_db, stage_sql, budgets);
    }

    reset_dir(stage)?;
    if let Err(e) = run_duckdb_phase(tmp_db, stage_sql, budgets) {
        return Err(staging_failure(e, stage));
    }
    if let Err(first) = validate_stage(stage) {
        // The COPY reported success yet left unusable files: something else
        // touched the directory, or the write was cut short. Redo it once from
        // an empty directory before giving up.
        reset_dir(stage)?;
        run_duckdb_phase(tmp_db, stage_sql, budgets).map_err(|e| staging_failure(e, stage))?;
        validate_stage(stage).map_err(|second| {
            Error::sink(format!(
                "{second} (still broken after retrying the staging step; first attempt: {first})"
            ))
        })?;
    }
    // Staging is complete and validated: the Parquet export is no longer read.
    // Dropping it frees one full copy for the (much larger) table phase.
    if plan.drop_export {
        let _ = std::fs::remove_dir_all(&plan.export_dir);
    }

    // Load the metric tables batch by batch: after a batch commits, its staging
    // partitions are deleted, so the staging copy shrinks while the database
    // grows instead of both sitting at full size.
    for batch in metric_batches(table_sql, plan.metric_batch) {
        run_duckdb_phase(tmp_db, &batch.sql, budgets)?;
        for hash in &batch.hashes {
            let _ = std::fs::remove_dir_all(stage.join(format!("metric_key={hash}")));
        }
        check_budgets(budgets)?;
    }
    Ok(())
}

/// One run of the table phase: the statements of at most `size` metrics.
struct MetricBatch {
    hashes: Vec<String>,
    sql: String,
}

/// Splits the table phase at its `-- tsdb-metric <md5>` markers. Whatever comes
/// before the first marker (the session settings) is repeated in every batch.
fn metric_batches(table_sql: &str, size: usize) -> Vec<MetricBatch> {
    let mut preamble = String::new();
    let mut segments: Vec<(String, String)> = Vec::new();
    let mut current: Option<(String, String)> = None;
    for line in table_sql.lines() {
        if let Some(hash) = line.strip_prefix("-- tsdb-metric ") {
            if let Some(done) = current.take() {
                segments.push(done);
            }
            current = Some((hash.trim().to_string(), String::new()));
            continue;
        }
        match current.as_mut() {
            Some((_, sql)) => {
                sql.push_str(line);
                sql.push('\n');
            }
            None => {
                preamble.push_str(line);
                preamble.push('\n');
            }
        }
    }
    if let Some(done) = current.take() {
        segments.push(done);
    }
    if segments.is_empty() {
        return vec![MetricBatch {
            hashes: Vec::new(),
            sql: table_sql.to_string(),
        }];
    }
    let per_batch = if size == 0 {
        segments.len()
    } else {
        size.max(1)
    };
    segments
        .chunks(per_batch)
        .map(|chunk| {
            let mut sql = preamble.clone();
            let mut hashes = Vec::with_capacity(chunk.len());
            for (hash, text) in chunk {
                hashes.push(hash.clone());
                sql.push_str(text);
            }
            MetricBatch { hashes, sql }
        })
        .collect()
}

fn staging_failure(e: Error, stage: &Path) -> Error {
    Error::sink(format!("{e} while staging the per-metric Parquet in {}; the DuckDB error above is the real cause (most often free disk space - staging writes a second copy of the export)", stage.display()))
}

/// Splits the generated script at [`PHASE_MARKER`]. Without the marker the whole
/// script is treated as a single phase (older generated files).
fn split_phases(sql: &str) -> (String, String) {
    match sql.find(PHASE_MARKER) {
        Some(idx) => (
            sql[..idx].to_string(),
            sql[idx + PHASE_MARKER.len()..].to_string(),
        ),
        None => (sql.to_string(), String::new()),
    }
}

/// Rejects a staging directory that would make the metric-table phase fail:
/// empty, or containing files that never got their first row group.
fn validate_stage(stage: &Path) -> Result<usize> {
    let mut files = 0usize;
    let mut truncated: Vec<PathBuf> = Vec::new();
    let entries = std::fs::read_dir(stage)
        .map_err(|e| Error::sink(format!("read staging directory {}: {e}", stage.display())))?;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(entry.path())? {
            let file = file?;
            if !file.file_type()?.is_file() {
                continue;
            }
            if !file
                .path()
                .extension()
                .map(|x| x == "parquet")
                .unwrap_or(false)
            {
                continue;
            }
            files += 1;
            if file.metadata()?.len() < PARQUET_MIN_BYTES {
                truncated.push(file.path());
            }
        }
    }
    if files == 0 {
        return Err(Error::sink(format!(
            "staging produced no Parquet files in {}",
            stage.display()
        )));
    }
    if !truncated.is_empty() {
        truncated.sort();
        return Err(Error::sink(format!("staging left {} truncated Parquet file(s), e.g. {} (the COPY was interrupted, or another process is writing the same directory)", truncated.len(), truncated[0].display())));
    }
    Ok(files)
}

fn reset_dir(dir: &Path) -> Result<()> {
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::create_dir_all(dir)?;
    Ok(())
}

fn tmp_db_path(db: &Path) -> PathBuf {
    let name = db
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "db.duckdb".to_string());
    db.with_file_name(format!(".{name}.tmp"))
}

/// Runs one phase of the script through the DuckDB CLI under a disk budget.
///
/// The statement list is handed over as a file: a multi-megabyte DDL script
/// piped through stdin can block forever when duckdb exits early on an error.
/// The child is polled while it runs so a phase that outgrows
/// `budget.limit` is killed instead of filling the filesystem.
fn run_duckdb_phase(db: &Path, sql: &str, budgets: &[&DiskBudget]) -> Result<()> {
    if sql.trim().is_empty() {
        return Ok(());
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!("tsdb-duckdb-{}-{stamp}.sql", std::process::id()));
    std::fs::write(&path, sql)?;
    let file = std::fs::File::open(&path)?;
    let spawned = std::process::Command::new("duckdb")
        // The database path comes first so a "duckdb" wrapper script that only
        // looks at argv[1] keeps working; -bail still applies.
        .arg(db)
        .arg("-bail")
        .stdin(std::process::Stdio::from(file))
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn();
    let mut child = match spawned {
        Ok(child) => child,
        Err(e) => {
            let _ = std::fs::remove_file(&path);
            return Err(Error::sink(format!("run duckdb: {e}")));
        }
    };

    let outcome = loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => break Ok(()),
            Ok(Some(status)) => {
                break Err(Error::sink(format!(
                    "duckdb exited with status {status} while loading {}",
                    db.display()
                )))
            }
            Ok(None) => {}
            Err(e) => break Err(Error::sink(format!("wait for duckdb: {e}"))),
        }
        // Walking a staging tree with tens of thousands of files costs real
        // time, so the poll interval adapts: never tight-loop on a huge tree.
        let walk = std::time::Instant::now();
        let over_budget = check_budgets(budgets);
        let walk = walk.elapsed();
        if let Err(e) = over_budget {
            let _ = child.kill();
            let _ = child.wait();
            break Err(e);
        }
        std::thread::sleep((walk * 4).max(Duration::from_millis(500)));
    };
    let _ = std::fs::remove_file(&path);
    outcome
}

/// Disk footprint of one load: the directories and files it writes.
struct DiskBudget<'a> {
    /// 0 = this budget is disabled.
    limit: u64,
    dirs: Vec<&'a Path>,
    files: Vec<&'a Path>,
    /// What is measured, for the error message ("temporary data", "load").
    label: &'static str,
    /// Config field to point the operator at.
    config_field: &'static str,
}

fn check_budgets(budgets: &[&DiskBudget]) -> Result<()> {
    for budget in budgets {
        budget.check()?;
    }
    Ok(())
}

impl DiskBudget<'_> {
    /// A budget that never trips; used for helper scripts and tests.
    #[cfg(test)]
    fn unlimited() -> Self {
        Self {
            limit: u64::MAX,
            dirs: Vec::new(),
            files: Vec::new(),
            label: "unlimited",
            config_field: "none",
        }
    }

    fn usage(&self) -> u64 {
        let mut total = 0u64;
        for dir in &self.dirs {
            total += dir_bytes(dir);
        }
        for file in &self.files {
            if let Ok(md) = std::fs::metadata(file) {
                total += md.len();
            }
        }
        total
    }

    fn check(&self) -> Result<()> {
        if self.limit == 0 {
            return Ok(());
        }
        let used = self.usage();
        if used > self.limit {
            return Err(Error::sink(format!(
                "DuckDB load exceeded its {} budget: {} used of {} ({}); the load was aborted and the database was not published",
                self.label,
                fmt_bytes(used),
                fmt_bytes(self.limit),
                self.config_field
            )));
        }
        Ok(())
    }
}

fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(md) = entry.metadata() else { continue };
        if md.is_dir() {
            total += dir_bytes(&entry.path());
        } else {
            total += md.len();
        }
    }
    total
}

fn fmt_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= GIB {
        format!("{:.1} GiB", b / GIB)
    } else {
        format!("{:.1} MiB", b / MIB)
    }
}

/// Prevents two loads (engine auto-load, `duckdb-load`, a `force` re-run) from
/// writing the same staging directory at the same time.
struct LoadLock {
    path: PathBuf,
}

impl LoadLock {
    fn acquire(stage: &Path) -> Result<Self> {
        let parent = stage.parent().unwrap_or(stage);
        std::fs::create_dir_all(parent)?;
        let name = stage
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "load".to_string());
        let path = parent.join(format!(".{name}.load.lock"));
        for _ in 0..2 {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    use std::io::Write as _;
                    let _ = writeln!(file, "pid={}", std::process::id());
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if is_stale_lock(&path) {
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    return Err(Error::sink(format!("another DuckDB load is already running for {} (lock {}); wait for it to finish, or delete the lock file if the process is gone", stage.display(), path.display())));
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::sink(format!(
            "could not acquire the DuckDB load lock {}",
            path.display()
        )))
    }
}

impl Drop for LoadLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn is_stale_lock(path: &Path) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .map(|age| age > STALE_LOCK)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_quotes() {
        assert_eq!(sql_escape("/tmp/a'b"), "/tmp/a''b");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn reserved_label_is_renamed() {
        let mut labels = BTreeSet::new();
        labels.insert("value".to_string());
        labels.insert("job".to_string());
        let (cols, warnings) = label_columns(&labels);
        assert_eq!(cols.len(), 2);
        assert!(cols
            .iter()
            .any(|c| c.key == "value" && c.column == "label_value"));
        assert!(!warnings.is_empty());
    }

    #[test]
    fn metric_key_matches_duckdb_md5() {
        assert_eq!(
            metric_key("fixture_metric_00"),
            "a243d64f533370f28c0625fdea762cdc"
        );
    }

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tsdb-duckdb-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn tenant_listing() {
        let dir = scratch_dir("tenants");
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-a")).unwrap();
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-b")).unwrap();
        assert_eq!(output_tenants(&dir), vec!["tenant-a", "tenant-b"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn prometheus_uses_one_database() {
        let dir = scratch_dir("single");
        assert_eq!(
            resolve_db_paths(&dir, "", None).unwrap(),
            dir.join("migration.duckdb")
        );
        assert_eq!(
            resolve_db_paths(&dir, "/tmp/some-dir", None).unwrap(),
            PathBuf::from("/tmp/some-dir/migration.duckdb")
        );
        assert_eq!(
            resolve_db_paths(&dir, "/tmp/a.duckdb", None).unwrap(),
            PathBuf::from("/tmp/a.duckdb")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn mimir_gets_one_database_per_tenant() {
        let dir = scratch_dir("multi");
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-a")).unwrap();
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-b")).unwrap();
        assert_eq!(
            resolve_db_paths(&dir, "", Some("tenant-a")).unwrap(),
            dir.join("duckdb/tenant-a.duckdb")
        );
        assert_eq!(
            resolve_db_paths(&dir, "", Some("tenant-b")).unwrap(),
            dir.join("duckdb/tenant-b.duckdb")
        );
        assert!(resolve_db_paths(&dir, "/tmp/f.duckdb", Some("tenant-a")).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn single_tenant_mimir_accepts_a_file() {
        let dir = scratch_dir("one");
        std::fs::create_dir_all(dir.join("blocks/tenant=tenant-a")).unwrap();
        assert_eq!(
            resolve_db_paths(&dir, "/tmp/only.duckdb", Some("tenant-a")).unwrap(),
            PathBuf::from("/tmp/only.duckdb")
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn phases_split_at_the_marker() {
        let sql = format!("SELECT 1;\n{PHASE_MARKER}\nCREATE TABLE m AS SELECT 2;");
        let (stage, tables) = split_phases(&sql);
        assert!(stage.contains("SELECT 1;"), "{stage}");
        assert!(!stage.contains("CREATE TABLE m"), "{stage}");
        assert!(tables.contains("CREATE TABLE m"), "{tables}");
        let (whole, none) = split_phases("SELECT 1;");
        assert_eq!(whole, "SELECT 1;");
        assert!(none.is_empty());
    }

    #[test]
    fn blocks_split_into_chunks_at_markers() {
        let sql = "SET threads=4;\n-- tsdb-block b1\nCOPY (SELECT 1) TO 'x';\n-- tsdb-load b1\n-- tsdb-metric h1\nCREATE TABLE \"m\" AS SELECT 1;\n-- tsdb-block b2\nCOPY (SELECT 2) TO 'x';\n-- tsdb-load b2\nINSERT INTO \"m\" SELECT 2;\n";
        let chunks = block_chunks(sql).expect("chunked script");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].block, "b1");
        assert!(chunks[0].stage_sql.contains("COPY (SELECT 1)"));
        assert!(chunks[0].stage_sql.contains("SET threads=4;"));
        assert!(!chunks[0].stage_sql.contains("CREATE TABLE"));
        assert!(chunks[0].load_sql.contains("CREATE TABLE"));
        assert!(chunks[1].load_sql.contains("INSERT INTO"));
        assert!(block_chunks("COPY (SELECT 1) TO 'x';").is_none());
    }

    #[test]
    fn validate_stage_rejects_truncated_and_empty_staging() {
        let dir = scratch_dir("stage");
        let err = validate_stage(&dir).unwrap_err().to_string();
        assert!(err.contains("no Parquet files"), "{err}");

        let part = dir.join("metric_key=80deb84baf75fecf4bc7f94b38687527");
        std::fs::create_dir_all(&part).unwrap();
        std::fs::write(part.join("data_0.parquet"), b"PAR1PAR1").unwrap();
        assert_eq!(validate_stage(&dir).unwrap(), 1);

        std::fs::write(part.join("data_1.parquet"), b"").unwrap();
        let err = validate_stage(&dir).unwrap_err().to_string();
        assert!(err.contains("truncated"), "{err}");
        assert!(err.contains("data_1.parquet"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_second_load_cannot_take_the_lock() {
        let dir = scratch_dir("lock");
        let stage = dir.join(".duckdb_stage").join("tenant-a");
        let first = LoadLock::acquire(&stage).unwrap();
        let err = match LoadLock::acquire(&stage) {
            Ok(_) => panic!("a second load must not start"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("another DuckDB load"), "{err}");
        drop(first);
        LoadLock::acquire(&stage).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn resource_caps_are_written_into_the_script() {
        let limits = LoadLimits {
            memory_bytes: 24 * 1024 * 1024 * 1024,
            stage_bytes: 200 * 1024 * 1024 * 1024,
            disk_bytes: 0,
            threads: 8,
            temp_dir: PathBuf::from("/out/.duckdb_stage/.spill"),
            stage_compression: crate::config::Compression::Zstd,
        };
        let sql = limits.settings_sql();
        assert!(sql.contains("SET memory_limit='24GiB';"), "{sql}");
        assert!(
            sql.contains("SET max_temp_directory_size='200GiB';"),
            "{sql}"
        );
        assert!(
            sql.contains("SET temp_directory='/out/.duckdb_stage/.spill';"),
            "{sql}"
        );
        assert!(sql.contains("SET threads=8;"), "{sql}");

        let sink = crate::config::SinkConfig::default();
        let defaults = LoadLimits::from_sink(&sink, Path::new("/out"));
        assert_eq!(defaults.memory_bytes, 24 * 1024 * 1024 * 1024);
        assert_eq!(defaults.stage_bytes, 200 * 1024 * 1024 * 1024);
        assert_eq!(defaults.disk_bytes, 0, "no total cap by default");
        assert_eq!(defaults.threads, 0);
        assert!(defaults.settings_sql().contains("memory_limit='24GiB'"));
        assert!(!defaults.settings_sql().contains("SET threads="));
        assert_eq!(
            defaults.temp_dir,
            PathBuf::from("/out/.duckdb_stage/.spill")
        );
    }

    #[test]
    fn duckdb_sizes_are_binary_units() {
        assert_eq!(duckdb_size(24 * 1024 * 1024 * 1024), "24GiB");
        assert_eq!(duckdb_size(512 * 1024 * 1024), "512MiB");
        assert_eq!(duckdb_size(1234), "1234B");
    }

    #[test]
    fn dir_bytes_counts_nested_files() {
        let dir = scratch_dir("bytes");
        std::fs::create_dir_all(dir.join("a/b")).unwrap();
        std::fs::write(dir.join("a/b/one.parquet"), vec![0u8; 100]).unwrap();
        std::fs::write(dir.join("a/two.parquet"), vec![0u8; 50]).unwrap();
        assert_eq!(dir_bytes(&dir), 150);
        assert_eq!(dir_bytes(&dir.join("missing")), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn budgets_report_which_limit_tripped() {
        let dir = scratch_dir("budget-unit");
        std::fs::write(dir.join("data.parquet"), vec![0u8; 4096]).unwrap();
        let stage = DiskBudget {
            limit: 1024,
            dirs: vec![dir.as_path()],
            files: Vec::new(),
            label: "temporary data",
            config_field: "sink.duckdb_stage_limit_mb",
        };
        let err = stage.check().unwrap_err().to_string();
        assert!(err.contains("temporary data"), "{err}");
        assert!(err.contains("sink.duckdb_stage_limit_mb"), "{err}");
        let off = DiskBudget {
            limit: 0,
            dirs: vec![dir.as_path()],
            files: Vec::new(),
            label: "total disk",
            config_field: "sink.duckdb_disk_limit_mb",
        };
        assert!(off.check().is_ok(), "0 disables the budget");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn hourly_windows_are_aligned_and_labelled_in_utc() {
        // 2026-11-11T10:00:00Z .. 12:30Z
        let start = 1_794_391_200_000i64;
        let windows = windows_for(start, start + 2 * 3_600_000 + 1_800_000, 60);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].label(), "2026-11-11T10");
        assert_eq!(windows[1].label(), "2026-11-11T11");
        assert_eq!(windows[2].label(), "2026-11-11T12");
        assert_eq!(windows[0].end_ms - windows[0].start_ms, 3_600_000);
        // A range inside one hour gives exactly one window.
        assert_eq!(windows_for(start, start + 60_000, 60).len(), 1);
    }

    #[test]
    fn window_names_prefix_the_database_file() {
        let window = Window {
            start_ms: 1_794_391_200_000,
            end_ms: 1_794_394_800_000,
        };
        assert_eq!(
            window_db_path(Path::new("/out/duckdb/t1.duckdb"), &window),
            Path::new("/out/duckdb").join("2026-11-11T10-t1.duckdb")
        );
        assert_eq!(
            sql_path(Path::new("/out"), Some("t1"), Some(window)),
            PathBuf::from("/out/2026-11-11T10-load_duckdb_t1.sql")
        );
    }

    /// Two hours of samples become two databases, named by the hour and filled
    /// with only that hour's rows.
    #[test]
    fn loads_one_database_per_hour() {
        if !duckdb_available() {
            return;
        }
        let out = scratch_dir("hourly");
        let dir = out.join("blocks/tenant=t1/block=01KEHOUR");
        std::fs::create_dir_all(dir.join("series")).unwrap();
        std::fs::create_dir_all(dir.join("samples")).unwrap();
        let slash = |p: &Path| p.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/");
        let series = format!(
            "COPY (SELECT 1::UBIGINT AS series_uid, 1::UBIGINT AS series_ref, MAP {{'__name__': 'm_a'}} AS labels) TO '{}' (FORMAT PARQUET);",
            slash(&dir.join("series").join("p.parquet"))
        );
        // 2026-11-11T10:00Z and 11:30Z: one row in each hour.
        let samples = format!(
            "COPY (SELECT * FROM (VALUES (1::UBIGINT, 1794391200000::BIGINT, 1.0::DOUBLE), (1::UBIGINT, 1794396600000::BIGINT, 2.0::DOUBLE)) t(series_uid, ts, value)) TO '{}' (FORMAT PARQUET);",
            slash(&dir.join("samples").join("p.parquet"))
        );
        let setup_db = out.join("setup.duckdb");
        let no_budget = DiskBudget::unlimited();
        run_duckdb_phase(&setup_db, &format!("{series}\n{samples}\n"), &[&no_budget]).unwrap();

        let sink = crate::config::SinkConfig {
            duckdb_window_minutes: 60,
            ..Default::default()
        };
        let plans = plan_with_sink(&out, &sink).unwrap();
        let labels: Vec<String> = plans
            .iter()
            .map(|p| {
                p.window
                    .map(|w| w.label())
                    .unwrap_or_else(|| "-".to_string())
            })
            .collect();
        assert_eq!(labels, vec!["2026-11-11T10", "2026-11-11T11"]);
        assert_eq!(
            plans[0].db_path.file_name().unwrap().to_str().unwrap(),
            "2026-11-11T10-t1.duckdb"
        );

        for plan in &plans {
            execute(plan).unwrap();
        }
        assert_eq!(count_rows(&plans[0].db_path, "m_a"), "1");
        assert_eq!(count_rows(&plans[1].db_path, "m_a"), "1");
        // The windowed script keeps only the staging step plus the window filter.
        let sql_text = std::fs::read_to_string(&plans[0].sql_path).unwrap();
        assert!(
            sql_text.contains("s.ts >= 1794391200000 AND s.ts < 1794394800000"),
            "{sql_text}"
        );
        assert!(sql_text.contains(PHASE_MARKER), "{sql_text}");
        std::fs::remove_dir_all(&out).unwrap();
    }
    #[test]
    fn metric_batches_split_on_markers_and_repeat_the_preamble() {
        let sql = "SET threads=4;\n-- tsdb-metric aaa\nCREATE TABLE \"a\" AS SELECT 1;\n\n-- tsdb-metric bbb\nCREATE TABLE \"b\" AS SELECT 2;\n\n-- tsdb-metric ccc\nCREATE TABLE \"c\" AS SELECT 3;\n";
        let batches = metric_batches(sql, 2);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].hashes, vec!["aaa", "bbb"]);
        assert!(batches[0].sql.contains("SET threads=4;"));
        assert!(batches[0].sql.contains("CREATE TABLE \"a\""));
        assert!(!batches[0].sql.contains("CREATE TABLE \"c\""));
        assert_eq!(batches[1].hashes, vec!["ccc"]);
        assert!(batches[1].sql.contains("SET threads=4;"));

        assert_eq!(metric_batches(sql, 0).len(), 1);
        let plain = metric_batches("SET threads=4;\nSELECT 1;", 4);
        assert_eq!(plain.len(), 1);
        assert!(plain[0].hashes.is_empty());
        assert!(plain[0].sql.contains("SELECT 1;"));
    }
    /// Writes a tiny two-metric export: series uid 1 -> m_a, uid 2 -> m_b.
    fn write_parquet_export(out: &Path, block: &str, samples: usize) {
        let dir = out
            .join("blocks")
            .join("tenant=t1")
            .join(format!("block={block}"));
        std::fs::create_dir_all(dir.join("series")).unwrap();
        std::fs::create_dir_all(dir.join("samples")).unwrap();
        let slash = |p: &Path| p.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/");
        let series = format!(
            "COPY (SELECT * FROM (VALUES (1::UBIGINT, 1::UBIGINT, MAP {{'__name__': 'm_a'}}), (2::UBIGINT, 2::UBIGINT, MAP {{'__name__': 'm_b'}})) t(series_uid, series_ref, labels)) TO '{}' (FORMAT PARQUET);",
            slash(&dir.join("series").join("p.parquet"))
        );
        let samples_sql = format!(
            "COPY (SELECT (i % 2 + 1)::UBIGINT AS series_uid, (1700000000000 + i)::BIGINT AS ts, (i % 100)::DOUBLE AS value FROM range(0, {samples}) t(i)) TO '{}' (FORMAT PARQUET);",
            slash(&dir.join("samples").join("p.parquet"))
        );
        let setup_db = out.join(format!("setup-{block}.duckdb"));
        let no_budget = DiskBudget::unlimited();
        run_duckdb_phase(
            &setup_db,
            &format!("{series}\n{samples_sql}\n"),
            &[&no_budget],
        )
        .unwrap();
    }

    fn count_rows(db: &Path, table: &str) -> String {
        let output = std::process::Command::new("duckdb")
            .arg(db)
            .arg("-csv")
            .arg("-c")
            .arg(format!("SELECT count(*) FROM {}", quote_ident(table)))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .last()
            .unwrap_or("")
            .trim()
            .to_string()
    }

    /// Two blocks, staged one at a time and appended into the same metric
    /// tables: each block's staging and export disappear before the next one.
    #[test]
    fn loads_block_by_block_and_drops_each_export() {
        if !duckdb_available() {
            return;
        }
        let out = scratch_dir("chunked");
        write_parquet_export(&out, "01KEB1", 2);
        write_parquet_export(&out, "01KEB2", 2);

        let sink = crate::config::SinkConfig {
            duckdb_chunk_by_block: true,
            duckdb_drop_export: true,
            ..Default::default()
        };
        let plans = plan_with_sink(&out, &sink).unwrap();
        let plan = &plans[0];
        let sql_text = std::fs::read_to_string(&plan.sql_path).unwrap();
        assert!(sql_text.contains("-- tsdb-block 01KEB1"), "{sql_text}");
        assert!(sql_text.contains("-- tsdb-block 01KEB2"), "{sql_text}");
        assert!(
            sql_text.contains("CREATE OR REPLACE TABLE \"m_a\""),
            "{sql_text}"
        );
        assert!(sql_text.contains("INSERT INTO \"m_a\""), "{sql_text}");
        execute(plan).unwrap();

        assert!(!plan.export_dir.join("block=01KEB1").exists());
        assert!(!plan.export_dir.join("block=01KEB2").exists());
        assert!(std::fs::read_dir(&plan.stage_dir).unwrap().next().is_none());
        assert_eq!(count_rows(&plan.db_path, "m_a"), "2");
        assert_eq!(count_rows(&plan.db_path, "m_b"), "2");

        std::fs::remove_dir_all(&out).unwrap();
    }

    /// Batched loading must still publish every table, and each batch must
    /// release its staging partitions.
    #[test]
    fn loads_metric_batches_and_reclaims_staging() {
        if !duckdb_available() {
            return;
        }
        let out = scratch_dir("batched");
        write_parquet_export(&out, "01KEBATCH", 4);

        let sink = crate::config::SinkConfig {
            duckdb_metric_batch: 1,
            duckdb_drop_export: true,
            ..Default::default()
        };
        let plans = plan_with_sink(&out, &sink).unwrap();
        let plan = &plans[0];
        execute(plan).unwrap();

        assert!(plan.db_path.is_file());
        assert!(
            std::fs::read_dir(&plan.stage_dir).unwrap().next().is_none(),
            "staging not reclaimed"
        );
        assert!(!plan.export_dir.exists(), "export not dropped");
        assert_eq!(count_rows(&plan.db_path, "m_a"), "2");
        assert_eq!(count_rows(&plan.db_path, "m_b"), "2");

        std::fs::remove_dir_all(&out).unwrap();
    }

    #[test]
    fn a_broken_table_phase_fails_fast_and_keeps_the_stage() {
        if !duckdb_available() {
            return;
        }
        let dir = scratch_dir("broken");
        let tmp = dir.join("x.duckdb");
        let stage = dir.join("stage");
        let parent = stage
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let stage_sql = format!(
            "COPY (SELECT md5('m_a') AS metric_key, 1 AS ts, 1.0 AS value) TO '{parent}' (FORMAT PARQUET, PARTITION_BY (metric_key));"
        );
        let plan = DuckDbPlan {
            tenant: None,
            sql_path: dir.join("x.sql"),
            db_path: dir.join("x.duckdb"),
            stage_dir: stage.clone(),
            metrics: 1,
            warnings: Vec::new(),
            export_dir: dir.join("blocks"),
            drop_export: false,
            metric_batch: 0,
            samples: 0,
            window: None,
            schemas: BTreeMap::new(),
            limits: LoadLimits {
                memory_bytes: 1024 * 1024 * 1024,
                stage_bytes: 0,
                disk_bytes: 0,
                threads: 0,
                temp_dir: dir.join("spill"),
                stage_compression: crate::config::Compression::Snappy,
            },
        };
        let budget = DiskBudget::unlimited();
        let err = load_phases(
            &tmp,
            &stage_sql,
            "SELECT * FROM does_not_exist;",
            &plan,
            &[&budget],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("duckdb exited"), "{err}");
        assert!(stage.is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// End-to-end with the real DuckDB CLI: build a tiny export, run the
    /// generated two-phase load and check the published database.
    #[test]
    fn loads_a_synthetic_export_and_publishes_atomically() {
        if !duckdb_available() {
            return;
        }
        let out = scratch_dir("e2e");
        write_parquet_export(&out, "01KESYNTH", 3);

        let plans = plan(&out, "").unwrap();
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert_eq!(plan.metrics, 2, "warnings: {:?}", plan.warnings);
        assert_eq!(plan.samples, 3, "sample rows from the parquet footers");
        let sql_text = std::fs::read_to_string(&plan.sql_path).unwrap();
        assert!(sql_text.contains(PHASE_MARKER), "no phase marker");
        assert!(sql_text.contains("COPY (\n"), "{sql_text}");
        assert!(!sql_text.contains("_tsdb_metric_long"), "{sql_text}");
        assert!(sql_text.contains("COMPRESSION zstd"), "{sql_text}");
        assert!(sql_text.contains("SET memory_limit='24GiB';"), "{sql_text}");
        assert!(
            sql_text.contains("SET max_temp_directory_size='200GiB';"),
            "{sql_text}"
        );

        execute(plan).unwrap();

        assert!(plan.db_path.is_file());
        let tmp_db = tmp_db_path(&plan.db_path);
        assert!(
            !tmp_db.exists(),
            "temp db left behind: {}",
            tmp_db.display()
        );
        assert!(std::fs::read_dir(&plan.stage_dir).unwrap().next().is_none());
        let lock = plan.stage_dir.parent().unwrap().join(".t1.load.lock");
        assert!(!lock.exists(), "lock left behind: {}", lock.display());
        assert_eq!(count_rows(&plan.db_path, "m_a"), "2");
        assert_eq!(count_rows(&plan.db_path, "m_b"), "1");

        std::fs::remove_dir_all(&out).unwrap();
    }

    /// A load that busts the temporary-data budget must not publish a database.
    #[test]
    fn an_over_budget_load_does_not_publish() {
        if !duckdb_available() {
            return;
        }
        let out = scratch_dir("budget");
        // ~1M samples: the zstd staging alone is several MiB.
        write_parquet_export(&out, "01KEBUDGET", 1_000_000);

        // Cap the *total* footprint: the staging tree of this shape is tiny,
        // but the database being written is not.
        let sink = crate::config::SinkConfig {
            duckdb_stage_limit_mb: 0,
            duckdb_disk_limit_mb: 1,
            ..Default::default()
        };
        let plans = plan_with_sink(&out, &sink).unwrap();
        let plan = &plans[0];
        let err = execute(plan).unwrap_err().to_string();
        assert!(err.contains("total disk"), "{err}");
        assert!(err.contains("sink.duckdb_disk_limit_mb"), "{err}");
        assert!(
            !plan.db_path.exists(),
            "an over-budget load must not publish {}",
            plan.db_path.display()
        );
        assert!(!tmp_db_path(&plan.db_path).exists());
        std::fs::remove_dir_all(&out).unwrap();
    }
}
